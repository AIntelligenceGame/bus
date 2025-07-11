package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"reflect"

	"gorm.io/driver/clickhouse"
	"gorm.io/gorm"
)

// 通用迁移参数
var (
	srcDSN           string
	dstDSN           string
	srcTable         string
	dstTable         string
	parallelism      int
	timeField        string
	startTime        string
	isSrcDistributed bool
	isDstDistributed bool
	clusterName      string
	ignoreFields     []string
	doneSegmentsFile string // 新增：done_segments 文件名
)

func init() {
	flag.StringVar(&srcDSN, "src-dsn", "clickhouse://default:@localhost:9000/default", "源ClickHouse DSN (支持tcp/http)")
	flag.StringVar(&dstDSN, "dst-dsn", "clickhouse://default:@localhost:9000/default", "目标ClickHouse DSN (支持tcp/http)")
	flag.StringVar(&srcTable, "src-table", "", "源表名")
	flag.StringVar(&dstTable, "dst-table", "", "目标表名")
	flag.IntVar(&parallelism, "parallelism", 4, "并发数")
	flag.StringVar(&timeField, "time-field", "", "用于迁移的时间字段（DateTime类型）")
	flag.StringVar(&startTime, "starttime", "1970-01-01 08:00:01", "迁移起始时间")
	flag.BoolVar(&isSrcDistributed, "is-src-distributed", false, "源表是否为分布式表")
	flag.BoolVar(&isDstDistributed, "is-dst-distributed", false, "目标表是否为分布式表")
	flag.StringVar(&clusterName, "cluster-name", "", "ClickHouse集群名（分布式表rename时用）")
	flag.Func("ignore-field", "忽略校验和插入的字段，可指定多次", func(s string) error {
		ignoreFields = append(ignoreFields, s)
		return nil
	})
	flag.StringVar(&doneSegmentsFile, "done-segments", "", "断点续传文件名，留空则自动生成")
}

func isIgnoredField(name string) bool {
	for _, f := range ignoreFields {
		if f == name {
			return true
		}
	}
	return false
}

type columnInfo struct {
	Name string
	Type string
}

type migrationResult struct {
	SegmentStart time.Time
	SegmentEnd   time.Time
	RowsRead     int
	RowsWritten  int
	Duration     time.Duration
	Error        error
}

var doneSegmentMu sync.Mutex

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	flag.Parse()
	if srcTable == "" || dstTable == "" || timeField == "" {
		log.Fatal("src-table、dst-table、time-field 参数必填")
	}
	// 动态生成默认 doneSegmentsFile
	if doneSegmentsFile == "" {
		doneSegmentsFile = fmt.Sprintf("done_segments_%s_to_%s.txt", srcTable, dstTable)
	}
	fmt.Println("srcDSN:", srcDSN)
	fmt.Println("dstDSN:", dstDSN)

	srcDB, err := gorm.Open(clickhouse.Open(srcDSN), &gorm.Config{})
	if err != nil {
		log.Fatalf("连接源库失败: %v", err)
	}
	dstDB, err := gorm.Open(clickhouse.Open(dstDSN), &gorm.Config{})
	if err != nil {
		log.Fatalf("连接目标库失败: %v", err)
	}

	// 字段顺序/类型一致性校验
	err = compareTableColumns(srcDB, dstDB, srcTable, dstTable)
	if err != nil {
		log.Fatalf("表结构不一致: %v", err)
	}

	columns, err := getTableColumns(srcDB, srcTable)
	if err != nil {
		log.Fatalf("获取表结构失败: %v", err)
	}
	if !checkTimeField(columns, timeField) {
		log.Fatalf("字段 %s 不存在或不是DateTime类型", timeField)
	}

	minTime, maxTime, err := getTimeRange(srcDB, srcTable, timeField, startTime)
	if err != nil {
		log.Fatalf("获取时间范围失败: %v", err)
	}
	if minTime.IsZero() || maxTime.IsZero() {
		log.Println("数据源无数据，任务终止")
		return
	}

	logFile, err := os.Create("log.json")
	if err != nil {
		log.Fatalf("创建日志文件失败: %v", err)
	}
	defer logFile.Close()

	err = logFirstRowFieldMapping(srcDB, srcTable, columns, logFile)
	if err != nil {
		log.Fatalf("迁移前字段映射校验失败: %v", err)
	}

	// 1. 全量+增量迁移 srcTable → dstTable
	var wg sync.WaitGroup
	segmentChan := make(chan time.Time, parallelism*2)
	results := make(chan migrationResult, parallelism*2)
	doneSegments := loadDoneSegments(doneSegmentsFile)
	for i := 0; i < parallelism; i++ {
		wg.Add(1)
		go worker(srcDB, dstDB, columns, segmentChan, results, &wg, srcTable, dstTable, timeField, doneSegments)
	}
	go processResults(results, logFile, minTime, maxTime)
	generateHourlySegmentsWithSkip(minTime, maxTime, segmentChan, doneSegments)
	close(segmentChan)
	wg.Wait()
	close(results)

	// 增量迁移
	for {
		newMin, newMax, err := getTimeRange(srcDB, srcTable, timeField, maxTime.Format("2006-01-02 15:04:05"))
		if err != nil {
			log.Fatalf("增量获取时间范围失败: %v", err)
		}
		if newMin.IsZero() || !newMax.After(maxTime) {
			log.Println("无新增数据，增量迁移完成")
			break
		}
		log.Printf("检测到新数据，增量迁移 %s ~ %s", newMin, newMax)
		var incWg sync.WaitGroup
		incChan := make(chan time.Time, parallelism*2)
		incResults := make(chan migrationResult, parallelism*2)
		doneSegments = loadDoneSegments(doneSegmentsFile)
		for i := 0; i < parallelism; i++ {
			incWg.Add(1)
			go worker(srcDB, dstDB, columns, incChan, incResults, &incWg, srcTable, dstTable, timeField, doneSegments)
		}
		go processResults(incResults, logFile, newMin, newMax)
		generateHourlySegmentsWithSkip(newMin, newMax, incChan, doneSegments)
		close(incChan)
		incWg.Wait()
		close(incResults)
		maxTime = newMax
	}

	// 2. rename 源表为 _bak
	err = renameSrcTableToBak(srcDB, srcTable)
	if err != nil {
		log.Fatalf("重命名源表失败: %v", err)
	}
	bakTable := srcTable + "_bak"

	// 3. 获取 _bak 最大时间戳
	var maxTimeInSrcTableBeforeRename time.Time
	rowBak := srcDB.Raw(fmt.Sprintf("SELECT max(%s) FROM %s", timeField, bakTable)).Row()
	if err := rowBak.Scan(&maxTimeInSrcTableBeforeRename); err != nil {
		log.Fatalf("获取 _bak 表最大时间戳失败: %v", err)
	}
	log.Printf("_bak 表最大时间戳: %s", maxTimeInSrcTableBeforeRename.Format("2006-01-02 15:04:05"))

	// 4. 补差：对比 _bak 和 dstTable，将 _bak 新数据补到 dstTable
	bakCols, err := getTableColumns(srcDB, bakTable)
	if err != nil {
		log.Fatalf("获取 _bak 表结构失败: %v", err)
	}
	colNames := []string{}
	for _, c := range bakCols {
		if isIgnoredField(c.Name) {
			continue
		}
		colNames = append(colNames, c.Name) // 不带反引号
	}
	colList := ""
	for i, name := range colNames {
		if i > 0 {
			colList += ","
		}
		colList += "`" + name + "`"
	}
	// 查询 _bak 表 timeField = maxTimeInSrcTableBeforeRename 的所有数据
	var bakRowsAtMax []map[string]interface{}
	err = srcDB.Table(bakTable).Select(colList).Where(fmt.Sprintf("%s = ?", timeField), maxTimeInSrcTableBeforeRename).Find(&bakRowsAtMax).Error
	if err != nil {
		log.Fatalf("_bak 表 timeField=maxTimeInSrcTableBeforeRename 查询失败: %v", err)
	}
	// 查询目标表（dstTable） timeField = maxTimeInSrcTableBeforeRename 的所有数据
	var dstRowsAtMax []map[string]interface{}
	err = dstDB.Table(dstTable).Select(colList).Where(fmt.Sprintf("%s = ?", timeField), maxTimeInSrcTableBeforeRename).Find(&dstRowsAtMax).Error
	if err != nil {
		log.Fatalf("目标表 timeField=maxTimeInSrcTableBeforeRename 查询失败: %v", err)
	}
	// 用 reflect.DeepEqual 做全量行对比
	var needInsertRows []map[string]interface{}
	for _, bakRow := range bakRowsAtMax {
		found := false
		for _, dstRow := range dstRowsAtMax {
			if mapsEqual(bakRow, dstRow) {
				found = true
				break
			}
		}
		if !found {
			needInsertRows = append(needInsertRows, bakRow)
		}
	}
	if len(needInsertRows) > 0 {
		log.Printf("_bak 表 timeField=%s 需补差 %d 条", maxTimeInSrcTableBeforeRename.Format("2006-01-02 15:04:05"), len(needInsertRows))
		if err := dstDB.Table(dstTable).CreateInBatches(needInsertRows, 1000).Error; err != nil {
			log.Fatalf("_bak 表 timeField=maxTimeInSrcTableBeforeRename 补差写入失败: %v", err)
		}
		log.Printf("_bak 表 timeField=maxTimeInSrcTableBeforeRename 补差写入完成")
	} else {
		log.Printf("_bak 表 timeField=%s 无需补差", maxTimeInSrcTableBeforeRename.Format("2006-01-02 15:04:05"))
	}
	// 处理 _bak 表 timeField > maxTimeInSrcTableBeforeRename 的数据（批量迁移）
	bakMin, bakMax, err := getTimeRange(srcDB, bakTable, timeField, maxTimeInSrcTableBeforeRename.Add(time.Nanosecond).Format("2006-01-02 15:04:05"))
	if err != nil {
		log.Fatalf("_bak 表兜底增量时间范围获取失败: %v", err)
	}
	if !bakMin.IsZero() && bakMax.After(maxTimeInSrcTableBeforeRename) {
		log.Printf("_bak 表有新数据: %s ~ %s，开始兜底增量迁移", bakMin, bakMax)
		var bakWg sync.WaitGroup
		bakChan := make(chan time.Time, parallelism*2)
		bakResults := make(chan migrationResult, parallelism*2)
		for i := 0; i < parallelism; i++ {
			bakWg.Add(1)
			go worker(srcDB, dstDB, bakCols, bakChan, bakResults, &bakWg, bakTable, dstTable, timeField, nil)
		}
		go processResults(bakResults, logFile, bakMin, bakMax)
		generateHourlySegmentsWithSkip(bakMin, bakMax, bakChan, nil)
		close(bakChan)
		bakWg.Wait()
		close(bakResults)
		log.Printf("_bak 表兜底增量迁移完成")
	} else {
		log.Printf("_bak 表无新数据，无需兜底增量")
	}

	// 5. rename 目标表为 srcTable
	err = renameDstTableToSrc(dstDB, dstTable, srcTable)
	if err != nil {
		log.Fatalf("重命名目标表失败: %v", err)
	}
	log.Println("最终切换完成，迁移流程结束")

	// 迁移完成后重命名 done_segments.txt，避免下次任务误用
	err = renameDoneSegmentsFile(doneSegmentsFile)
	if err != nil {
		log.Printf("重命名 %s 失败: %v", doneSegmentsFile, err)
	} else {
		log.Printf("%s 已重命名，避免下次任务误用", doneSegmentsFile)
	}
}

// 断点续传记录
func loadDoneSegments(filename string) map[string]bool {
	done := map[string]bool{}
	f, err := os.Open(filename)
	if err != nil {
		return done
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		done[scanner.Text()] = true
	}
	return done
}

func saveDoneSegment(seg string) {
	doneSegmentMu.Lock()
	defer doneSegmentMu.Unlock()
	f, err := os.OpenFile(doneSegmentsFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("记录断点失败: %v", err)
		return
	}
	defer f.Close()
	f.WriteString(seg + "\n")
}

// 迁移完成后重命名 done_segments 文件，后缀为当前时间戳，保证唯一
func renameDoneSegmentsFile(filename string) error {
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return nil // 文件不存在无需处理
	}
	timestamp := time.Now().Format("20060102_150405")
	base := strings.TrimSuffix(filename, ".txt")
	newName := fmt.Sprintf("%s_%s.txt", base, timestamp)
	return os.Rename(filename, newName)
}

// GORM版本的表结构获取
func getTableColumns(db *gorm.DB, table string) ([]columnInfo, error) {
	var createSQL string
	err := db.Raw(fmt.Sprintf("SHOW CREATE TABLE %s", table)).Scan(&createSQL).Error
	if err != nil {
		return nil, err
	}
	lines := strings.Split(createSQL, "\n")
	cols := []columnInfo{}
	// 优化后的字段正则：支持嵌套括号、引号、逗号、空格、所有 ClickHouse 字段类型
	// 字段名：反引号或无反引号，字段类型：允许嵌套括号、引号、逗号、空格、数字、字母等，直到遇到逗号或行尾
	fieldRe := regexp.MustCompile(`(?m)^\s*(?:` + "`" + `)?([a-zA-Z0-9_]+)(?:` + "`" + `)?\s+((?:[a-zA-Z0-9_]+(?:\([^)]*\))?)(?:\s*(?:Nullable|Array|Enum|Decimal|LowCardinality|FixedString|DateTime64|DateTime|Date|UUID|Int\d*|UInt\d*|Float\d*|String|Map|Tuple|IPv4|IPv6|Enum8|Enum16|Decimal\([^)]*\))*)?(?:\([^)]*\))?(?:\s*('[^']*'|"[^"]*")*)*)`)
	inFields := false
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "(") {
			inFields = true
			continue
		}
		if strings.HasPrefix(line, ")") || strings.HasPrefix(line, "ENGINE") {
			break
		}
		if !inFields {
			continue
		}
		if strings.HasPrefix(line, "INDEX") ||
			strings.HasPrefix(line, "PRIMARY") ||
			strings.HasPrefix(line, "ORDER") ||
			strings.HasPrefix(line, "SETTINGS") {
			continue
		}
		if m := fieldRe.FindStringSubmatch(line); m != nil {
			cols = append(cols, columnInfo{Name: m[1], Type: strings.TrimSpace(m[2])})
		}
	}
	return cols, nil
}

// GORM版本的时间范围获取
func getTimeRange(db *gorm.DB, table, timeField, start string) (time.Time, time.Time, error) {
	var minT, maxT time.Time
	row := db.Raw(fmt.Sprintf("SELECT min(%s), max(%s) FROM %s WHERE %s >= ?", timeField, timeField, table, timeField), start).Row()
	if err := row.Scan(&minT, &maxT); err != nil {
		return time.Time{}, time.Time{}, err
	}
	if minT.IsZero() || maxT.IsZero() {
		return time.Time{}, time.Time{}, nil
	}
	return minT, maxT, nil
}

func checkTimeField(cols []columnInfo, field string) bool {
	if isIgnoredField(field) {
		return false
	}
	for _, c := range cols {
		t := strings.ToLower(c.Type)
		if c.Name == field && (strings.HasPrefix(t, "datetime") || strings.HasPrefix(t, "date") || strings.HasPrefix(t, "timestamp")) {
			return true
		}
	}
	return false
}

func generateHourlySegmentsWithSkip(minTime, maxTime time.Time, segmentChan chan<- time.Time, doneSegments map[string]bool) {
	minTime = minTime.Truncate(time.Hour)
	maxTime = maxTime.Truncate(time.Hour).Add(time.Hour)
	for t := minTime; t.Before(maxTime); t = t.Add(time.Hour) {
		segKey := t.Format("2006-01-02 15:04:05")
		if doneSegments != nil && doneSegments[segKey] {
			continue // 跳过已完成
		}
		segmentChan <- t
	}
}

func worker(srcDB, dstDB *gorm.DB, columns []columnInfo, segmentChan <-chan time.Time, results chan<- migrationResult, wg *sync.WaitGroup, srcTable, dstTable, timeField string, doneSegments map[string]bool) {
	defer wg.Done()
	colNames := []string{}
	colIndexes := []int{}
	for i, c := range columns {
		if isIgnoredField(c.Name) {
			continue
		}
		colNames = append(colNames, "`"+c.Name+"`")
		colIndexes = append(colIndexes, i)
	}
	colList := strings.Join(colNames, ",")
	placeholders := strings.Repeat("?,", len(colNames))
	if len(placeholders) > 0 {
		placeholders = placeholders[:len(placeholders)-1]
	}
	for startHour := range segmentChan {
		segKey := startHour.Format("2006-01-02 15:04:05")
		if doneSegments != nil && doneSegments[segKey] {
			continue
		}
		endHour := startHour.Add(time.Hour)
		result := migrationResult{SegmentStart: startHour, SegmentEnd: endHour}
		startTime := time.Now()
		rowsRead, rowsWritten, err := migrateSegment(srcDB, dstDB, columns, colIndexes, srcTable, dstTable, timeField, startHour, endHour, colList, placeholders)
		result.Duration = time.Since(startTime)
		result.RowsRead = rowsRead
		result.RowsWritten = rowsWritten
		result.Error = err
		results <- result
		if result.Error == nil {
			saveDoneSegment(segKey)
		}
	}
}

func migrateSegment(srcDB, dstDB *gorm.DB, columns []columnInfo, colIndexes []int, srcTable, dstTable, timeField string, startHour, endHour time.Time, colList, placeholders string) (int, int, error) {
	// 用明确字段名替换 SELECT *，并过滤 ignoreFields
	fieldNames := []string{}
	for _, c := range columns {
		if isIgnoredField(c.Name) {
			continue
		}
		fieldNames = append(fieldNames, c.Name)
	}
	selectFields := strings.Join(fieldNames, ",")
	q := fmt.Sprintf("SELECT %s FROM %s WHERE %s >= ? AND %s < ? ORDER BY %s", selectFields, srcTable, timeField, timeField, timeField)
	rows, err := srcDB.Raw(q, startHour, endHour).Rows()
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()
	batchSize := 10000
	vals := make([][]interface{}, 0, batchSize)
	cols := make([]interface{}, len(columns))
	rowPtrs := make([]interface{}, len(columns))
	rowsRead, rowsWritten := 0, 0
	for rows.Next() {
		for i := range cols {
			rowPtrs[i] = &cols[i]
		}
		if err := rows.Scan(rowPtrs...); err != nil {
			return rowsRead, rowsWritten, err
		}
		rowCopy := make([]interface{}, len(colIndexes))
		for j, idx := range colIndexes {
			rowCopy[j] = cols[idx]
		}
		vals = append(vals, rowCopy)
		rowsRead++
		if len(vals) == batchSize {
			w, err := insertBatch(dstDB, dstTable, vals, colList, placeholders)
			if err != nil {
				return rowsRead, rowsWritten, err
			}
			rowsWritten += w
			vals = vals[:0]
		}
	}
	if len(vals) > 0 {
		w, err := insertBatch(dstDB, dstTable, vals, colList, placeholders)
		if err != nil {
			return rowsRead, rowsWritten, err
		}
		rowsWritten += w
	}
	return rowsRead, rowsWritten, nil
}

// GORM版本的insertBatch，使用 CreateInBatches 优化批量插入
func insertBatch(db *gorm.DB, table string, vals [][]interface{}, colList, placeholders string) (int, error) {
	if len(vals) == 0 {
		return 0, nil
	}
	inserted := 0
	batchSize := 1000 // GORM 推荐批量大小
	// 构造 map 列表用于 GORM 批量插入
	columns := strings.Split(strings.ReplaceAll(colList, "`", ""), ",")
	records := make([]map[string]interface{}, 0, len(vals))
	for _, row := range vals {
		rec := map[string]interface{}{}
		for i, col := range columns {
			rec[strings.TrimSpace(col)] = row[i]
		}
		records = append(records, rec)
	}
	for i := 0; i < len(records); i += batchSize {
		end := i + batchSize
		if end > len(records) {
			end = len(records)
		}
		batch := records[i:end]
		if err := db.Table(table).CreateInBatches(batch, batchSize).Error; err != nil {
			log.Printf("批量写入失败: %v", err)
			return inserted, err
		}
		inserted += len(batch)
	}
	return inserted, nil
}

func processResults(results <-chan migrationResult, logFile *os.File, minTime, maxTime time.Time) {
	totalSegments := 0
	processedSegments := 0
	totalRows := 0
	minTime = minTime.Truncate(time.Hour)
	maxTime = maxTime.Truncate(time.Hour).Add(time.Hour)
	for t := minTime; t.Before(maxTime); t = t.Add(time.Hour) {
		totalSegments++
	}
	for result := range results {
		processedSegments++
		totalRows += result.RowsRead
		if result.Error != nil {
			log.Printf("Segment %s failed: %v", result.SegmentStart.Format(time.RFC3339), result.Error)
		} else {
			log.Printf("Segment %s completed: %d rows in %v", result.SegmentStart.Format(time.RFC3339), result.RowsRead, result.Duration)
		}
		logEntry := map[string]interface{}{
			"segment_start": result.SegmentStart.Format(time.RFC3339),
			"segment_end":   result.SegmentEnd.Format(time.RFC3339),
			"rows_read":     result.RowsRead,
			"rows_written":  result.RowsWritten,
			"duration_ms":   result.Duration.Milliseconds(),
			"error":         "",
		}
		if result.Error != nil {
			logEntry["error"] = result.Error.Error()
		}
		entryJSON, err := json.Marshal(logEntry)
		if err == nil {
			logFile.Write(entryJSON)
			logFile.WriteString("\n")
		}
		segmentProgress := float64(processedSegments) / float64(totalSegments) * 100
		rowProgress := segmentProgress
		log.Printf("Overall progress: Segments %.1f%%, Rows %.1f%%", segmentProgress, rowProgress)
	}
}

// GORM版本的字段顺序/类型一致性校验
func compareTableColumns(srcDB, dstDB *gorm.DB, srcTable, dstTable string) error {
	srcCols, err := getTableColumns(srcDB, srcTable)
	if err != nil {
		return fmt.Errorf("获取源表结构失败: %v", err)
	}
	dstCols, err := getTableColumns(dstDB, dstTable)
	if err != nil {
		return fmt.Errorf("获取目标表结构失败: %v", err)
	}
	if len(srcCols) != len(dstCols) {
		return fmt.Errorf("源表和目标表字段数量不一致")
	}
	for i := range srcCols {
		if srcCols[i].Name != dstCols[i].Name || srcCols[i].Type != dstCols[i].Type {
			return fmt.Errorf("字段不一致: 源表[%s %s], 目标表[%s %s]", srcCols[i].Name, srcCols[i].Type, dstCols[i].Name, dstCols[i].Type)
		}
	}
	return nil
}

// 新增：rename 源表为 _bak
func renameSrcTableToBak(srcDB *gorm.DB, srcTable string) error {
	bakTable := srcTable + "_bak"
	var renameSQL string
	if isSrcDistributed && clusterName != "" {
		renameSQL = fmt.Sprintf("RENAME TABLE %s TO %s ON CLUSTER %s", srcTable, bakTable, clusterName)
	} else {
		renameSQL = fmt.Sprintf("RENAME TABLE %s TO %s", srcTable, bakTable)
	}
	if err := srcDB.Exec(renameSQL).Error; err != nil {
		return fmt.Errorf("重命名源表失败: %w", err)
	}
	return nil
}

// 新增：rename 目标表为 srcTable
func renameDstTableToSrc(dstDB *gorm.DB, dstTable, srcTable string) error {
	var renameSQL string
	if isDstDistributed && clusterName != "" {
		renameSQL = fmt.Sprintf("RENAME TABLE %s TO %s ON CLUSTER %s", dstTable, srcTable, clusterName)
	} else {
		renameSQL = fmt.Sprintf("RENAME TABLE %s TO %s", dstTable, srcTable)
	}
	if err := dstDB.Exec(renameSQL).Error; err != nil {
		return fmt.Errorf("重命名目标表失败: %w", err)
	}
	return nil
}

// 顺序无关、支持嵌套的 map 等价比较
func mapsEqual(a, b interface{}) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	aa, aok := a.(map[string]interface{})
	bb, bok := b.(map[string]interface{})
	if aok && bok {
		if len(aa) != len(bb) {
			return false
		}
		for k, av := range aa {
			bv, ok := bb[k]
			if !ok {
				return false
			}
			if !mapsEqual(av, bv) {
				return false
			}
		}
		return true
	}
	// 支持 []interface{} 的顺序比较
	aaArr, aArrOk := a.([]interface{})
	bbArr, bArrOk := b.([]interface{})
	if aArrOk && bArrOk {
		if len(aaArr) != len(bbArr) {
			return false
		}
		for i := range aaArr {
			if !mapsEqual(aaArr[i], bbArr[i]) {
				return false
			}
		}
		return true
	}
	// 其它类型直接用 reflect.DeepEqual
	return reflect.DeepEqual(a, b)
}

// 迁移前抽取一条数据，按迁移字段顺序和拼接方式，打印字段名、类型、值，写入日志
func logFirstRowFieldMapping(srcDB *gorm.DB, srcTable string, columns []columnInfo, logFile *os.File) error {
	colNames := []string{}
	colTypes := []string{}
	for _, c := range columns {
		if isIgnoredField(c.Name) {
			continue
		}
		colNames = append(colNames, c.Name)
		colTypes = append(colTypes, c.Type)
	}
	colList := ""
	for i, name := range colNames {
		if i > 0 {
			colList += ", "
		}
		colList += "`" + name + "`"
	}
	var row = make(map[string]interface{})
	err := srcDB.Table(srcTable).Select(colList).Order("rand()").Limit(1).Find(&row).Error
	if err != nil {
		return fmt.Errorf("源表抽取随机行失败: %v", err)
	}
	if len(row) == 0 {
		logFile.WriteString("源表无数据，跳过首行字段映射校验\n")
		return nil
	}
	logFile.WriteString("迁移前字段顺序/类型/值映射校验如下：\n")
	for i, name := range colNames {
		val := row[name]
		logFile.WriteString(fmt.Sprintf("字段%d: %s\t类型: %s\t值: %v\n", i+1, name, colTypes[i], val))
	}
	return nil
}
