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
	ignoreFields     []string // 新增：忽略字段
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
	// 新增：支持多次指定 --ignore-field
	flag.Func("ignore-field", "忽略校验和插入的字段，可指定多次", func(s string) error {
		ignoreFields = append(ignoreFields, s)
		return nil
	})
}

// 判断字段名是否在忽略列表
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

func main() {
	flag.Parse()
	if srcTable == "" || dstTable == "" || timeField == "" {
		log.Fatal("src-table、dst-table、time-field 参数必填")
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

	logFile, err := os.Create("log.json")
	if err != nil {
		log.Fatalf("创建日志文件失败: %v", err)
	}
	defer logFile.Close()

	var wg sync.WaitGroup
	segmentChan := make(chan time.Time, parallelism*2)
	results := make(chan migrationResult, parallelism*2)
	doneSegments := loadDoneSegments()
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
		doneSegments = loadDoneSegments()
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

	// rename 表
	err = renameTables(srcDB, dstDB, srcTable, dstTable)
	if err != nil {
		log.Fatalf("重命名表失败: %v", err)
	}
	log.Println("迁移和重命名完成")
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
	// 字段正则：兼容有无反引号，类型支持复杂内容（如Nullable(DateTime), String, UInt64等）
	fieldRe := regexp.MustCompile(`(?m)^\s*(?:` + "`" + `)?([a-zA-Z0-9_]+)(?:` + "`" + `)?\s+([a-zA-Z0-9()]+)`) // 允许括号和下划线
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
			cols = append(cols, columnInfo{Name: m[1], Type: m[2]})
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
		if c.Name == field && strings.HasPrefix(strings.ToLower(c.Type), "datetime") {
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

type migrationResult struct {
	SegmentStart time.Time
	SegmentEnd   time.Time
	RowsRead     int
	RowsWritten  int
	Duration     time.Duration
	Error        error
}

// GORM版本的worker
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

// GORM版本的migrateSegment
func migrateSegment(srcDB, dstDB *gorm.DB, columns []columnInfo, colIndexes []int, srcTable, dstTable, timeField string, startHour, endHour time.Time, colList, placeholders string) (int, int, error) {
	// 用明确字段名替换 SELECT *
	fieldNames := []string{}
	for _, c := range columns {
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

// GORM版本的insertBatch
func insertBatch(db *gorm.DB, table string, vals [][]interface{}, colList, placeholders string) (int, error) {
	if len(vals) == 0 {
		return 0, nil
	}
	inserted := 0
	q := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, colList, placeholders)
	for _, row := range vals {
		retry := 0
		for {
			if err := db.Exec(q, row...).Error; err != nil {
				retry++
				if retry < 3 {
					log.Printf("写入失败重试: %v", err)
					time.Sleep(2 * time.Second)
					continue
				} else {
					log.Printf("跳过异常行: %v", err)
					break
				}
			}
			inserted++
			break
		}
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

// GORM版本的rename
func renameTables(srcDB, dstDB *gorm.DB, srcTable, dstTable string) error {
	bakTable := srcTable + "_bak"
	var renameSrc, renameDst string
	if isSrcDistributed && clusterName != "" {
		renameSrc = fmt.Sprintf("RENAME TABLE %s TO %s ON CLUSTER %s", srcTable, bakTable, clusterName)
	} else if isSrcDistributed || clusterName != "" {
		return fmt.Errorf("分布式表rename必须指定集群名")
	} else {
		renameSrc = fmt.Sprintf("RENAME TABLE %s TO %s", srcTable, bakTable)
	}
	if isDstDistributed && clusterName != "" {
		renameDst = fmt.Sprintf("RENAME TABLE %s TO %s ON CLUSTER %s", dstTable, srcTable, clusterName)
	} else if isDstDistributed || clusterName != "" {
		return fmt.Errorf("分布式表rename必须指定集群名")
	} else {
		renameDst = fmt.Sprintf("RENAME TABLE %s TO %s", dstTable, srcTable)
	}
	if err := srcDB.Exec(renameSrc).Error; err != nil {
		return fmt.Errorf("重命名源表失败: %w", err)
	}
	if err := dstDB.Exec(renameDst).Error; err != nil {
		return fmt.Errorf("重命名目标表失败: %w", err)
	}
	return nil
}

// 断点续传记录
func loadDoneSegments() map[string]bool {
	done := map[string]bool{}
	f, err := os.Open("done_segments.txt")
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
	f, err := os.OpenFile("done_segments.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("记录断点失败: %v", err)
		return
	}
	defer f.Close()
	f.WriteString(seg + "\n")
}
