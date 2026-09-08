package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/AIntelligenceGame/bus/logger"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"go.uber.org/zap"
)

type MyEventHandler struct {
	canal.DummyEventHandler
	tables []string
}

type C_Sharp_Arg struct {
	Addr      string
	User      string
	Password  string
	Database  string
	Tables    []string
	TAddr     string
	TUser     string
	TPassword string
	TDatabase string
}

func (h *MyEventHandler) hasTable(name string) bool {
	if len(h.tables) == 0 {
		return true
	}
	for _, t := range h.tables {
		if t == name {
			return true
		}
	}
	return false
}

func (h *MyEventHandler) OnRow(ev *canal.RowsEvent) error {
	if !h.hasTable(ev.Table.Name) {
		return nil
	}
	zap.L().Info("binlog row event",
		zap.String("schema", string(ev.Table.Schema)),
		zap.String("table", ev.Table.Name),
		zap.String("action", ev.Action),
		zap.Int("rows_count", len(ev.Rows)),
	)
	for _, row := range ev.Rows {
		for columnIndex, currColumn := range ev.Table.Columns {
			zap.L().Debug("row column",
				zap.String("column", currColumn.Name),
				zap.Int("index", columnIndex),
				zap.Any("value", row[columnIndex]),
			)
		}
	}
	return nil
}

func (h *MyEventHandler) OnTableChanged(header *replication.EventHeader, schema string, table string) error {
	zap.L().Info("table changed",
		zap.String("schema", schema),
		zap.String("table", table),
	)
	return nil
}

func (h *MyEventHandler) OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, force bool) error {
	zap.L().Info("binlog position synced",
		zap.String("file", pos.Name),
		zap.Uint32("pos", pos.Pos),
		zap.Bool("force", force),
	)
	return nil
}

func (h *MyEventHandler) OnRotate(header *replication.EventHeader, r *replication.RotateEvent) error {
	zap.L().Info("binlog rotate",
		zap.Uint64("position", r.Position),
		zap.String("next_log", string(r.NextLogName)),
	)
	return nil
}

func (h *MyEventHandler) OnRowsQueryEvent(r *replication.RowsQueryEvent) error {
	zap.L().Info("rows query event",
		zap.String("query", string(r.Query)),
	)
	return nil
}

func (h *MyEventHandler) OnDDL(header *replication.EventHeader, nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	fields := []zap.Field{
		zap.String("binlog_file", nextPos.Name),
		zap.Uint32("binlog_pos", nextPos.Pos),
		zap.String("schema", string(queryEvent.Schema)),
		zap.String("query", string(queryEvent.Query)),
		zap.Uint32("exec_time", queryEvent.ExecutionTime),
		zap.Uint32("slave_proxy_id", queryEvent.SlaveProxyID),
	}
	if queryEvent.StatusVars != nil {
		fields = append(fields, zap.ByteString("status_vars", queryEvent.StatusVars[:]))
	}
	zap.L().Info("ddl event", fields...)
	return nil
}

func (h *MyEventHandler) String() string {
	return "MyEventHandler"
}

func (h *MyEventHandler) OnXID(header *replication.EventHeader, pos mysql.Position) error {
	zap.L().Info("transaction XID",
		zap.String("file", pos.Name),
		zap.Uint32("pos", pos.Pos),
	)
	return nil
}

func (h *MyEventHandler) OnGTID(header *replication.EventHeader, set mysql.BinlogGTIDEvent) error {
	zap.L().Info("GTID event",
		zap.Any("gtid", set),
	)
	return nil
}

func DefaultExport(csa *C_Sharp_Arg) {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = csa.Addr
	cfg.User = csa.User
	cfg.Password = csa.Password
	cfg.Dump.TableDB = csa.Database
	cfg.Dump.Tables = csa.Tables

	c, err := canal.NewCanal(cfg)
	if err != nil {
		zap.L().Error("failed to create canal", zap.Error(err))
		return
	}

	c.SetEventHandler(&MyEventHandler{tables: csa.Tables})
	zap.L().Info("canal started",
		zap.String("addr", csa.Addr),
		zap.String("database", csa.Database),
		zap.Strings("tables", csa.Tables),
	)
	c.Run()
}

func main() {
	var (
		mysqlHost      string
		mysqlPort      string
		mysqlUser      string
		mysqlPassword  string
		mysqlDatabase  string
		mysqlTables    string
		targetAddr     string
		targetUser     string
		targetPassword string
		targetDatabase string
	)

	flag.StringVar(&mysqlHost, "mysql-host", "127.0.0.1", "MySQL host address")
	flag.StringVar(&mysqlPort, "mysql-port", "3306", "MySQL port")
	flag.StringVar(&mysqlUser, "mysql-user", "root", "MySQL username")
	flag.StringVar(&mysqlPassword, "mysql-password", "", "MySQL password")
	flag.StringVar(&mysqlDatabase, "mysql-database", "", "MySQL database name to sync")
	flag.StringVar(&mysqlTables, "mysql-tables", "", "Comma-separated table names to sync (empty=all)")
	flag.StringVar(&targetAddr, "target-addr", "", "Target MySQL host:port")
	flag.StringVar(&targetUser, "target-user", "", "Target MySQL username")
	flag.StringVar(&targetPassword, "target-password", "", "Target MySQL password")
	flag.StringVar(&targetDatabase, "target-database", "", "Target MySQL database")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "MySQL Data Transfer Service (DTS)\n\n")
		fmt.Fprintf(os.Stderr, "Usage: %s [flags]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Flags:\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	_ = logger.InitLogger(logger.LoggerConfig{})

	tables := []string{}
	if mysqlTables != "" {
		for _, t := range strings.Split(mysqlTables, ",") {
			t = strings.TrimSpace(t)
			if t != "" {
				tables = append(tables, t)
			}
		}
	}

	if mysqlDatabase == "" {
		zap.L().Error("mysql-database is required")
		flag.Usage()
		os.Exit(1)
	}

	zap.L().Info("starting DTS service",
		zap.String("mysql_host", mysqlHost),
		zap.String("mysql_port", mysqlPort),
		zap.String("mysql_user", mysqlUser),
		zap.String("mysql_database", mysqlDatabase),
		zap.Strings("mysql_tables", tables),
	)

	csa_in := C_Sharp_Arg{
		Addr:      fmt.Sprintf("%s:%s", mysqlHost, mysqlPort),
		User:      mysqlUser,
		Password:  mysqlPassword,
		Database:  mysqlDatabase,
		Tables:    tables,
		TAddr:     targetAddr,
		TUser:     targetUser,
		TPassword: targetPassword,
		TDatabase: targetDatabase,
	}
	DefaultExport(&csa_in)
}

/*
这段注释不要删掉，生成dll文件和调用dll参考使用
	// go build -buildmode=c-shared -o mysqlslave.dll mysql57slave/slave.go
	// [DllImport("mysqlslave.dll", EntryPoint="DefaultExport")]
	// static extern void DefaultExport();

	// [DllImport("exportgo.dll", EntryPoint="Sum")]
	// static extern int Sum(int a, int b);

	// static void Main()
	// {
	// 	Console.WriteLine("Hello World!");
	// 	PrintBye();

	// 	Console.WriteLine(Sum(33, 22));
	// }
*/
