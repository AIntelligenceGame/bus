package main

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
)

// TableSchemaDetails 表示一张表的结构信息。
type TableSchemaDetails struct {
	Table       string              `json:"table"`
	Database    string              `json:"database,omitempty"`
	Columns     []SchemaColumn      `json:"columns"`
	PrimaryKey  []string            `json:"primary_key,omitempty"`
	Indexes     []SchemaIndex       `json:"indexes,omitempty"`
	ForeignKeys []SchemaForeignKey  `json:"foreign_keys,omitempty"`
	Comment     string              `json:"comment,omitempty"`
}

// SchemaColumn 表示表中的一列。
type SchemaColumn struct {
	Name          string `json:"name"`
	Type          string `json:"type"`
	Nullable      bool   `json:"nullable"`
	PrimaryKey    bool   `json:"primary_key,omitempty"`
	AutoIncrement bool   `json:"auto_increment,omitempty"`
	HasDefault    bool   `json:"has_default,omitempty"`
	Default       string `json:"default,omitempty"`
	OnUpdate      string `json:"on_update,omitempty"`
	Comment       string `json:"comment,omitempty"`
	Extra         string `json:"extra,omitempty"`
	Generated     string `json:"generated,omitempty"`
}

// SchemaIndex 表示表上的一个索引。
type SchemaIndex struct {
	Name       string   `json:"name"`
	Columns    []string `json:"columns"`
	Unique     bool     `json:"unique,omitempty"`
	Primary    bool     `json:"primary,omitempty"`
	FullText   bool     `json:"fulltext,omitempty"`
	Spatial    bool     `json:"spatial,omitempty"`
	Type       string   `json:"type,omitempty"`
	Expression string   `json:"expression,omitempty"`
}

// SchemaForeignKey 表示表上的一个外键。
type SchemaForeignKey struct {
	Name              string   `json:"name"`
	Columns           []string `json:"columns"`
	ReferencedTable   string   `json:"referenced_table"`
	ReferencedColumns []string `json:"referenced_columns,omitempty"`
	OnDelete          string   `json:"on_delete,omitempty"`
	OnUpdate          string   `json:"on_update,omitempty"`
}

// describeTable 从 catalog 读取表的结构信息。
// 与 AST 层的 CREATE TABLE 解析不同，这里读的是数据库中真实存在的表。
func describeTable(ctx *sql.Context, catalog sql.Catalog, dbName, tableName string) (*TableSchemaDetails, error) {
	db, err := catalog.Database(ctx, dbName)
	if err != nil {
		return nil, err
	}
	table, ok, err := db.GetTableInsensitive(ctx, tableName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("table not found: %s.%s", dbName, tableName)
	}

	details := &TableSchemaDetails{
		Table:       table.Name(),
		Database:    dbName,
		Columns:     []SchemaColumn{},
		Indexes:     []SchemaIndex{},
		ForeignKeys: []SchemaForeignKey{},
	}

	schema := table.Schema()
	for _, col := range schema {
		colInfo := SchemaColumn{
			Name:          col.Name,
			Type:          col.Type.String(),
			Nullable:      col.Nullable,
			PrimaryKey:    col.PrimaryKey,
			AutoIncrement: col.AutoIncrement,
			Comment:       col.Comment,
			Extra:         col.Extra,
		}
		if col.Default != nil {
			colInfo.HasDefault = true
			colInfo.Default = col.Default.String()
		}
		if col.OnUpdate != nil {
			colInfo.OnUpdate = col.OnUpdate.String()
		}
		if col.Generated != nil {
			colInfo.Generated = col.Generated.String()
		}
		if col.PrimaryKey {
			details.PrimaryKey = append(details.PrimaryKey, col.Name)
		}
		details.Columns = append(details.Columns, colInfo)
	}

	// 索引：表实现了 IndexAddressable 才能拿到
	if indexable, ok := table.(sql.IndexAddressable); ok {
		indexes, err := indexable.GetIndexes(ctx)
		if err == nil {
			for _, idx := range indexes {
				if idx.IsGenerated() {
					continue
				}
				details.Indexes = append(details.Indexes, SchemaIndex{
					Name:     idx.ID(),
					Columns:  idx.Expressions(),
					Unique:   idx.IsUnique(),
					Primary:  strings.EqualFold(idx.ID(), "primary"),
					FullText: idx.IsFullText(),
					Spatial:  idx.IsSpatial(),
					Type:     idx.IndexType(),
				})
			}
		}
	}

	// 外键：表实现了 ForeignKeyTable 才能拿到
	if fkTable, ok := table.(sql.ForeignKeyTable); ok {
		fks, err := fkTable.GetDeclaredForeignKeys(ctx)
		if err == nil {
			for _, fk := range fks {
				details.ForeignKeys = append(details.ForeignKeys, SchemaForeignKey{
					Name:              fk.Name,
					Columns:           fk.Columns,
					ReferencedTable:   fk.ParentTable,
					ReferencedColumns: fk.ParentColumns,
					OnDelete:          string(fk.OnDelete),
					OnUpdate:          string(fk.OnUpdate),
				})
			}
		}
	}

	return details, nil
}
