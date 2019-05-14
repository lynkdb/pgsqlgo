// Copyright 2018 Eryx <evorui аt gmail dοt com>, All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pgsqlgo

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/lynkdb/iomix/rdb"
	"github.com/lynkdb/iomix/rdb/modeler"
)

type DialectModeler struct {
	base rdb.Connector
}

func (dc *DialectModeler) IndexSync(tableName string, index *modeler.Index) error {

	pre, action := "", ""
	switch index.Type {

	case modeler.IndexTypePrimaryKey:
		pre = "pre"

	case modeler.IndexTypeIndex:
		action = "INDEX"
		pre = "idx"

	case modeler.IndexTypeUnique:
		action = "UNIQUE INDEX"
		pre = "uni"

	default:
		return errors.New("Invalid Index Type Add")
	}

	idx_name := fmt.Sprintf("%s_%s__%s", pre, tableName, strings.ToLower(strings.Join(index.Cols, "_")))

	// CREATE UNIQUE INDEX user_uid_uni ON "user" (uid)
	sql := ""
	if index.Type == modeler.IndexTypePrimaryKey {
		sql = fmt.Sprintf("ALTER TABLE %s.public.%s ADD CONSTRAINT pri_%s__%s PRIMARY KEY (%s)",
			dc.base.DBName(), tableName, tableName, strings.ToLower(strings.Join(index.Cols, "_")), strings.Join(index.Cols, ","))
	} else {
		sql = fmt.Sprintf("CREATE %s %s ON %s.public.%s (%s)",
			action, idx_name, dc.base.DBName(), tableName, strings.Join(index.Cols, ","))
	}

	//fmt.Println("IndexSync", sql)

	_, err := dc.base.ExecRaw(sql)

	return err
}

func (dc *DialectModeler) IndexDel(tableName string, index *modeler.Index) error {

	// PRIMARY KEY can be modified, can not be deleted
	if index.Type == modeler.IndexTypePrimaryKey {
		return nil
	}

	pre := ""
	switch index.Type {

	case modeler.IndexTypeIndex:
		pre = "idx"

	case modeler.IndexTypeUnique:
		pre = "uni"

	default:
		return errors.New("Invalid Index Type Del")
	}

	sql := fmt.Sprintf("DROP INDEX %s_%s__%s",
		pre, tableName, strings.ToLower(strings.Join(index.Cols, "_")),
	)
	//fmt.Println("IndexDel", sql)

	_, err := dc.base.ExecRaw(sql)

	return err
}

func (dc *DialectModeler) IndexSet(tableName string, index *modeler.Index) error {

	sql := ""
	pre := ""

	switch index.Type {
	case modeler.IndexTypePrimaryKey:
		pre = "pri"
	case modeler.IndexTypeIndex:
		pre = "idx"
	case modeler.IndexTypeUnique:
		pre = "uni"

	default:
		return errors.New("Invalid Index Type Set")
	}

	idx_name := fmt.Sprintf("%s_%s__%s", pre, tableName, strings.ToLower(strings.Join(index.Cols, "_")))

	switch index.Type {
	case modeler.IndexTypePrimaryKey:

	case modeler.IndexTypeIndex:
		sql = fmt.Sprintf("DROP INDEX IF EXISTS %s; CREATE INDEX %s ON %s.%s (%s)",
			idx_name, idx_name, dc.base.DBName(), tableName,
			strings.ToLower(strings.Join(index.Cols, ",")),
		)
	case modeler.IndexTypeUnique:
		sql = fmt.Sprintf("DROP INDEX IF EXISTS %s; CREATE UNIQUE INDEX %s ON %s.%s (%s)",
			idx_name, idx_name, dc.base.DBName(), tableName,
			strings.ToLower(strings.Join(index.Cols, ",")),
		)

	default:
		return errors.New("Invalid Index Type")
	}

	//fmt.Println("IndexSet", sql)
	_, err := dc.base.ExecRaw(sql)

	return err
}

func (dc *DialectModeler) IndexDump(tableName string) ([]*modeler.Index, error) {

	indexes := []*modeler.Index{}

	sql := "SELECT indexname, indexdef "
	sql += "FROM pg_indexes "
	sql += "WHERE schemaname = $1 AND tablename = $2"

	//fmt.Println("IndexDump", sql, tableName)

	rows, err := dc.base.DB().Query(sql, "public", tableName)
	if err != nil {
		return indexes, err
	}
	defer rows.Close()

	for rows.Next() {

		var (
			indexType           int
			indexName, indexDef string
			cols                = []string{}
			exist               = false
		)

		if err = rows.Scan(&indexName, &indexDef); err != nil {
			return indexes, err
		}

		indexDef = strings.TrimSpace(indexDef)
		if indexDef != "" {
			nl := strings.IndexByte(indexDef, '(')
			nr := strings.IndexByte(indexDef, ')')
			if nl > 0 && nl < nr {
				cols = strings.Split(indexDef[nl+1:nr], ",")
				sort.Strings(cols)
			}
		}

		if strings.HasPrefix(indexName, "pri_") {
			indexType = modeler.IndexTypePrimaryKey
		} else if strings.HasPrefix(indexName, "idx_") {
			indexType = modeler.IndexTypeIndex
		} else if strings.HasPrefix(indexName, "uni_") {
			indexType = modeler.IndexTypeUnique
		}

		for _, v := range indexes {
			if v.NameKey(tableName) == indexName {
				exist = true
				break
			}
		}

		// //fmt.Println("    items", indexName, indexType, cols)

		if !exist {
			indexes = append(indexes, modeler.NewIndex(indexName, indexType).AddColumn(cols...))
		}
	}

	return indexes, nil
}

func (dc *DialectModeler) ColumnTypeSql(table_name string, col *modeler.Column) string {
	return dc.QuoteStr(col.Name) + " " + dialectColumnTypeFmt(table_name, col)
}

func (dc *DialectModeler) ColumnSync(tableName string, col *modeler.Column) error {

	col.Fix()

	seq_name := "seq_" + tableName + "__" + col.Name

	if col.IncrAble {
		dc.base.ExecRaw(fmt.Sprintf("CREATE SEQUENCE %s;", seq_name))
	}

	sql := fmt.Sprintf("ALTER TABLE %s.public.%s ADD COLUMN %s %s;",
		dc.base.DBName(), tableName, col.Name, dialectColumnTypeFmt(tableName, col))

	if col.IncrAble {
		sql += fmt.Sprintf("ALTER TABLE %s.public.%s ALTER COLUMN %s SET DEFAULT nextval('%s');",
			dc.base.DBName(), tableName, col.Name, seq_name)
	}

	if !col.IncrAble {

		if !col.NullAble {
			sql += fmt.Sprintf("ALTER TABLE %s.public.%s ALTER COLUMN %s SET NOT NULL;",
				dc.base.DBName(), tableName, col.Name)
		}

		if col.Default != "" {
			sql += fmt.Sprintf("ALTER TABLE %s.public.%s ALTER COLUMN %s SET DEFAULT '%s';",
				dc.base.DBName(), tableName, col.Name, col.Default)
		}
	}

	//fmt.Println("ColumnSync", sql)

	_, err := dc.base.ExecRaw(sql)

	return err
}

func (dc *DialectModeler) ColumnDel(tableName string, col *modeler.Column) error {

	sql := fmt.Sprintf("ALTER TABLE %s.public.%s DROP COLUMN IF EXISTS %s", dc.base.DBName(), tableName, col.Name)
	//fmt.Println("ColumnDel", sql)

	_, err := dc.base.ExecRaw(sql)

	return err
}

func (dc *DialectModeler) ColumnSet(tableName string, col *modeler.Column) error {

	col.Fix()

	sql := fmt.Sprintf("ALTER TABLE %s.public.%s ALTER COLUMN %s TYPE %s;",
		dc.base.DBName(), tableName, col.Name, dialectColumnTypeFmt(tableName, col))

	if col.IncrAble {
		seq_name := "seq_" + tableName + "__" + col.Name
		dc.base.ExecRaw(fmt.Sprintf("CREATE SEQUENCE %s;", seq_name))
		sql += fmt.Sprintf("ALTER TABLE %s.public.%s ALTER COLUMN %s SET DEFAULT nextval('%s');",
			dc.base.DBName(), tableName, col.Name, seq_name)
	}

	if !col.IncrAble {
		if !col.NullAble {
			sql += fmt.Sprintf("ALTER TABLE %s.public.%s ALTER COLUMN %s SET NOT NULL;",
				dc.base.DBName(), tableName, col.Name)
		}

		if col.Default != "" {
			sql += fmt.Sprintf("ALTER TABLE %s.public.%s ALTER COLUMN %s SET DEFAULT '%s';",
				dc.base.DBName(), tableName, col.Name, col.Default)
		}
	}

	//fmt.Println("ColumnSet", sql)

	_, err := dc.base.ExecRaw(sql)

	return err
}

func (dc *DialectModeler) ColumnDump(tableName string) ([]*modeler.Column, error) {

	var (
		cols    = []*modeler.Column{}
		selects = []string{
			"column_name",
			"is_nullable",
			"column_default",
			"udt_name",
			"character_maximum_length",
			"numeric_precision",
			"numeric_scale",
		}
	)

	q := "SELECT " + strings.Join(selects, ",")
	q += " FROM INFORMATION_SCHEMA.columns "
	q += "WHERE table_schema = 'public' AND table_catalog = $1 AND table_name = $2"

	//fmt.Println("CulumnQuery", q, tableName)
	rs, err := dc.base.QueryRaw(q, dc.base.DBName(), tableName)
	if err != nil {
		return cols, err
	}

	for _, entry := range rs {

		var (
			col       = &modeler.Column{}
			numeric_p = 0
			numeric_s = 0
		)

		for name, v := range entry.Fields {

			content := strings.TrimSpace(v.String())

			switch name {

			case "column_name":
				col.Name = content

			case "is_nullable":
				if "YES" == content {
					col.NullAble = true
				}

			case "column_default":
				if strings.HasPrefix(content, "nextval(") {
					col.IncrAble = true
				} else if len(content) > 0 {
					col.Default = content
				}

			case "udt_name":

				switch content {
				case "int8":
					col.Type = "int64"
				case "int4":
					col.Type = "int32"
				case "int2":
					col.Type = "int16"
				case "int1":
					col.Type = "int16"
				case "float8":
					col.Type = "float64"
				case "numeric":
					col.Type = "float64-decimal"
				case "varchar":
					col.Type = "string"
				case "text":
					col.Type = "string-text"
				case "date":
					col.Type = "date"
				case "timestamptz":
					col.Type = "datetime"
				case "bool":
					col.Type = "bool"
				}

			case "character_maximum_length":
				if content != "" {
					if iv, err := strconv.Atoi(content); err == nil {
						col.Length = strconv.Itoa(iv)
					}
				}

			case "numeric_precision":
				if content != "" {
					numeric_p, err = strconv.Atoi(content)
				}

			case "numeric_scale":
				if content != "" {
					numeric_s, err = strconv.Atoi(content)
				}
			}
		}

		if strings.HasPrefix(col.Type, "int") {
			col.NullAble = true
		}

		if col.Type == "float64-decimal" {
			col.Length = fmt.Sprintf("%d,%d", numeric_p, numeric_s)
		}

		if col.Default != "" {
			if n := strings.Index(col.Default, "::"); n > 0 {
				col.Default = strings.Trim(col.Default[:n], "'")
			}
			if col.IsNumber() {
				if col.IsInt() {
					if _, err := strconv.Atoi(col.Default); err != nil {
						col.Default = ""
					}
				}
				if col.IsFloat() {
					if _, err := strconv.ParseFloat(col.Default, 64); err != nil {
						col.Default = ""
					}
				}
			} else if col.Type == "string" {

			} else {
				col.Default = ""
			}
		}

		col.Fix()

		cols = append(cols, col)
	}

	return cols, nil
}

func (dc *DialectModeler) TableSync(table *modeler.Table) error {

	sql := "CREATE TABLE IF NOT EXISTS " + dc.base.DBName() + ".public." + dc.QuoteStr(table.Name) + "()"

	_, err := dc.base.ExecRaw(sql)

	return err
}

func (dc *DialectModeler) TableDump() ([]*modeler.Table, error) {

	tables := []*modeler.Table{}

	q := "SELECT table_name "
	q += "FROM INFORMATION_SCHEMA.tables "
	q += "WHERE table_schema = 'public' AND table_type = 'BASE TABLE' AND table_catalog = '" + dc.base.DBName() + "'"

	//fmt.Println("TableDump", q)
	rows, err := dc.base.DB().Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {

		var name string
		if err = rows.Scan(&name); err != nil {
			return nil, err
		}

		var (
			idxs, _ = dc.IndexDump(name)
			cols, _ = dc.ColumnDump(name)
		)

		tables = append(tables, &modeler.Table{
			Name:    name,
			Engine:  "",
			Charset: "UTF8",
			Columns: cols,
			Indexes: idxs,
			Comment: "",
		})
	}

	return tables, nil
}

func (dc *DialectModeler) TableExist(tableName string) bool {

	q := "SELECT count(*) FROM INFORMATION_SCHEMA.tables "
	q += "WHERE table_catalog = ? AND table_name = ? AND table_type = 'BASE TABLE'"

	rows, err := dc.base.QueryRaw(q, dc.base.DBName(), tableName)
	if err != nil {
		return false
	}

	return len(rows) > 0
}

func (dc *DialectModeler) SchemaSync(newds *modeler.Schema) error {

	curds, err := dc.SchemaDump()
	if err != nil {
		return err
	}

	for _, newTable := range newds.Tables {

		var (
			exist    = false
			curTable *modeler.Table
		)

		for _, curTable = range curds.Tables {

			if newTable.Name == curTable.Name {
				exist = true
				break
			}
		}

		if !exist {
			if err := dc.TableSync(newTable); err != nil {
				return err
			}
			curTable = modeler.NewTable(newTable.Name, "", "")
		}

		// Column
		for _, newcol := range newTable.Columns {

			var (
				colExist  = false
				colChange = false
			)

			dialectColumnTypeFix(newcol)

			for _, curcol := range curTable.Columns {

				if newcol.Name != curcol.Name {
					continue
				}

				colExist = true

				if newcol.Type != curcol.Type ||
					newcol.Length != curcol.Length ||
					newcol.NullAble != curcol.NullAble ||
					newcol.IncrAble != curcol.IncrAble ||
					newcol.Default != curcol.Default {
					colChange = true

					//fmt.Println("new", newcol)
					//fmt.Println("cur", curcol)

					break
				}
			}

			if !colExist {
				if err := dc.ColumnSync(newTable.Name, newcol); err != nil {
					return err
				}
			}

			if colChange {
				if err := dc.ColumnSet(newTable.Name, newcol); err != nil {
					return err
				}
			}
		}

		// Delete Unused Indexes
		for _, curidx := range curTable.Indexes {

			curExist := false

			for _, newidx := range newTable.Indexes {

				if newidx.NameKey(curTable.Name) == curidx.NameKey(curTable.Name) {
					curExist = true
					break
				}
			}

			if !curExist {
				if err := dc.IndexDel(newTable.Name, curidx); err != nil {
					return err
				}
			}
		}

		// Delete Unused Columns
		for _, curcol := range curTable.Columns {

			colExist := false

			for _, newcol := range newTable.Columns {

				if newcol.Name == curcol.Name {
					colExist = true
					break
				}
			}

			if !colExist {
				if err := dc.ColumnDel(newTable.Name, curcol); err != nil {
					return err
				}
			}
		}

		// Add New, or Update Changed Indexes
		for _, newidx := range newTable.Indexes {

			var (
				newIdxExist  = false
				newIdxChange = false
			)

			for _, curidx := range curTable.Indexes {

				if newidx.NameKey(newTable.Name) != curidx.NameKey(newTable.Name) {
					continue
				}

				newIdxExist = true

				sort.Strings(newidx.Cols)
				sort.Strings(curidx.Cols)

				if newidx.Type != curidx.Type ||
					strings.Join(newidx.Cols, ",") != strings.Join(curidx.Cols, ",") {

					newIdxChange = true
				}

				break
			}

			if newIdxChange {
				if err := dc.IndexSet(newTable.Name, newidx); err != nil {
					return err
				}

			} else if !newIdxExist {
				if err := dc.IndexSync(newTable.Name, newidx); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (dc *DialectModeler) SchemaSyncByJson(js string) error {
	ds, err := modeler.NewSchemaByJson(js)
	if err != nil {
		return err
	}
	return dc.SchemaSync(ds)
}

func (dc *DialectModeler) SchemaSyncByJsonFile(js_path string) error {
	ds, err := modeler.NewSchemaByJsonFile(js_path)
	if err != nil {
		return err
	}
	return dc.SchemaSync(ds)
}

func (dc *DialectModeler) SchemaDump() (*modeler.Schema, error) {

	var (
		ds = &modeler.Schema{
			Charset: "UTF8",
		}
		err error
	)

	ds.Tables, err = dc.TableDump()

	return ds, err
}

func (dc *DialectModeler) QuoteStr(str string) string {
	return dialectQuote + str + dialectQuote
}
