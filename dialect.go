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

package postgrego

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/lynkdb/iomix/rdb"
	"github.com/lynkdb/iomix/rdb/modeler"
)

const (
	dialect_quote        = `"`
	dialect_datetime_fmt = "2006-01-02 15:04:05 -0700 MST"
)

var dialect_column_types = map[string]string{
	"bool":            "bool",
	"string":          "varchar(%v)",
	"string-text":     "text",
	"date":            "date",
	"datetime":        "timestamp with time zone",
	"int8":            "smallint",
	"int16":           "smallint",
	"int32":           "integer",
	"int64":           "bigint",
	"uint8":           "smallint",
	"uint16":          "integer",
	"uint32":          "bigint",
	"uint64":          "bigint",
	"float64":         "double precision",
	"float64-decimal": "numeric(%v, %v)",
}

func dialect_column_type_fix(col *modeler.Column) {

	if strings.HasPrefix(col.Type, "uint") {
		col.Type = col.Type[1:]
	}
	if col.Type == "int8" {
		col.Type = "int16"
	}

	col.Fix()
}

func dialect_column_type_fmt(table_name string, col *modeler.Column) string {

	sql, ok := dialect_column_types[col.Type]
	if !ok {
		return col.Type
	}

	col.Fix()

	switch col.Type {
	case "string":
		sql = fmt.Sprintf(sql, col.Length)

	case "float64-decimal":
		lens := strings.Split(col.Length, ",")
		if lens[0] == "" {
			lens[0] = "10"
		}
		if len(lens) < 2 {
			lens = append(lens, "2")
		}
		sql = fmt.Sprintf(sql, lens[0], lens[1])
	}

	return sql
}

var dialect_stmts = map[string]string{
	"insertIgnore": "INSERT INTO %s (%s) VALUES (%s) ON CONFLICT DO NOTHING", // >= v9.5
}

func dialect_stmt_bind_var(sql string, num int) string {
	for i := 1; i <= num; i++ {
		sql = strings.Replace(sql, "?", "$"+strconv.Itoa(i), 1)
	}
	return sql
}

func dialect_quote_str(name string) string {
	if name == "*" {
		return name
	}
	return dialect_quote + name + dialect_quote
}

type Dialect struct {
	rdb.Base
}

func (dc *Dialect) Modeler() (modeler.Modeler, error) {
	return &DialectModeler{
		base: dc,
	}, nil
}

func (dc *Dialect) QuoteStr(str string) string {
	return dialect_quote + str + dialect_quote
}

func (dc *Dialect) NewFilter() rdb.Filter {
	return NewFilter()
}

func (dc *Dialect) NewQueryer() rdb.Queryer {
	return NewQueryer()
}