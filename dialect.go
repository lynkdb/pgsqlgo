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

// Numeric Types
// Name Storage Size Description Range
// smallint    2 bytes   small-range integer -32768 to +32767
// integer     4 bytes   typical choice for integer -2147483648 to +2147483647
// bigint      8 bytes   large-range integer -9223372036854775808 to +9223372036854775807
// decimal     variable  user-specified precision, exact up to 131072 digits before the decimal point; up to 16383 digits after the decimal point
// numeric     variable  user-specified precision, exact up to 131072 digits before the decimal point; up to 16383 digits after the decimal point
// real        4 bytes   variable-precision, inexact 6 decimal digits precision
// double      precision 8 bytes variable-precision, inexact 15 decimal digits precision
// smallserial 2 bytes   small autoincrementing integer 1 to 32767
// serial      4 bytes   autoincrementing integer 1 to 2147483647
// bigserial   8 bytes   large autoincrementing integer 1 to 9223372036854775807

// Character Types
// Name	Description
// character varying(n), varchar(n)	variable-length with limit
// character(n), char(n)	fixed-length, blank padded
// text	variable unlimited length

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
	dbName string
}

func (dc *Dialect) DBName() string {
	return dc.dbName
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

func (dc *Dialect) Close() {
	dc.Base.Close()
}
