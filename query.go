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
	"strings"

	"github.com/lynkdb/iomix/rdb"
)

type Queryer struct {
	cols   string
	table  string
	order  string
	limit  int64
	offset int64
	where  rdb.Filter
}

func NewQueryer() rdb.Queryer {
	return &Queryer{
		cols:   "*",
		limit:  1,
		offset: 0,
	}
}

func (q *Queryer) Select(s string) rdb.Queryer {
	q.cols = s
	return q
}

func (q *Queryer) From(s string) rdb.Queryer {
	q.table = s
	return q
}

func (q *Queryer) Order(s string) rdb.Queryer {
	q.order = s
	return q
}

func (q *Queryer) Limit(num int64) rdb.Queryer {
	q.limit = num
	return q
}

func (q *Queryer) Offset(num int64) rdb.Queryer {
	q.offset = num
	return q
}

func (q *Queryer) Parse() (sql string, params []interface{}) {

	if len(q.table) == 0 {
		return
	}

	cols := strings.Split(q.cols, ",")
	for i, v := range cols {
		cols[i] = dialect_quote_str(v)
	}

	sql = fmt.Sprintf("SELECT %s FROM %s ", strings.Join(cols, ","), q.table)

	frsql, ps := q.Where().Parse()
	if len(ps) > 0 {
		sql += "WHERE " + frsql + " "
		params = ps
	}

	if len(q.order) > 0 {
		sql += "ORDER BY " + q.order + " "
	}

	if q.offset > 0 {
		sql += "LIMIT ?,?"
		params = append(params, q.offset, q.limit)
	} else {
		sql += "LIMIT ?"
		params = append(params, q.limit)
	}

	return
}

func (q *Queryer) Where() rdb.Filter {
	if q.where == nil {
		q.where = NewFilter()
	}
	return q.where
}

func (q *Queryer) SetFilter(fr rdb.Filter) {
	q.where = fr
}
