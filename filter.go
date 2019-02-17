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
	"strings"

	"github.com/lynkdb/iomix/rdb"
)

const (
	filterExprSep = "."
)

var filterOperators = map[string]string{
	"eq":   "= ?",
	"ne":   "<> ?",
	"gt":   "> ?",
	"ge":   ">= ?",
	"lt":   "< ?",
	"le":   "<= ?",
	"like": "LIKE ?",
	"in":   "IN (?)",
}

type filterItem struct {
	exprs    []string
	args     []interface{}
	filter   *Filter
	isOr     bool
	isNot    bool
	isFilter bool
}

type Filter struct {
	params []filterItem
}

func NewFilter() rdb.Filter {
	return &Filter{}
}

func (fr *Filter) And(expr string, args ...interface{}) rdb.Filter {

	if expr == "" || len(args) == 0 {
		return nil
	}

	fr.params = append(fr.params, filterItem{
		exprs: strings.Split(expr, filterExprSep),
		args:  args,
	})

	return fr
}

func (fr *Filter) Or(expr string, args ...interface{}) rdb.Filter {

	if expr == "" || len(args) == 0 {
		return nil
	}

	fr.params = append(fr.params, filterItem{
		exprs: strings.Split(expr, filterExprSep),
		args:  args,
		isOr:  true,
	})

	return fr
}

func (fr *Filter) Parse() (where string, params []interface{}) {

	if fr == nil || len(fr.params) == 0 {
		return
	}

	for i, p := range fr.params {

		if i > 0 {
			if p.isOr {
				where += "OR "
			} else {
				where += "AND "
			}
		}

		if p.isNot {
			where += "NOT "
		}

		if p.isFilter {

			w, ps := p.filter.Parse()
			if w != "" {
				w = fmt.Sprintf("( %s) ", w)
			}
			where += w

			params = append(params, ps...)

		} else {

			operator := ""

			if len(p.exprs) == 1 {
				p.exprs = append(p.exprs, "eq")
			}

			if v, ok := filterOperators[p.exprs[1]]; ok {
				operator = v
			}

			if operator == "" {
				operator = "= ?"
			}

			if len(p.exprs) > 1 && p.exprs[1] == "in" && len(p.args) > 1 {

				res := []string{}
				for i := 0; i < len(p.args); i++ {
					res = append(res, "?")
				}

				where += fmt.Sprintf("%s IN (%s) ", dialect_quote_str(p.exprs[0]), strings.Join(res, ","))
				params = append(params, p.args...)

			} else {

				where += fmt.Sprintf("%s %s ", dialect_quote_str(p.exprs[0]), operator)
				params = append(params, p.args[0])
			}
		}
	}

	return
}
