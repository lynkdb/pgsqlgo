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
	"database/sql"
	"errors"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/lynkdb/iomix/connect"
	"github.com/lynkdb/iomix/rdb"
)

func NewConnector(cfg connect.ConnOptions) (rdb.Connector, error) {

	dsn := ""

	// postgres://pqgotest:password@localhost/pqgotest?sslmode=verify-full
	if cfg.Value("host") != "" {
		dsn = fmt.Sprintf(
			`postgres://%s:%s@%s:%s/%s?sslmode=disable`,
			cfg.Value("user"), cfg.Value("pass"),
			cfg.Value("host"), cfg.Value("port"), cfg.Value("dbname"),
		)
	} else if cfg.Value("socket") != "" {
		dsn = fmt.Sprintf(
			`%s:%s@unix(%s)/%s?charset=%s`,
			cfg.Value("user"), cfg.Value("pass"),
			cfg.Value("socket"), cfg.Value("dbname"), cfg.Value("charset"),
		)
	} else {
		return nil, errors.New("Incorrect configuration")
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	base, err := rdb.NewBase(cfg, db)
	if err != nil {
		return nil, err
	}
	base.BindVar = dialect_stmt_bind_var
	base.QuoteStr = dialect_quote_str
	base.TypeDatetimeFmt = dialect_datetime_fmt

	for k, v := range dialect_stmts {
		base.StmtSet(k, v)
	}

	return &Dialect{
		Base: *base,
	}, nil
}
