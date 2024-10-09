package breaql_test

import (
	"testing"

	"github.com/ebi-yade/breaql"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
)

func TestRunMySQL(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		want       breaql.BreakingChanges
		expectsErr bool
	}{
		{
			name: "DropDatabase",
			sql:  "DROP DATABASE test_db;",
			want: breaql.BreakingChanges{
				Databases: breaql.DatabaseChanges{"test_db": {"DROP DATABASE test_db;"}},
			},
			expectsErr: false,
		},
		{
			name: "DropTable",
			sql:  "DROP TABLE test_table;",
			want: breaql.BreakingChanges{
				Tables: breaql.TableChanges{"test_table": {"DROP TABLE test_table;"}},
			},
			expectsErr: false,
		},
		{
			name: "TruncateTable",
			sql:  "TRUNCATE TABLE test_table;",
			want: breaql.BreakingChanges{
				Tables: breaql.TableChanges{"test_table": {"TRUNCATE TABLE test_table;"}},
			},
			expectsErr: false,
		},
		{
			name: "RenameTable",
			sql:  "RENAME TABLE test_table_old TO test_table_new;",
			want: breaql.BreakingChanges{
				Tables: breaql.TableChanges{"test_table_old": {"RENAME TABLE test_table_old TO test_table_new;"}},
			},
			expectsErr: false,
		},
		{
			name: "AlterTableDropColumn",
			sql:  "ALTER TABLE test_table DROP COLUMN column_name;",
			want: breaql.BreakingChanges{
				Tables: breaql.TableChanges{"test_table": {"ALTER TABLE test_table DROP COLUMN column_name;"}},
			},
			expectsErr: false,
		},
		{
			name: "AlterTableDropIndex",
			sql:  "ALTER TABLE test_table DROP INDEX index_name;",
			want: breaql.BreakingChanges{
				Tables: breaql.TableChanges{"test_table": {"ALTER TABLE test_table DROP INDEX index_name;"}},
			},
			expectsErr: false,
		},
		{
			name: "AlterTableDropForeignKey",
			sql:  "ALTER TABLE test_table DROP FOREIGN KEY fk_name;",
			want: breaql.BreakingChanges{
				Tables: breaql.TableChanges{"test_table": {"ALTER TABLE test_table DROP FOREIGN KEY fk_name;"}},
			},
			expectsErr: false,
		},
		{
			name: "AlterTableModifyColumn",
			sql:  "ALTER TABLE test_table MODIFY column_name VARCHAR(255);",
			want: breaql.BreakingChanges{
				Tables: breaql.TableChanges{"test_table": {"ALTER TABLE test_table MODIFY column_name VARCHAR(255);"}},
			},
			expectsErr: false,
		},
		{
			name:       "CreateTable",
			sql:        "CREATE TABLE test_table (id INT PRIMARY KEY);",
			want:       breaql.BreakingChanges{},
			expectsErr: false,
		},
		{
			name: "MultipleStatementsWithBreakingChanges",
			sql: `CREATE TABLE test_table (id INT PRIMARY KEY);
				ALTER TABLE test_table DROP COLUMN id;
				DROP DATABASE test_db;`,
			want: breaql.BreakingChanges{
				Tables:    breaql.TableChanges{"test_table": {"ALTER TABLE test_table DROP COLUMN id;"}},
				Databases: breaql.DatabaseChanges{"test_db": {"DROP DATABASE test_db;"}},
			},
			expectsErr: false,
		},
		{
			name: "MultipleStatementsWithNonBreakingAndBreakingChanges",
			sql: `CREATE TABLE test_table (id INT PRIMARY KEY);
				ALTER TABLE test_table ADD COLUMN new_column INT;
				ALTER TABLE test_table DROP COLUMN id;
				ALTER TABLE test_table DROP COLUMN new_column;
				ALTER TABLE test_table ADD INDEX idx_new_column (new_column);`,
			want: breaql.BreakingChanges{
				Tables: breaql.TableChanges{"test_table": {"ALTER TABLE test_table DROP COLUMN id;", "ALTER TABLE test_table DROP COLUMN new_column;"}},
			},
			expectsErr: false,
		},
		{
			name:       "InvalidSQL",
			sql:        "INVALID SQL STATEMENT;",
			expectsErr: true,
		},
	}

	opts := []cmp.Option{
		cmpopts.EquateEmpty(),
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := breaql.RunMySQL(tt.sql)
			if tt.expectsErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if diff := cmp.Diff(tt.want, got, opts...); diff != "" {
					t.Errorf("RunMySQL() mismatch (-want +got):\n%s", diff)
				}
			}
		})
	}
}
