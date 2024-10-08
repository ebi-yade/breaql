package breaql

import (
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	//_ "github.com/pingcap/tidb/pkg/types/parser_driver"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

func RunMySQL(sql string) ([]string, error) {
	p := parser.New()
	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return nil, err
	}

	var breakingStmt []string

	for _, stmtNode := range stmtNodes {
		switch stmt := stmtNode.(type) {
		case *ast.DropTableStmt:
			breakingStmt = append(breakingStmt, stmt.Text())
		case *ast.AlterTableStmt:
			for _, spec := range stmt.Specs {
				switch spec.Tp {
				case ast.AlterTableDropColumn:
					breakingStmt = append(breakingStmt, stmt.Text())
				case ast.AlterTableDropIndex:
					breakingStmt = append(breakingStmt, stmt.Text())
				case ast.AlterTableModifyColumn:
					// Check for potential data loss when modifying column type
					if spec.NewColumns != nil && len(spec.NewColumns) > 0 {
						oldCol := spec.OldColumnName.String()
						newCol := spec.NewColumns[0]
						if !isCompatibleColumnModification(oldCol, newCol) {
							breakingStmt = append(breakingStmt, stmt.Text())
						}
					}
				}
			}
		}
	}

	return breakingStmt, nil
}

func isCompatibleColumnModification(oldCol string, newCol *ast.ColumnDef) bool {
	// This is a simplified check. In a real-world scenario, you'd want to do more thorough type compatibility checks.
	oldType := strings.ToLower(oldCol)
	newType := strings.ToLower(newCol.Tp.String())

	// Check for common incompatible type changes
	incompatibleChanges := map[string][]string{
		"int":       {"varchar", "text", "date", "datetime"},
		"varchar":   {"int", "date", "datetime"},
		"date":      {"int", "varchar"},
		"datetime":  {"int", "varchar"},
		"timestamp": {"int", "varchar"},
	}

	if incompatible, ok := incompatibleChanges[oldType]; ok {
		for _, incType := range incompatible {
			if strings.HasPrefix(newType, incType) {
				return false
			}
		}
	}

	return true
}
