package breaql

import (
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	//_ "github.com/pingcap/tidb/pkg/types/parser_driver"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

type BreakingChange struct {
	Type        string
	Table       string
	Column      string
	Description string
}

func RunMySQL(sql string) ([]BreakingChange, error) {
	p := parser.New()
	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return nil, err
	}

	var changes []BreakingChange

	for _, stmtNode := range stmtNodes {
		switch stmt := stmtNode.(type) {
		case *ast.DropTableStmt:
			for _, table := range stmt.Tables {
				changes = append(changes, BreakingChange{
					Type:        "DROP TABLE",
					Table:       table.Name.String(),
					Description: fmt.Sprintf("Dropping table %s", table.Name.String()),
				})
			}
		case *ast.AlterTableStmt:
			for _, spec := range stmt.Specs {
				switch spec.Tp {
				case ast.AlterTableDropColumn:
					changes = append(changes, BreakingChange{
						Type:        "DROP COLUMN",
						Table:       stmt.Table.Name.String(),
						Column:      spec.OldColumnName.String(),
						Description: fmt.Sprintf("Dropping column %s from table %s", spec.OldColumnName.String(), stmt.Table.Name.String()),
					})
				case ast.AlterTableDropIndex:
					changes = append(changes, BreakingChange{
						Type:        "DROP INDEX",
						Table:       stmt.Table.Name.String(),
						Column:      spec.Name,
						Description: fmt.Sprintf("Dropping index %s from table %s", spec.Name, stmt.Table.Name.String()),
					})
				case ast.AlterTableModifyColumn:
					// Check for potential data loss when modifying column type
					if spec.NewColumns != nil && len(spec.NewColumns) > 0 {
						oldCol := spec.OldColumnName.String()
						newCol := spec.NewColumns[0]
						if !isCompatibleColumnModification(oldCol, newCol) {
							changes = append(changes, BreakingChange{
								Type:        "MODIFY COLUMN",
								Table:       stmt.Table.Name.String(),
								Column:      oldCol,
								Description: fmt.Sprintf("Potentially destructive modification of column %s in table %s", oldCol, stmt.Table.Name.String()),
							})
						}
					}
				}
			}
		}
	}

	return changes, nil
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
