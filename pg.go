package breaql

import (
	"log/slog"
	"slices"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// RunPostgreSQL parses the given DDL statements and returns the breaking ones.
func RunPostgreSQL(sql string) (BreakingChanges, error) {
	sql = strings.TrimSpace(sql)
	if !strings.HasSuffix(sql, ";") {
		sql += ";"
	}

	tree, err := pg_query.Parse(sql)
	if err != nil {
		return BreakingChanges{}, &ParseError{original: err, Message: err.Error(), funcName: "pg_query.Parse"}
	}

	changes := NewBreakingChanges()

	for _, rawStmt := range tree.Stmts {
		start := rawStmt.GetStmtLocation()
		end := start + rawStmt.GetStmtLen()
		stmtText := strings.TrimSpace(sql[start:end]) + ";"
		slog.Info("processing stmt", slog.String("stmt", stmtText))

		node := rawStmt.GetStmt()
		if node == nil {
			slog.Warn("skipping empty statement", slog.String("stmt", stmtText))
			continue
		}

		switch n := node.GetNode().(type) {
		case *pg_query.Node_DropdbStmt:
			changes.Databases.add(n.DropdbStmt.GetDbname(), stmtText)

		case *pg_query.Node_DropStmt:
			switch n.DropStmt.RemoveType {
			case pg_query.ObjectType_OBJECT_SCHEMA:
				for _, obj := range n.DropStmt.GetObjects() {
					if str := obj.GetString_(); str != nil {
						changes.Schemas.add(str.GetSval(), stmtText)
					}
				}

			case pg_query.ObjectType_OBJECT_TABLE:
				for _, obj := range n.DropStmt.GetObjects() {
					if list := obj.GetList(); list != nil && len(list.GetItems()) > 0 {
						var parts []string
						for _, item := range list.GetItems() {
							if str := item.GetString_(); str != nil {
								parts = append(parts, str.GetSval())
							}
						}
						table := strings.Join(parts, ".")
						changes.Tables.add(table, stmtText)
					}
				}

			case pg_query.ObjectType_OBJECT_INDEX:
				for _, obj := range n.DropStmt.GetObjects() {
					if list := obj.GetList(); list != nil && len(list.GetItems()) > 0 {
						var parts []string
						for _, item := range list.GetItems() {
							if str := item.GetString_(); str != nil {
								parts = append(parts, str.GetSval())
							}
						}
						index := strings.Join(parts, ".")
						changes.Indexes.add(index, stmtText)
					}
				}
			}

		case *pg_query.Node_TruncateStmt:
			for _, rel := range n.TruncateStmt.GetRelations() {
				if rv := rel.GetRangeVar(); rv != nil {
					table := rv.GetRelname()
					if schema := rv.GetSchemaname(); schema != "" {
						table = schema + "." + table
					}
					changes.Tables.add(table, stmtText)
				}
			}

		case *pg_query.Node_RenameStmt:
			if rv := n.RenameStmt.GetRelation(); rv != nil {
				table := rv.GetRelname()
				if schema := rv.GetSchemaname(); schema != "" {
					table = schema + "." + table
				}
				changes.Tables.add(table, stmtText)
			}

		case *pg_query.Node_AlterTableStmt:
			if rv := n.AlterTableStmt.GetRelation(); rv != nil {
				if slices.ContainsFunc(n.AlterTableStmt.GetCmds(), isBreakingAlterTableCmd) {
					table := rv.GetRelname()
					if schema := rv.GetSchemaname(); schema != "" {
						table = schema + "." + table
					}
					changes.Tables.add(table, stmtText)
				}
			}
		}
	}

	return changes, nil
}

func isBreakingAlterTableCmd(cmd *pg_query.Node) bool {
	switch c := cmd.GetNode().(type) {
	case *pg_query.Node_AlterTableCmd:
		switch c.AlterTableCmd.GetSubtype() {
		case pg_query.AlterTableType_AT_DropColumn,
			pg_query.AlterTableType_AT_DropConstraint,
			pg_query.AlterTableType_AT_AlterColumnType:
			return true
		}
	}
	return false
}
