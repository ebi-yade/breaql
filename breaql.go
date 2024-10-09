package breaql

import (
	"strings"

	"github.com/samber/lo"
)

type BreakingChanges struct {
	Tables    TableChanges    `json:"tables"`
	Databases DatabaseChanges `json:"databases"`
}

func NewBreakingChanges() BreakingChanges {
	return BreakingChanges{
		Tables:    make(TableChanges),
		Databases: make(DatabaseChanges),
	}
}

// Exist return if any changes exist.
func (bc BreakingChanges) Exist() bool {
	return bc.Tables.Exist() || bc.Databases.Exist()
}

// FormatSQL returns the breaking changes in SQL format.
func (bc BreakingChanges) FormatSQL() string {
	builder := strings.Builder{}
	for _, table := range bc.Tables.Tables() {
		builder.WriteString("-- Table: " + table + "\n")
		for _, stmt := range bc.Tables.Statements(table) {
			builder.WriteString("        " + stmt + "\n")
		}
	}
	for _, database := range bc.Databases.Databases() {
		builder.WriteString("-- Database: " + database + "\n")
		for _, stmt := range bc.Databases.Statements(database) {
			builder.WriteString("        " + stmt + "\n")
		}
	}

	return builder.String()
}

type TableChanges map[string][]string

func (tc TableChanges) add(table string, statements ...string) {
	tc[table] = append(tc[table], statements...)
}

// Tables returns the affected table names.
func (tc TableChanges) Tables() []string {
	return lo.Keys(tc)
}

// Statements returns the breaking statements for the given table.
func (tc TableChanges) Statements(table string) []string {
	return tc[table]
}

// Exist return if any changes exist.
func (tc TableChanges) Exist() bool {
	return len(tc) > 0
}

type DatabaseChanges map[string][]string

func (dc DatabaseChanges) add(database string, statements ...string) {
	dc[database] = append(dc[database], statements...)
}

// Databases returns the affected database names.
func (dc DatabaseChanges) Databases() []string {
	return lo.Keys(dc)
}

// Statements returns the breaking statements for the given database.
func (dc DatabaseChanges) Statements(database string) []string {
	return dc[database]
}

// Exist return if any changes exist.
func (dc DatabaseChanges) Exist() bool {
	return len(dc) > 0
}
