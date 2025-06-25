package main

import (
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/alecthomas/kong"
	"github.com/ebi-yade/breaql"
	"github.com/pingcap/errors"
)

type Input struct {
	Driver   string `name:"driver" default:"mysql" help:"Database driver (mysql, pg)"`
	Path     string `name:"path" default:"-" help:"Path to the SQL file"`
	LogLevel string `name:"log-level" default:"info" help:"Log level"`
}

func main_() error {
	input := Input{}
	flagParser, err := kong.New(&input, kong.UsageOnError())
	if err != nil {
		return errors.Wrap(err, "error kong.New")
	}
	_, err = flagParser.Parse(os.Args[1:])
	if err != nil {
		return errors.Wrap(err, "error flagParser.Parse")
	}

	switch input.LogLevel {
	case "debug":
		slog.SetLogLoggerLevel(slog.LevelDebug)
	case "info":
		slog.SetLogLoggerLevel(slog.LevelInfo)
	default:
		return errors.Errorf("invalid log level: %s", input.LogLevel)
	}

	// Read the DDLs
	var ddlReader io.Reader
	if input.Path == "-" {
		ddlReader = os.Stdin
	} else {
		file, err := os.Open(input.Path)
		if err != nil {
			return errors.Wrap(err, "error os.Open")
		}
		defer file.Close()
		ddlReader = file
	}
	ddl, err := io.ReadAll(ddlReader)
	if err != nil {
		return errors.Wrap(err, "error io.ReadAll")
	}

	// Detect destructive changes
	var changes breaql.BreakingChanges
	switch input.Driver {
	case "mysql":
		changes, err = breaql.RunMySQL(string(ddl))
		if err != nil {
			return errors.Wrap(err, "error breaql.RunMySQL")
		}
	case "pg":
		changes, err = breaql.RunPostgreSQL(string(ddl))
		if err != nil {
			return errors.Wrap(err, "error breaql.RunPostgreSQL")
		}
	default:
		return errors.Errorf("unsupported driver: %s", input.Driver)
	}
	if changes.Exist() {
		fmt.Println("-- Detected destructive changes:")
		fmt.Printf(changes.FormatSQL())
	} else {
		fmt.Println("-- No destructive changes detected. --")
	}

	return nil
}

func main() {
	if err := main_(); err != nil {
		switch err := errors.Cause(err).(type) {
		case *breaql.ParseError:
			slog.Error("Parse Error!", slog.String("message", err.Message))
		default:
			slog.Error(fmt.Sprintf("error: %v", err))
		}
		os.Exit(1)
	}
}
