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
	Driver string `name:"driver" default:"mysql" help:"Database driver"`
	Path   string `name:"path" default:"-" help:"Path to the SQL file"`
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
	var changes []breaql.BreakingChange
	switch input.Driver {
	case "mysql":
		changes, err = breaql.RunMySQL(string(ddl))
		if err != nil {
			return errors.Wrap(err, "error breaql.RunMySQL")
		}
	default:
		return errors.Errorf("unsupported driver: %s", input.Driver)
	}
	if len(changes) > 0 {
		fmt.Println("-- Detected destructive changes: --")
		for _, change := range changes {
			fmt.Printf("%s: %s\n", change.Type, change.Description)
		}
	} else {
		fmt.Println("-- No destructive changes detected. --")
	}

	return nil
}

func main() {
	if err := main_(); err != nil {
		slog.Error(fmt.Sprintf("error: %v", err))
		os.Exit(1)
	}
}
