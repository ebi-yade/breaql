# breaql

Detects breaking changes in DDL statements

## Install

### as a CLI tool

```shell
go install github.com/ebi-yade/breaql/cmd/breaql@latest
```

### as a Go package

```shell
go get github.com/ebi-yade/breaql
```

## Usage

### via CLI

You can pass the DDL statements via stdin or as a file.

```shell
echo '
        CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(100));
        ALTER TABLE users DROP COLUMN age;
        DROP TABLE users;
  ' | go run cmd/breaql/main.go --driver mysql
```

And then you will see the output like this:

```sql
-- Detected destructive changes:
-- No.1
      ALTER TABLE users DROP COLUMN age;
-- No.2
      DROP TABLE users;
```

### via Go application

```go
package main

import (
	"fmt"
	"log"

	"github.com/ebi-yade/breaql"
)

func main() {
	ddl := `
        CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(100));
        ALTER TABLE users DROP COLUMN age; -- breaking!
    `

	changes, err := breaql.RunMySQL(ddl)
	if err != nil {
		log.Fatal(err)
	}

	if len(changes) == 0 {
		fmt.Println("No breaking changes detected")
	} else {
		fmt.Println("-- Detected destructive changes:")
		for i, change := range changes {
			fmt.Printf("-- No.%d\n        %s\n", i+1, change)
		}
	}
}

```
