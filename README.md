# breaql
Detects breaking changes in DDL statements

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

## Note about Dependencies

```shell
go mod init github.com/ebi-yade/breaql
go get -v github.com/pingcap/tidb/pkg/parser@1a0c3ac
go get github.com/alecthomas/kong # => v1.2.1
```
