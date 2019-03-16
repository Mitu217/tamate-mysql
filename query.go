// ========================================================
//
// datasourceからddl(Schema)とsql(Rows)を作成するメソッドを作る
//
// ========================================================

package mysql

import (
	"fmt"
	"strings"

	"github.com/go-tamate/tamate/driver"
)

func generateGetInformationSchemaQuery(tableName string) (string, error) {
	return fmt.Sprintf("SELECT COLUMN_NAME, ORDINAL_POSITION, COLUMN_TYPE, COLUMN_KEY, IS_NULLABLE, EXTRA FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE() and TABLE_NAME = '%s'", tableName), nil
}

func generateCreateDBQuery(dbName string) (string, error) {
	return fmt.Sprintf("CREATE DATABASE `%s`", dbName), nil
}

func generateDropDBQuery(dbName string) (string, error) {
	return fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dbName), nil
}

func generateCreateTableQuery(sc *driver.Schema) (string, error) {
	var defs []string

	for _, col := range sc.Columns {
		ct, err := columnTypeFromGenericToMySQL(col.Type)
		if err != nil {
			return "", err
		}
		def := fmt.Sprintf("`%s` %s", col.Name, ct)

		for _, n := range sc.PrimaryKey.ColumnNames {
			if n == col.Name {
				def += " PRIMARY KEY"
				break
			}
		}

		if col.NotNull {
			def += " NOT NULL"
		}

		if col.AutoIncrement {
			def += " AUTO INCREMENT"
		}

		defs = append(defs, def)
	}

	return fmt.Sprintf("CREATE TABLE `%s` (%s)", sc.Name, strings.Join(defs, ", ")), nil
}

func generateDropTableQuery(tableName string) (string, error) {
	return fmt.Sprintf("DROP TABLE IF EXISTS `%s`", tableName), nil
}

func generateSelectRowsQuery(tableName string) (string, error) {
	return fmt.Sprintf("SELECT id, name FROM %s", tableName), nil
}

func generateInsertRowQuery(tableName string, row *driver.Row) (string, error) {
	columnNames := row.Values.ColumnNames()
	values := make([]string, len(columnNames))
	for i := range columnNames {
		values[i] = "?"
	}
	return fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)", tableName, strings.Join(columnNames, ", "), strings.Join(values, ", ")), nil
}
