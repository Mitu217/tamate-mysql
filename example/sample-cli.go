package main

import (
	"fmt"

	"github.com/go-tamate/tamate"

	// mysql driver
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/go-tamate/tamate-mysql"
)

const (
	MYSQL_USER     = "root"
	MYSQL_PASSWORD = "example"
	DB_NAME        = "tamatest"
)

func main() {
	dsn := fmt.Sprintf("%s:%s@/%s", MYSQL_USER, MYSQL_PASSWORD, DB_NAME)
	ds, err := tamate.Open("mysql", dsn)
	if err != nil {
		panic(err)
	}
	defer ds.Close()
}
