package mysql

import (
	"database/sql"
	"fmt"
	"log"
)

func dropDatabase(dsn string, dbname string) error {
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	if _, err := db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dbname)); err != nil {
		return err
	}
	return nil
}

func createDatabase(dsn string, dbname string) error {
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	if _, err := db.Exec(fmt.Sprintf("CREATE DATABASE `%s`", dbname)); err != nil {
		log.Fatal(err)
	}
	return nil
}

func useDatabase(dsn string, dbname string) error {
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	if _, err := db.Exec(fmt.Sprintf("USE `%s`", dbname)); err != nil {
		log.Fatal(err)
	}
	return nil
}
