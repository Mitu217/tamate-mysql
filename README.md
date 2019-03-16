# tamate-mysql

[![LICENSE](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![GoDoc](https://godoc.org/github.com/go-tamate/tamate?status.svg)](https://godoc.org/github.com/go-tamate/tamate-mysql)
[![Go Report Card](https://goreportcard.com/badge/github.com/go-tamate/tamate)](https://goreportcard.com/report/github.com/go-tamate/tamate-mysql)

[![CircleCI](https://circleci.com/gh/go-tamate/tamate.svg?style=svg)](https://circleci.com/gh/go-tamate/tamate-mysql)

A MySQL-Driver for [go-tamate/tamate](https://godoc.org/github.com/go-tamate/tamate) package

---------------------------------------

  * [Features](#features)
  * [Requirements](#requirements)
  * [Installation](#installation)
  * [Usage](#usage)
    * [DSN](#dsn-data-source-name)
  * [Testing / Development](#testing--development)
  * [License](#license)

---------------------------------------

## Features
 * Export schema's ddl from datasource
 * Export row's sql from datasource
 * Supports alter of schema

## Requirements
 * Go 1.12 or higher. We aim to support the 3 latest versions of Go.

---------------------------------------

## Installation
Simple install the package to your [$GOPATH](https://github.com/golang/go/wiki/GOPATH "GOPATH") with the [go tool](https://golang.org/cmd/go/ "go command") from shell:
```bash
$ go get -u github.com/go-tamate/tamate-mysql
```
Make sure [Git is installed](https://git-scm.com/downloads) on your machine and in your system's `PATH`.

## Usage
_Tamate Driver_ is an implementation of `tamate/driver` interface.

Use `mysql` as `driverName` and a valid [DSN](#dsn-data-source-name)  as `dataSourceName`:
```go
import  "github.com/go-tamate/tamate"
import  _ "github.com/go-sql-driver/mysql"
import  _ "github.com/go-tamate/tamate-mysql"

ds, err := tamate.Open("mysql", "./")
```

Use this to `Get`, `Set`, `GettingDiff`, etc.

### DSN (Data Source Name)

Please refer to the usage of [go-sql-driver](https://github.com/go-sql-driver/mysql#dsn-data-source-name)

## Testing / Development

Please execute the following command at the root of the project

```bash
docker-compose up -d
go test ./...
docker-compose down
```

---------------------------------------

## License
* MIT
    * see [LICENSE](./LICENSE)