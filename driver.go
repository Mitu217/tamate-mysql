package mysql

import (
	"context"

	"github.com/go-tamate/tamate"
	"github.com/go-tamate/tamate/driver"
)

const driverName = "mysql"

type mysqlDriver struct{}

func (md *mysqlDriver) Open(ctx context.Context, dsn string) (driver.Conn, error) {
	return newMySQLConn(dsn)
}

func init() {
	tamate.Register(driverName, &mysqlDriver{})
}
