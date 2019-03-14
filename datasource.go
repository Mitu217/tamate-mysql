package mysql

import (
	"context"

	"github.com/Mitu217/tamate"
	"github.com/Mitu217/tamate/driver"
)

const driverName = "mysql"

type mysqlDriver struct{}

func (md *mysqlDriver) Open(ctx context.Context, dsn string) (driver.Conn, error) {
	mc, err := newMySQLConn(dsn)
	if err != nil {
		return nil, err
	}
	return mc, nil
}

func init() {
	tamate.Register(driverName, &mysqlDriver{})
}
