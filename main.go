package main

import (
	"fmt"

	"github.com/g-shifu/go-mysql-ssh/pkg"
)

func main() {
	//init
	cfg := pkg.NewConfig()
	cfg.DB_HOST = "your mysql host"
	cfg.DB_USER = "your mysql username"
	cfg.DB_PASS = "your mysql password"
	cfg.DB_SELECT = "select database"
	cfg.USE_SSH = true
	cfg.SSH_REMOTE = "ssh remote address"
	cfg.SSH_USER = "ssh user"
	cfg.SSH_PASS = "ssh password"
	pkg.InitDB(cfg)

	//test
	rtn, err := pkg.DBQueryRows("show tables;")
	fmt.Println(rtn, err)

	//close db
	defer pkg.CloseDB()
}
