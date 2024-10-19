package main

import (
	"flag"
	"net/url"
	"time"

	"github.com/muidea/magicCommon/foundation/dao"
	"github.com/muidea/magicCommon/foundation/log"
	"github.com/muidea/magicCommon/foundation/util"
)

var databaseDSN = "root:rootkit@127.0.0.1:3306/test_db"
var configFile = "./config.json"

type Config struct {
	SQL []string `json:"sql"`
}

func main() {
	flag.StringVar(&databaseDSN, "databaseDSN", databaseDSN, "database dsn")
	flag.StringVar(&configFile, "configFile", configFile, "config file")
	flag.Parse()
	if databaseDSN == "" {
		flag.PrintDefaults()
		return
	}

	dsnUrl, dsnErr := url.Parse("mysql://" + databaseDSN)
	if dsnErr != nil {
		log.Errorf("illegal databaseDSN, error:%s", dsnErr.Error())
		return
	}

	configPtr := &Config{}
	configErr := util.LoadConfig(configFile, configPtr)
	if configErr != nil {
		log.Errorf("illegal config, error:%s", configErr.Error())
		return
	}

	dbUser := dsnUrl.User.Username()
	dbPassword, _ := dsnUrl.User.Password()
	dbName := ""
	if dsnUrl.Path != "" {
		dbName = dsnUrl.Path[1:]
	}
	if dbName == "" {
		dbName = "mysql"
	}

	startTime := time.Now()
	dbDao, dbErr := dao.Fetch(dbUser, dbPassword, dsnUrl.Host, dbName, "")
	if dbErr != nil {
		log.Errorf("fetch database dao failed, error:%s", dbErr.Error())
		return
	}

	for _, sql := range configPtr.SQL {
		_, err := dbDao.Execute(sql)
		if err == nil {
			log.Infof("%s execute %s finish！ elapse:%v", dbDao, sql, time.Since(startTime))
		} else {
			log.Errorf("%s execute %s failed！ error:%s, elapse:%v", dbDao, sql, err.Error(), time.Since(startTime))
		}
	}
}
