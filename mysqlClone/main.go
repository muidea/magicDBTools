package main

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/muidea/magicCommon/foundation/dao"
	"github.com/muidea/magicCommon/foundation/log"
	"github.com/muidea/magicCommon/foundation/util"
)

/*
实现数据库clone
*/
var sourceDSN = "root:rootkit@127.0.0.1:3306/srcDB"
var targetDSN = "root:rootkit@127.0.0.1:3306/dstDB"
var batchSize = 3000
var cloneInstance = false
var excludeDBs = ""
var excludeTables = ""
var truncateTable = true
var skipAutoIncrement = false
var filterConfigFile = ""
var concurrencyNum = 10

type databaseFilter map[string]string

var filterConfig map[string]databaseFilter

func main() {
	flag.StringVar(&sourceDSN, "sourceDSN", sourceDSN, "source database info")
	flag.StringVar(&targetDSN, "targetDSN", targetDSN, "target database info")
	flag.IntVar(&batchSize, "batchSize", batchSize, "batch submit size")
	flag.IntVar(&concurrencyNum, "concurrency", concurrencyNum, "concurrency number for clone ")
	flag.BoolVar(&cloneInstance, "cloneInstance", cloneInstance, "clone all databases in instance")
	flag.BoolVar(&truncateTable, "truncateTable", truncateTable, "truncate table data before clone")
	flag.BoolVar(&skipAutoIncrement, "skipAutoIncrement", skipAutoIncrement, "skip auto increment")
	flag.StringVar(&excludeDBs, "excludes", excludeDBs, "exclude databases")
	flag.StringVar(&excludeTables, "excludeTables", excludeTables, "exclude tables")
	flag.StringVar(&filterConfigFile, "filterConfig", filterConfigFile, "filter config file")
	flag.Parse()
	if sourceDSN == "" || targetDSN == "" {
		flag.PrintDefaults()
		return
	}
	if sourceDSN == targetDSN {
		return
	}

	if filterConfigFile != "" {
		filterConfig = map[string]databaseFilter{}
		err := util.LoadConfig(filterConfigFile, &filterConfig)
		if err != nil {
			log.Errorf("load filter config failed, error:%s", err.Error())
			return
		}
	}

	sourceUrl, sourceErr := url.Parse("mysql://" + sourceDSN)
	targetUrl, targetErr := url.Parse("mysql://" + targetDSN)
	if sourceErr != nil || targetErr != nil {
		log.Errorf("illegal sourceDSN or targetDSN")
		return
	}

	sourceUser := sourceUrl.User.Username()
	sourcePassword, _ := sourceUrl.User.Password()
	sourceDB := ""
	if sourceUrl.Path != "" {
		sourceDB = sourceUrl.Path[1:]
	}

	startTime := time.Now()
	sourceDao, sourceErr := dao.Fetch(sourceUser, sourcePassword, sourceUrl.Host, sourceDB, "")
	if sourceErr != nil {
		log.Errorf("fetch source dao failed, error:%s", sourceErr.Error())
		return
	}
	defer sourceDao.Release()

	targetUser := targetUrl.User.Username()
	targetPassword, _ := targetUrl.User.Password()
	targetDB := ""
	if targetUrl.Path != "" {
		targetDB = targetUrl.Path[1:]
	}

	targetDao, targetErr := dao.Fetch(targetUser, targetPassword, targetUrl.Host, targetDB, "")
	if targetErr != nil {
		log.Errorf("fetch target dao failed, error:%s", targetErr.Error())
		return
	}
	defer targetDao.Release()

	targetErr = disableForeignKeyCheck(targetDao)
	if targetErr != nil {
		log.Errorf("disable %s foreign key check failed, error:%s", targetDao, targetErr.Error())
		return
	}
	defer enableForeignKeyCheck(targetDao)

	if !cloneInstance {
		if sourceDB != "" {
			err := cloneDatabase(sourceDao, targetDao, sourceDB, targetDB)
			if err == nil {
				log.Infof("clone database from %s to %s finish！ elapse:%v", sourceDao, targetDao, time.Since(startTime))
			} else {
				log.Errorf("clone database from %s to %s terminate！ err:%s, elapse:%v", sourceDao, targetDao, err.Error(), time.Since(startTime))
			}
		}

		return
	}

	databaseList, databaseErr := enumCloneDatabases(sourceDao)
	if databaseErr != nil {
		log.Errorf("enum database from %s failed, error:%s", sourceDao, databaseErr.Error())
		return
	}

	for _, val := range databaseList {
		err := cloneDatabase(sourceDao, targetDao, val, targetDB)
		if err == nil {
			log.Infof("clone database from %s to %s finish！ elapse:%v", sourceDao, targetDao, time.Since(startTime))
		} else {
			log.Errorf("clone database from %s to %s terminate！ err:%s, elapse:%v", sourceDao, targetDao, err.Error(), time.Since(startTime))
		}
	}
}

// SET @@session.unique_checks = 0;
// SET @@session.foreign_key_checks = 0;
func disableForeignKeyCheck(dbHandle dao.Dao) error {
	_, err := dbHandle.Execute("SET @@session.foreign_key_checks = 0")
	if err != nil {
		return err
	}
	_, err = dbHandle.Execute("SET @@session.unique_checks = 0")
	return err
}

func enableForeignKeyCheck(dbHandle dao.Dao) error {
	_, err := dbHandle.Execute("SET @@session.unique_checks = 1")
	dbHandle.Execute("SET @@session.foreign_key_checks = 1")
	return err
}

func enumCloneDatabases(dbHandle dao.Dao) ([]string, error) {
	excludeInternal := []string{
		"information_schema",
		"performance_schema",
		"mysql",
		"sys",
	}

	items := strings.Split(excludeDBs, ",")
	excludeInternal = append(excludeInternal, items...)

	err := dbHandle.Query("SHOW DATABASES")
	if err != nil {
		log.Errorf("enum %s database failed, error:%s", dbHandle, err.Error())
		return nil, err
	}

	var databaseNameList []string
	defer dbHandle.Finish()
	for dbHandle.Next() {
		var dbName string
		err = dbHandle.GetField(&dbName)
		if err != nil {
			log.Errorf("enum %s database get name failed, error:%s", dbHandle, err.Error())
			return nil, err
		}

		excludeFlag := false
		for _, val := range excludeInternal {
			if val == dbName {
				excludeFlag = true
			}
		}

		if !excludeFlag {
			databaseNameList = append(databaseNameList, dbName)
		}
	}

	return databaseNameList, nil
}

func cloneDatabase(sourceDao dao.Dao, targetDao dao.Dao, sourceDB, targetDB string) error {
	err := sourceDao.UseDatabase(sourceDB)
	if err != nil {
		log.Errorf("switch source %s database to %s failed, err:%s", sourceDao, sourceDB, err.Error())
		return err
	}
	if targetDB == "" {
		targetDB = sourceDB
	}

	err = targetDao.CreateDatabase(targetDB)
	if err != nil {
		log.Errorf("create target %s database to %s failed, err:%s", targetDao, targetDB, err.Error())
		return err
	}

	err = targetDao.UseDatabase(targetDB)
	if err != nil {
		log.Errorf("switch target %s database to %s failed, err:%s", targetDao, targetDB, err.Error())
		return err
	}
	if sourceDao.String() == targetDao.String() {
		return nil
	}

	time.Sleep(10 * time.Second)

	log.Warnf("clone database from %s to %s", sourceDao, targetDao)
	err = cloneAllTable(sourceDao, targetDao)
	if err == nil {
		log.Infof("clone database all table %s finish！", sourceDB)
	} else {
		log.Errorf("clone database all table %s terminate！ err:%s", sourceDB, err.Error())
		return err
	}

	err = cloneAllView(sourceDao, targetDao)
	if err == nil {
		log.Infof("clone database all view %s finish！", sourceDB)
	} else {
		log.Errorf("clone database all view %s terminate！ err:%s", sourceDB, err.Error())
		return err
	}

	err = cloneAllProcedure(sourceDao, targetDao)
	if err == nil {
		log.Infof("clone database all procedure %s finish！", sourceDB)
	} else {
		log.Errorf("clone database all procedure %s terminate！ err:%s", sourceDB, err.Error())
		return err
	}

	err = cloneAllTrigger(sourceDao, targetDao)
	if err == nil {
		log.Infof("clone database all trigger %s finish！", sourceDB)
	} else {
		log.Errorf("clone database all trigger %s terminate！ err:%s", sourceDB, err.Error())
	}

	return err
}

func cloneAllTable(sourceDao dao.Dao, targetDao dao.Dao) error {
	tableNameList, err := enumTables(sourceDao)
	if err != nil {
		log.Errorf("enum tables failed, error:%s", err.Error())
		return err
	}

	var idx = -1
	var tableNameLock sync.RWMutex
	fetchTable := func() (int, string) {
		tableNameLock.Lock()
		defer tableNameLock.Unlock()
		idx++
		if idx >= len(tableNameList) {
			return -1, ""
		}

		return idx, tableNameList[idx]
	}

	wg := sync.WaitGroup{}
	ii := 0
	for ii < concurrencyNum {
		tableIndex, tableName := fetchTable()
		if tableIndex == -1 || tableName == "" {
			break
		}
		ii++

		wg.Add(1)
		go func() {
			defer wg.Done()
			srcDao, srcErr := sourceDao.Duplicate()
			if srcErr != nil {
				log.Errorf("duplicate source %s dao failed, error:%s", sourceDao, srcErr)
				return
			}
			defer srcDao.Release()
			tgtDao, tgtErr := targetDao.Duplicate()
			if tgtErr != nil {
				log.Errorf("duplicate target %s dao failed, error:%s", targetDao, tgtErr)
				return
			}
			defer tgtDao.Release()
			tgtErr = disableForeignKeyCheck(tgtDao)
			if tgtErr != nil {
				log.Errorf("disable %s foreign key check failed, error:%s", tgtDao, tgtErr.Error())
				return
			}
			defer enableForeignKeyCheck(tgtDao)

			for {
				if tableIndex == -1 || tableName == "" {
					break
				}

				log.Infof("concurrency %d clone tableName %s from %s to %s", tableIndex, tableName, sourceDao, targetDao)
				err = cloneTable(srcDao, tgtDao, tableName)
				if err != nil {
					log.Errorf("clone tableName %s from %s to %s failed, error:%s", tableName, srcDao, tgtDao, err.Error())
				}

				tableIndex, tableName = fetchTable()
			}
		}()
	}
	wg.Wait()

	return err
}

func enumTables(dbHandle dao.Dao) ([]string, error) {
	err := dbHandle.Query(fmt.Sprintf("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_TYPE='%s' AND TABLE_SCHEMA='%s'", dao.BaseTable, dbHandle.DBName()))
	if err != nil {
		log.Errorf("enum %s tables failed, error:%s", dbHandle, err.Error())
		return nil, err
	}

	excludeItems := strings.Split(excludeTables, ",")
	checkExist := func(str string) bool {
		for _, val := range excludeItems {
			if val == str {
				return true
			}
		}

		return false
	}

	var tableNameList []string
	defer dbHandle.Finish()
	for dbHandle.Next() {
		var tableName string
		err = dbHandle.GetField(&tableName)
		if err != nil {
			log.Errorf("enum %s tables get name failed, error:%s", dbHandle, err.Error())
			return nil, err
		}

		if cloneInstance {
			tableNameList = append(tableNameList, tableName)
			continue
		}

		if !checkExist(tableName) {
			tableNameList = append(tableNameList, tableName)
		}
	}

	return tableNameList, nil
}
func disableKeys(dbHandle dao.Dao, tableName string) error {
	sql := fmt.Sprintf("ALTER TABLE `%s` DISABLE KEYS", tableName)
	_, err := dbHandle.Execute(sql)
	if err != nil {
		log.Errorf("disable table %s keys failed, error:%s", tableName, err.Error())
	}
	return err
}
func enableKeys(dbHandle dao.Dao, tableName string) error {
	sql := fmt.Sprintf("ALTER TABLE `%s` ENABLE KEYS", tableName)
	_, err := dbHandle.Execute(sql)
	if err != nil {
		log.Errorf("enable table %s keys failed, error:%s", tableName, err.Error())
	}
	return err
}

func cloneTable(sourceDB dao.Dao, targetDB dao.Dao, tableName string) error {
	log.Infof("clone table %s, %s->%s", tableName, sourceDB, targetDB)

	startTime := time.Now()
	var err error
	if truncateTable {
		err = dropTable(targetDB, tableName)
		if err != nil {
			log.Errorf("drop target table %s failed, error:%s", tableName, err.Error())
			return err
		}
	}

	checkFlag, _, checkErr := targetDB.CheckTableExist(tableName)
	if checkErr != nil {
		log.Errorf("check target table %s exist failed, error:%s", tableName, checkErr.Error())
		return checkErr
	}

	if !checkFlag {
		checkFlag, _, checkErr = sourceDB.CheckTableExist(tableName)
		if checkErr != nil {
			log.Errorf("check source table %s exist failed, error:%s", tableName, checkErr.Error())
			return checkErr
		}

		sqlVal, sqlErr := getCreateTableSQL(sourceDB, tableName)
		if sqlErr != nil {
			log.Errorf("get source table %s  create sql failed, error:%s", tableName, sqlErr.Error())
			return sqlErr
		}

		err = createTable(targetDB, sqlVal)
		if err != nil {
			log.Errorf("create target table %s failed, sql:%s, error:%s", tableName, sqlVal, err.Error())
			return err
		}

		err = resetTable(targetDB, tableName)
		if err != nil {
			log.Errorf("reset target table %s failed, error:%s", tableName, err.Error())
			return err
		}
	}

	totalRecordsCount := 0
	pkIndex, pkName, isAutoInc, columnList, columnErr := getTableColumns(sourceDB, tableName)
	if columnErr != nil {
		log.Errorf("get source table %s columns failed, error:%s", tableName, columnErr.Error())
		return columnErr
	}

	firstFlag := true
	rawTableColumns := ""
	for _, val := range columnList {
		if !firstFlag {
			rawTableColumns += ", "
		}
		rawTableColumns += fmt.Sprintf("`%s`", val)
		firstFlag = false
	}

	getTableFilter := func() string {
		if filterConfig == nil {
			return ""
		}

		dbFilter, dbOK := filterConfig[sourceDB.DBName()]
		if dbOK {
			tableFilter, tableOK := dbFilter[tableName]
			if tableOK {
				return tableFilter
			}
		}

		return ""
	}

	columnSize := len(columnList)
	pageSize := batchSize
	var offset interface{}
	values := make([]interface{}, columnSize)
	columnsName := extractColumns(rawTableColumns, pkIndex, isAutoInc)
	columnsPlaceHolder := generatePlaceholders(columnSize, pkIndex, isAutoInc)
	columnsValueBuffer := generateDataValue(columnSize, pkIndex, isAutoInc)
	tableFilter := getTableFilter()
	// insertCount, insertFinish, error
	splitBatchInsert := func(placeHolder []string, values []interface{}, columnsCount int) (int, bool, error) {
		insertCount := 0
		insertFinish := false
		for batchLen := len(placeHolder); batchLen > 0; batchLen /= 2 {
			var insertErr error
			for sIdx := 0; sIdx < len(placeHolder); sIdx += batchLen {
				eIdx := sIdx + batchLen
				if eIdx > len(placeHolder) {
					eIdx = len(placeHolder)
				}
				sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", tableName, columnsName, strings.Join(placeHolder[sIdx:eIdx], ","))
				_, insertErr = targetDB.Execute(sql, values[sIdx*columnsCount:eIdx*columnsCount]...)
				if insertErr != nil {
					var mysqlError *mysql.MySQLError
					if !errors.As(insertErr, &mysqlError) || mysqlError.Number != 1390 {
						log.Errorf("insert %s target table %s row failed, error:%s", targetDB, tableName, insertErr.Error())
						return 0, false, insertErr
					}
					break
				}

				insertCount += batchLen
				if eIdx == len(placeHolder) {
					insertFinish = true
					break
				}
			}

			if insertCount > 0 && insertErr != nil {
				return insertCount, insertFinish, nil
			}

			if insertFinish {
				break
			}
		}

		return insertCount, insertFinish, nil
	}

	batchClone := func(pageNum int) (count int, err error) {
		startCloneTime := time.Now()
		var queryElapse time.Duration
		var inertElapse time.Duration
		err = targetDB.BeginTransaction()
		if err != nil {
			log.Errorf("targetDB %s BeginTransaction failed, error:%s", targetDB, err.Error())
			return
		}

		defer func() {
			startCommitTime := time.Now()
			if err == nil {
				err = targetDB.CommitTransaction()
				if err != nil {
					log.Errorf("targetDB %s CommitTransaction failed, error:%s", targetDB, err.Error())
				}
			} else {
				err = targetDB.RollbackTransaction()
				if err != nil {
					log.Errorf("targetDB %s RollbackTransaction failed, error:%s", targetDB, err.Error())
				}
			}

			log.Infof("batchClone, batch size:%d, all elapse:%v, query:%v, insert:%v, commit:%v", count, time.Since(startCloneTime), queryElapse, inertElapse, time.Since(startCommitTime))
		}()

		log.Infof("batch clone %s from %s to %s, pageSize:%d, offset:%v", tableName, sourceDB, targetDB, pageSize, offset)
		if pkName == "" || tableFilter != "" {
			offset = pageNum * pageSize
		}

		if tableFilter == "" {
			if pkName == "" {
				err = sourceDB.Query(fmt.Sprintf("SELECT %s FROM `%s` LIMIT %d OFFSET %v", rawTableColumns, tableName, pageSize, offset))
			} else {
				if offset != nil {
					switch offset.(type) {
					case string, []byte:
						err = sourceDB.Query(fmt.Sprintf("SELECT %s FROM `%s` WHERE `%s` > '%s' ORDER BY `%s` LIMIT %d", rawTableColumns, tableName, pkName, offset, pkName, pageSize))
					default:
						err = sourceDB.Query(fmt.Sprintf("SELECT %s FROM `%s` WHERE `%s` > %v ORDER BY `%s` LIMIT %d", rawTableColumns, tableName, pkName, offset, pkName, pageSize))
					}
				} else {
					err = sourceDB.Query(fmt.Sprintf("SELECT %s FROM `%s` ORDER BY `%s` LIMIT %d", rawTableColumns, tableName, pkName, pageSize))
				}
			}
		} else {
			err = sourceDB.Query(fmt.Sprintf("SELECT %s FROM `%s` WHERE %s LIMIT %d OFFSET %v", rawTableColumns, tableName, tableFilter, pageSize, offset))
		}
		if err != nil {
			log.Errorf("query table %s records failed, error:%s", tableName, err.Error())
			return
		}

		queryElapse = time.Since(startCloneTime)

		count = 0
		allColumnsPlaceHolder := []string{}
		allColumnsValues := []interface{}{}
		columnsCount := 0
		defer sourceDB.Finish()
		for sourceDB.Next() {
			for i := range values {
				values[i] = new(interface{})
			}

			err = sourceDB.GetField(values...)
			if err != nil {
				log.Errorf("scan table %s record failed, error:%s", tableName, err.Error())
				return
			}

			// 插入数据到实例B的表
			pkVal, columnsValue := extractValues(values, columnsValueBuffer, pkIndex, isAutoInc)
			columnsCount = len(columnsValue)
			allColumnsPlaceHolder = append(allColumnsPlaceHolder, columnsPlaceHolder)
			allColumnsValues = append(allColumnsValues, columnsValue...)
			if pkVal != nil {
				offset = pkVal
			}

			count++
		}

		startInsertTime := time.Now()
		if count > 0 {
			valueOffset := 0
			for {
				insertCount, insertFinish, insertErr := splitBatchInsert(allColumnsPlaceHolder[valueOffset:], allColumnsValues[valueOffset*columnsCount:], columnsCount)
				if insertErr != nil {
					err = insertErr
					log.Errorf("split batch insert %s target table %s row failed, error:%s", targetDB, tableName, err.Error())
					break
				}
				if insertFinish {
					break
				}

				valueOffset += insertCount
			}
		}

		inertElapse = time.Since(startInsertTime)
		return
	}

	disableKeys(targetDB, tableName)
	defer enableKeys(targetDB, tableName)

	pageNum := 0
	for {
		cloneCount, cloneErr := batchClone(pageNum)
		if cloneErr != nil {
			log.Errorf("batch clone %s failed, pageNum:%d, error:%s", tableName, pageNum, cloneErr.Error())
			return cloneErr
		}

		totalRecordsCount += cloneCount
		if cloneCount < pageSize {
			break
		}

		pageNum++
	}

	log.Warnf("clone table %s finish, records:%d, elapsed time:%v, %s->%s", tableName, totalRecordsCount, time.Since(startTime), sourceDB, targetDB)
	return nil
}

func getCreateTableSQL(dbHandle dao.Dao, tableName string) (string, error) {
	err := dbHandle.Query(fmt.Sprintf("SHOW CREATE TABLE `%s`", tableName))
	if err != nil {
		return "", err
	}

	var tableCreateList []string
	defer dbHandle.Finish()
	for dbHandle.Next() {
		var tableNameVal string
		var tableCreateVal string
		err = dbHandle.GetField(&tableNameVal, &tableCreateVal)
		if err != nil {
			log.Errorf("%s get table %s create sql failed, error:%s", dbHandle, tableName, err.Error())
			return "", err
		}

		tableCreateList = append(tableCreateList, tableCreateVal)
	}
	if len(tableCreateList) == 0 {
		return "", fmt.Errorf("table %s does not exist", tableName)
	}

	return tableCreateList[0], nil
}

func createTable(dbHandle dao.Dao, createSQL string) error {
	_, err := dbHandle.Execute(createSQL)
	return err
}

func dropTable(dbHandle dao.Dao, tableName string) error {
	_, err := dbHandle.Execute(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", tableName))
	return err
}

func resetTable(dbHandle dao.Dao, tableName string) error {
	_, err := dbHandle.Execute(fmt.Sprintf("TRUNCATE TABLE `%s`", tableName))
	return err
}

/*
pkIndex,pkName,isAutoInc, columns, error
*/
func getTableColumns(dbHandle dao.Dao, tableName string) (int, string, bool, []string, error) {
	err := dbHandle.Query(fmt.Sprintf("SHOW COLUMNS FROM `%s`", tableName))
	if err != nil {
		return -1, "", false, nil, err
	}
	const autoIncrement = "auto_increment"

	var pkIndex = -1
	var pkName string
	var isAutoInc = false
	var columns []string
	defer dbHandle.Finish()
	index := 0
	for dbHandle.Next() {
		var column, dataType, isNull, isKey, defaultVal, extraVal sql.NullString
		err = dbHandle.GetField(&column, &dataType, &isNull, &isKey, &defaultVal, &extraVal)
		if err != nil {
			log.Errorf("%s get table %s columns failed, error:%s", dbHandle, tableName, err.Error())
			return -1, "", false, nil, err
		}
		if isKey.String == "PRI" {
			pkName = column.String
			pkIndex = index
		}

		if !isAutoInc {
			isAutoInc = extraVal.String == autoIncrement
		}

		index++
		columns = append(columns, column.String)
	}

	return pkIndex, pkName, isAutoInc, columns, nil
}

func extractColumns(selectColumns string, pkIndex int, isAutoInc bool) string {
	firstFlag := true
	columnsName := ""
	for i, v := range strings.Split(selectColumns, ",") {
		if pkIndex == i && isAutoInc && skipAutoIncrement {
			continue
		}

		if !firstFlag {
			columnsName += ", "
		}
		columnsName += v
		firstFlag = false
	}

	return columnsName
}

func generatePlaceholders(count, pkIndex int, isAutoInc bool) string {
	firstFlag := true
	placeholders := "("
	for i := 0; i < count; i++ {
		if pkIndex == i && isAutoInc && skipAutoIncrement {
			continue
		}

		if !firstFlag {
			placeholders += ","
		}
		placeholders += "?"
		firstFlag = false
	}
	placeholders += ")"

	return placeholders
}

func generateDataValue(count, pkIndex int, isAutoInc bool) []interface{} {
	valueCount := 0
	for i := 0; i < count; i++ {
		if pkIndex == i && isAutoInc && skipAutoIncrement {
			continue
		}

		valueCount++
	}

	return make([]interface{}, valueCount)
}

func extractValues(values []interface{}, valueBuff []interface{}, pkIndex int, isAutoInc bool) (interface{}, []interface{}) {
	var pkVal interface{}
	offset := 0
	for i, v := range values {
		if pkIndex == i {
			pkVal = *(v.(*interface{}))
			if isAutoInc && skipAutoIncrement {
				continue
			}
		}

		valueBuff[offset] = *(v.(*interface{}))
		offset++
	}

	return pkVal, valueBuff
}

func cloneAllView(sourceDao dao.Dao, targetDao dao.Dao) error {
	viewNameList, err := enumView(sourceDao)
	if err != nil {
		log.Errorf("enum view failed, error:%s", err.Error())
		return err
	}

	for _, view := range viewNameList {
		err = cloneView(sourceDao, targetDao, view)
		if err != nil {
			log.Errorf("clone view %s failed, error:%s", view, err.Error())
			break
		}
	}

	return err
}

func enumView(dbHandle dao.Dao) ([]string, error) {
	err := dbHandle.Query(fmt.Sprintf("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_TYPE='%s' AND TABLE_SCHEMA='%s'", dao.View, dbHandle.DBName()))
	if err != nil {
		log.Errorf("enum %s views failed, error:%s", dbHandle, err.Error())
		return nil, err
	}

	var viewNameList []string
	defer dbHandle.Finish()
	for dbHandle.Next() {
		var view string
		err = dbHandle.GetField(&view)
		if err != nil {
			log.Errorf("enum %s views get name failed, error:%s", dbHandle, err.Error())
			return nil, err
		}

		viewNameList = append(viewNameList, view)
	}

	return viewNameList, nil
}

func cloneView(sourceDB dao.Dao, targetDB dao.Dao, viewName string) error {
	log.Infof("clone view %s, %s->%s", viewName, sourceDB, targetDB)

	err := dropView(targetDB, viewName)
	if err != nil {
		log.Errorf("drop target view %s failed, error:%s", viewName, err.Error())
		return err
	}

	sqlVal, sqlErr := getCreateViewSQL(sourceDB, viewName)
	if sqlErr != nil {
		log.Errorf("get source view %s  create sql failed, error:%s", viewName, sqlErr.Error())
		return sqlErr
	}

	err = createView(targetDB, sqlVal)
	if err != nil {
		log.Errorf("create target view %s failed, error:%s", viewName, err.Error())
		return err
	}

	log.Warnf("clone view %s finish, %s->%s", viewName, sourceDB, targetDB)
	return nil
}

func getCreateViewSQL(dbHandle dao.Dao, viewName string) (string, error) {
	err := dbHandle.Query(fmt.Sprintf("SHOW CREATE VIEW `%s`", viewName))
	if err != nil {
		return "", err
	}

	var viewCreateList []string
	defer dbHandle.Finish()
	for dbHandle.Next() {
		var viewNameVal string
		var viewCreateVal string
		var charSet string
		var temp string
		err = dbHandle.GetField(&viewNameVal, &viewCreateVal, &charSet, &temp)
		if err != nil {
			log.Errorf("%s get view %s create sql failed, error:%s", dbHandle, viewName, err.Error())
			return "", err
		}

		viewCreateList = append(viewCreateList, viewCreateVal)
	}
	if len(viewCreateList) == 0 {
		return "", fmt.Errorf("view %s does not exist", viewName)
	}

	return viewCreateList[0], nil
}

func createView(dbHandle dao.Dao, createSQL string) error {
	_, err := dbHandle.Execute(createSQL)
	return err
}

func dropView(dbHandle dao.Dao, viewName string) error {
	_, err := dbHandle.Execute(fmt.Sprintf("DROP VIEW IF EXISTS %s", viewName))
	return err
}

func cloneAllProcedure(sourceDao dao.Dao, targetDao dao.Dao) error {
	procedureNameList, err := enumProcedure(sourceDao)
	if err != nil {
		log.Errorf("enum procedure failed, error:%s", err.Error())
		return err
	}

	for _, procedure := range procedureNameList {
		err = cloneProcedure(sourceDao, targetDao, procedure)
		if err != nil {
			log.Errorf("clone procedure %s failed, error:%s", procedure, err.Error())
			break
		}
	}

	return err
}

func enumProcedure(dbHandle dao.Dao) ([]string, error) {
	err := dbHandle.Query(fmt.Sprintf(
		`SELECT ROUTINE_NAME AS ProcedureName
				FROM INFORMATION_SCHEMA.ROUTINES
				WHERE ROUTINE_TYPE = 'PROCEDURE' AND ROUTINE_SCHEMA = '%s'`, dbHandle.DBName()))
	if err != nil {
		log.Errorf("enum %s procedure failed, error:%s", dbHandle, err.Error())
		return nil, err
	}

	var procedureNameList []string
	defer dbHandle.Finish()
	for dbHandle.Next() {
		var procedureName string
		err = dbHandle.GetField(&procedureName)
		if err != nil {
			log.Errorf("enum %s procedure get name failed, error:%s", dbHandle, err.Error())
			return nil, err
		}

		procedureNameList = append(procedureNameList, procedureName)
	}

	return procedureNameList, nil
}

func cloneProcedure(sourceDB dao.Dao, targetDB dao.Dao, procedureName string) error {
	log.Infof("clone procedure %s, %s->%s", procedureName, sourceDB, targetDB)

	err := dropProcedure(targetDB, procedureName)
	if err != nil {
		log.Errorf("drop target procedure %s failed, error:%s", procedureName, err.Error())
		return err
	}

	sqlVal, sqlErr := getCreateProcedureSQL(sourceDB, procedureName)
	if sqlErr != nil {
		log.Errorf("get source procedure %s  create sql failed, error:%s", procedureName, sqlErr.Error())
		return sqlErr
	}

	err = createProcedure(targetDB, sqlVal)
	if err != nil {
		log.Errorf("create target procedure %s failed, error:%s", procedureName, err.Error())
		return err
	}

	log.Warnf("clone procedure %s finish, %s->%s", procedureName, sourceDB, targetDB)
	return nil
}

func getCreateProcedureSQL(dbHandle dao.Dao, procedureName string) (string, error) {
	err := dbHandle.Query(fmt.Sprintf("SHOW CREATE PROCEDURE %s", procedureName))
	if err != nil {
		return "", err
	}

	var procedureCreateList []string
	defer dbHandle.Finish()
	for dbHandle.Next() {
		var name string
		var mode string
		var create string
		var charset string
		var collation string
		var database string
		err = dbHandle.GetField(&name, &mode, &create, &charset, &collation, &database)
		if err != nil {
			log.Errorf("%s get procedure %s create sql failed, error:%s", dbHandle, procedureName, err.Error())
			return "", err
		}

		procedureCreateList = append(procedureCreateList, create)
	}
	if len(procedureCreateList) == 0 {
		return "", fmt.Errorf("procedure %s does not exist", procedureName)
	}

	return procedureCreateList[0], nil
}

func createProcedure(dbHandle dao.Dao, createSQL string) error {
	_, err := dbHandle.Execute(createSQL)
	return err
}

func dropProcedure(dbHandle dao.Dao, procedureName string) error {
	_, err := dbHandle.Execute(fmt.Sprintf("DROP PROCEDURE IF EXISTS %s", procedureName))
	return err
}

func cloneAllTrigger(sourceDao dao.Dao, targetDao dao.Dao) error {
	triggerNameList, err := enumTrigger(sourceDao)
	if err != nil {
		log.Errorf("enum trigger failed, error:%s", err.Error())
		return err
	}

	for _, trigger := range triggerNameList {
		err = cloneTrigger(sourceDao, targetDao, trigger)
		if err != nil {
			log.Errorf("clone trigger %s failed, error:%s", trigger, err.Error())
			break
		}
	}

	return err
}

func enumTrigger(dbHandle dao.Dao) ([]string, error) {
	err := dbHandle.Query(fmt.Sprintf(
		`SELECT TRIGGER_NAME
				FROM INFORMATION_SCHEMA.TRIGGERS
				WHERE TRIGGER_SCHEMA = '%s'`, dbHandle.DBName()))
	if err != nil {
		log.Errorf("enum %s trigger failed, error:%s", dbHandle, err.Error())
		return nil, err
	}

	var triggerNameList []string
	defer dbHandle.Finish()
	for dbHandle.Next() {
		var triggerName string
		err = dbHandle.GetField(&triggerName)
		if err != nil {
			log.Errorf("enum %s trigger get name failed, error:%s", dbHandle, err.Error())
			return nil, err
		}

		triggerNameList = append(triggerNameList, triggerName)
	}

	return triggerNameList, nil
}

func cloneTrigger(sourceDB dao.Dao, targetDB dao.Dao, triggerName string) error {
	log.Infof("clone trigger %s, %s->%s", triggerName, sourceDB, targetDB)

	err := dropTrigger(targetDB, triggerName)
	if err != nil {
		log.Errorf("drop target trigger %s failed, error:%s", triggerName, err.Error())
		return err
	}

	sqlVal, sqlErr := getCreateTriggerSQL(sourceDB, triggerName)
	if sqlErr != nil {
		log.Errorf("get source trigger %s  create sql failed, error:%s", triggerName, sqlErr.Error())
		return sqlErr
	}

	err = createTrigger(targetDB, sqlVal)
	if err != nil {
		log.Errorf("create target trigger %s failed, error:%s", triggerName, err.Error())
		return err
	}

	log.Warnf("clone trigger %s finish, %s->%s", triggerName, sourceDB, targetDB)
	return nil
}

func getCreateTriggerSQL(dbHandle dao.Dao, triggerName string) (string, error) {
	err := dbHandle.Query(fmt.Sprintf("SHOW CREATE TRIGGER %s", triggerName))
	if err != nil {
		return "", err
	}

	var triggerCreateList []string
	defer dbHandle.Finish()
	for dbHandle.Next() {
		var name string
		var mode string
		var create string
		var charset string
		var collation string
		var database string
		var createDt string
		err = dbHandle.GetField(&name, &mode, &create, &charset, &collation, &database, &createDt)
		if err != nil {
			log.Errorf("%s get trigger %s create sql failed, error:%s", dbHandle, triggerName, err.Error())
			return "", err
		}

		triggerCreateList = append(triggerCreateList, create)
	}
	if len(triggerCreateList) == 0 {
		return "", fmt.Errorf("trigger %s does not exist", triggerName)
	}

	return triggerCreateList[0], nil
}

func createTrigger(dbHandle dao.Dao, createSQL string) error {
	_, err := dbHandle.Execute(createSQL)
	return err
}

func dropTrigger(dbHandle dao.Dao, triggerName string) error {
	_, err := dbHandle.Execute(fmt.Sprintf("DROP TRIGGER IF EXISTS %s", triggerName))
	return err
}
