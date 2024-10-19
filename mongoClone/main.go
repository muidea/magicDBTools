package main

import (
	"context"
	"flag"
	"net/url"
	"strings"
	"time"

	"github.com/muidea/magicCommon/foundation/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var authMod = "authSource=admin"
var sourceDSN = "root:mongoSupos@127.0.0.1:27018/"
var targetDSN = "root:rootkit@127.0.0.1:27017/"
var batchSize = 2000
var cloneInstance = false
var excludeDBs = ""
var cleanCollection = false

func main() {
	flag.StringVar(&sourceDSN, "sourceDSN", sourceDSN, "source database info")
	flag.StringVar(&targetDSN, "targetDSN", targetDSN, "target database info")
	flag.IntVar(&batchSize, "batchSize", batchSize, "batch submit size")
	flag.BoolVar(&cloneInstance, "cloneInstance", cloneInstance, "clone all schema in database")
	flag.BoolVar(&cleanCollection, "dropCollection", cleanCollection, "empty collection data before copying")
	flag.StringVar(&excludeDBs, "excludes", excludeDBs, "exclude databases")
	flag.Parse()
	if sourceDSN == "" || targetDSN == "" {
		flag.PrintDefaults()
		return
	}
	if sourceDSN == targetDSN {
		return
	}

	sourceUrl, sourceErr := url.Parse("mongodb://" + sourceDSN)
	targetUrl, targetErr := url.Parse("mongodb://" + targetDSN)
	if sourceErr != nil || targetErr != nil {
		log.Errorf("illegal sourceDSN or targetDSN")
		return
	}
	sourceUrl.RawQuery = authMod
	targetUrl.RawQuery = authMod

	startTime := time.Now()
	sourceDao, sourceErr := mongo.Connect(context.Background(), options.Client().ApplyURI(sourceUrl.String()))
	if sourceErr != nil {
		log.Errorf("connect source mongodb failed, error:%s", sourceErr.Error())
		return
	}
	defer sourceDao.Disconnect(context.Background())

	targetDao, targetErr := mongo.Connect(context.Background(), options.Client().ApplyURI(targetUrl.String()))
	if targetErr != nil {
		log.Errorf("connect target mongodb failed, error:%s", targetErr.Error())
		return
	}
	defer targetDao.Disconnect(context.Background())

	sourceDBName := ""
	if sourceUrl.Path != "" {
		sourceDBName = sourceUrl.Path[1:]
	}
	targetDBName := ""
	if targetUrl.Path != "" {
		targetDBName = targetUrl.Path[1:]
	}

	if !cloneInstance {
		if sourceDBName != "" {
			err := cloneDatabase(sourceDao, targetDao, sourceDBName, targetDBName)
			if err == nil {
				log.Infof("clone database from %s:%v to %s:%v finish！ elapse:%v", sourceUrl.Host, sourceDBName, targetUrl.Host, targetDBName, time.Since(startTime))
			} else {
				log.Errorf("clone database from %s:%v to %s:%v terminate！ elapse:%v", sourceUrl.Host, sourceDBName, targetUrl.Host, targetDBName, time.Since(startTime))
			}
		}

		return
	}

	databaseList, databaseErr := enumCloneDatabase(sourceDao)
	if databaseErr != nil {
		log.Errorf("enum database from %s failed, error:%s", sourceDSN, databaseErr.Error())
		return
	}

	for _, val := range databaseList {
		newTarget := targetDBName
		if newTarget == "" {
			newTarget = val
		}

		err := cloneDatabase(sourceDao, targetDao, val, newTarget)
		if err == nil {
			log.Infof("clone database from %s/%v to %s/%v finish！ elapse:%v", sourceUrl.Host, val, targetUrl.Host, newTarget, time.Since(startTime))
		} else {
			log.Errorf("clone database from %s/%v to %s/%v terminate！ elapse:%v", sourceUrl.Host, val, targetUrl.Host, newTarget, time.Since(startTime))
		}
	}
}

func enumCloneDatabase(client *mongo.Client) (ret []string, err error) {
	excludeInternal := []string{
		"admin",
		"config",
		"local",
	}
	items := strings.Split(excludeDBs, ",")
	excludeInternal = append(excludeInternal, items...)

	databaseList, databaseErr := client.ListDatabases(context.Background(), bson.D{{}})
	if databaseErr != nil {
		log.Errorf("list mongodb databases failed, error:%s", databaseErr.Error())
		return
	}

	for _, val := range databaseList.Databases {
		excludeFlag := false
		for _, sv := range excludeInternal {
			if sv == val.Name {
				excludeFlag = true
			}
		}

		if !excludeFlag {
			ret = append(ret, val.Name)
		}
	}

	return
}

func cloneDatabase(sourceClient *mongo.Client, targetClient *mongo.Client, sourceDBName, targetDBName string) (err error) {
	dbSource := sourceClient.Database(sourceDBName)
	dbTarget := targetClient.Database(targetDBName)

	collectionList, collectionErr := dbSource.ListCollectionNames(context.Background(), bson.D{{}})
	if collectionErr != nil {
		log.Errorf("list source mongodb collection failed, error:%s", collectionErr.Error())
		err = collectionErr
		return
	}

	for _, collectionName := range collectionList {
		collectionSource := dbSource.Collection(collectionName)
		collectionTarget := dbTarget.Collection(collectionName)
		if cleanCollection {
			_ = collectionTarget.Drop(context.Background())
		}

		cursorPtr, cursorErr := collectionSource.Find(context.Background(), bson.D{{}})
		if cursorErr != nil {
			log.Errorf("load source mongodb collection failed, error:%s", cursorErr.Error())
			err = cursorErr
			return
		}

		itemCount := 0
		var documents []interface{}
		for cursorPtr.Next(context.Background()) {
			var document interface{}
			if err = cursorPtr.Decode(&document); err != nil {
				log.Errorf("decode source mongodb collection document failed, error:%s", err.Error())
				return
			}
			documents = append(documents, document)
			itemCount++
			if itemCount >= batchSize {
				log.Infof("batch save target mongodb, collection:%s, itemCount:%d", collectionName, itemCount)
				_, cursorErr = collectionTarget.InsertMany(context.Background(), documents)
				if cursorErr != nil {
					log.Errorf("batch save target mongodb failed, error:%s", cursorErr.Error())
					err = cursorErr
					return
				}

				itemCount = 0
				documents = documents[:0]
			}
		}

		if itemCount > 0 {
			log.Infof("batch save target mongodb, collection:%s, itemCount:%d", collectionName, itemCount)
			_, cursorErr = collectionTarget.InsertMany(context.Background(), documents)
			if cursorErr != nil {
				log.Errorf("batch save target mongodb failed, error:%s", cursorErr.Error())
				err = cursorErr
				return
			}
		}

		if err = cursorPtr.Err(); err != nil {
			log.Errorf("visit source mongodb collection failed, error:%s", err.Error())
			return
		}

		cursorPtr.Close(context.Background())
	}

	return
}
