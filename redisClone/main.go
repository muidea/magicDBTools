package main

import (
	"context"
	"flag"
	"net/url"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/muidea/magicCommon/foundation/log"
	"github.com/muidea/magicCommon/foundation/util"
)

var sourceDSN = "root:rootkit@127.0.0.1:6379/"
var targetDSN = "root:redisPassword@192.168.236.163:6379,192.168.236.163:6380,192.168.236.164:6379,192.168.236.164:6380,192.168.236.165:6379,192.168.236.165:6380/"
var configFile = "./config.json"

type Config struct {
	Keys map[string]string `json:"keys"`
}

func isCluster(svrAddr string) bool {
	items := strings.Split(svrAddr, ",")
	return len(items) > 1
}

func main() {
	flag.StringVar(&sourceDSN, "sourceDSN", sourceDSN, "source redis info")
	flag.StringVar(&targetDSN, "targetDSN", targetDSN, "target redis info")
	flag.StringVar(&configFile, "configFile", configFile, "config file")
	flag.Parse()
	if sourceDSN == "" || targetDSN == "" {
		flag.PrintDefaults()
		return
	}
	if sourceDSN == targetDSN {
		return
	}

	sourceUrl, sourceErr := url.Parse("tcp://" + sourceDSN)
	targetUrl, targetErr := url.Parse("tcp://" + targetDSN)
	if sourceErr != nil || targetErr != nil {
		log.Errorf("illegal sourceDSN or targetDSN")
		return
	}

	configPtr := &Config{}
	configErr := util.LoadConfig(configFile, configPtr)
	if configErr != nil {
		log.Errorf("illegal config, error:%s", configErr.Error())
		return
	}

	var sourceClient *redis.Client
	var targetClient *redis.Client
	var targetClusterClient *redis.ClusterClient

	sourcePassword, _ := sourceUrl.User.Password()
	sourceClient = redis.NewClient(&redis.Options{
		Addr:     sourceUrl.Host, // Redis 服务器地址
		Password: sourcePassword, // 密码，如果有的话
	})

	defer sourceClient.Close()

	targetPassword, _ := targetUrl.User.Password()
	if isCluster(targetUrl.Host) {
		targetClusterClient = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:    strings.Split(targetUrl.Host, ","),
			Password: targetPassword,
		})
	} else {
		targetClient = redis.NewClient(&redis.Options{
			Addr:     targetUrl.Host, // Redis 服务器地址
			Password: targetPassword, // 密码，如果有的话
		})
	}

	defer func() {
		if targetClient != nil {
			targetClient.Close()
		}
		if targetClusterClient != nil {
			targetClusterClient.Close()
		}
	}()

	for k, v := range configPtr.Keys {
		exists, err := sourceClient.Exists(context.Background(), k).Result()
		if err != nil {
			log.Errorf("check key:%s exist error:%s", k, err.Error())
			continue
		}
		if exists != 1 {
			log.Warnf("key %s does not exist", k)
			continue
		}

		keyType, keyErr := sourceClient.Type(context.Background(), k).Result()
		if keyErr != nil {
			log.Errorf("check key:%s type error:%s", k, err.Error())
			continue
		}
		if isCluster(targetUrl.Host) {
			switch keyType {
			case "string":
				err = syncStringToCluster(sourceClient, targetClusterClient, k, v)
			case "list":
				err = syncListToCluster(sourceClient, targetClusterClient, k, v)
			case "set":
				err = syncSSetToCluster(sourceClient, targetClusterClient, k, v)
			case "zset":
				err = syncZSetToCluster(sourceClient, targetClusterClient, k, v)
			case "hash":
				err = syncHashToCluster(sourceClient, targetClusterClient, k, v)
			default:
				log.Warnf("illegal keyType:%s", keyType)
			}
		} else {
			switch keyType {
			case "string":
				err = syncString(sourceClient, targetClient, k, v)
			case "list":
				err = syncList(sourceClient, targetClient, k, v)
			case "set":
				err = syncSSet(sourceClient, targetClient, k, v)
			case "zset":
				err = syncZSet(sourceClient, targetClient, k, v)
			case "hash":
				err = syncHash(sourceClient, targetClient, k, v)
			default:
				log.Warnf("illegal keyType:%s", keyType)
			}
		}

		if err != nil {
			log.Errorf("syncRedis %s from %s to %s failed, error:%s", k, sourceDSN, targetDSN, err.Error())
			continue
		}

		log.Warnf("syncRedis %s from %s to %s ok ", k, sourceDSN, targetDSN)
	}

	log.Warnf("syncRedis from %s to %s finish!", sourceDSN, targetDSN)
}

func syncString(sourceClient *redis.Client, targetClient *redis.Client, sourceKey, targetKey string) (err error) {
	strVal, strErr := sourceClient.Get(context.Background(), sourceKey).Result()
	if strErr != nil {
		err = strErr
		log.Errorf("get source key:%s, error:%s", sourceKey, err)
		return
	}
	err = targetClient.Set(context.Background(), targetKey, strVal, 5*time.Second).Err()
	if err != nil {
		log.Errorf("set target key:%s, error:%s", targetKey, err)
		return
	}
	log.Infof("set target key:%s ok", targetKey)
	return
}

func syncStringToCluster(sourceClient *redis.Client, targetClient *redis.ClusterClient, sourceKey, targetKey string) (err error) {
	strVal, strErr := sourceClient.Get(context.Background(), sourceKey).Result()
	if strErr != nil {
		err = strErr
		log.Errorf("get source key:%s, error:%s", sourceKey, err)
		return
	}
	err = targetClient.Set(context.Background(), targetKey, strVal, 5*time.Second).Err()
	if err != nil {
		log.Errorf("set target key:%s, error:%s", targetKey, err)
		return
	}

	log.Infof("set target key:%s ok", targetKey)
	return
}

func syncList(sourceClient *redis.Client, targetClient *redis.Client, sourceKey, targetKey string) (err error) {
	var cursor int64
	var batchSize int64 = 1000

	totalCount := 0
	for {
		var values []string
		values, err = sourceClient.LRange(context.Background(), sourceKey, cursor, cursor+batchSize).Result()
		if err != nil {
			log.Errorf("Error scanning source list key:%s, error:%s", sourceKey, err)
			return
		}

		for _, value := range values {
			_, err = targetClient.RPush(context.Background(), targetKey, value).Result()
			if err != nil {
				log.Errorf("Error inserting value into destination list, key:%s, error:%s", targetKey, err)
				return
			}
			totalCount++
		}

		cursor += batchSize
		if int64(len(values)) < batchSize {
			break
		}
	}

	log.Infof("set target key:%s, totalCount:%d ok", targetKey, totalCount)
	return
}

func syncListToCluster(sourceClient *redis.Client, targetClient *redis.ClusterClient, sourceKey, targetKey string) (err error) {
	var cursor int64
	var batchSize int64 = 1000
	totalCount := 0
	for {
		var values []string
		values, err = sourceClient.LRange(context.Background(), sourceKey, cursor, cursor+batchSize).Result()
		if err != nil {
			log.Errorf("Error scanning source list key:%s, error:%s", sourceKey, err)
			return
		}

		for _, value := range values {
			_, err = targetClient.RPush(context.Background(), targetKey, value).Result()
			if err != nil {
				log.Errorf("Error inserting value into destination list, key:%s, error:%s", targetKey, err)
				return
			}
			totalCount++
		}

		cursor += batchSize
		if int64(len(values)) < batchSize {
			break
		}
	}

	log.Infof("set target key:%s, totalCount:%d ok", targetKey, totalCount)
	return
}

func syncHash(sourceClient *redis.Client, targetClient *redis.Client, sourceKey, targetKey string) (err error) {
	batchSize := 1000

	var keys []string
	var cursor uint64
	totalCount := 0
	for {
		keys, cursor, err = sourceClient.HScan(context.Background(), sourceKey, cursor, "*", int64(batchSize)).Result()
		if err != nil {
			log.Errorf("Error scanning hash values, key:%s, error:%s", sourceKey, err.Error())
			return
		}

		pipeline := targetClient.Pipeline()
		for i := 0; i < len(keys); i += 2 {
			pipeline.HSet(context.Background(), targetKey, keys[i], keys[i+1])
			totalCount++
		}
		_, err = pipeline.Exec(context.Background())
		if err != nil {
			log.Errorf("Error setting hash values in target Redis, key:%s, error:%s", targetKey, err.Error())
			return
		}

		if cursor == 0 {
			break
		}
	}

	log.Infof("set target key:%s, totalCount:%d ok", targetKey, totalCount)
	return
}

func syncHashToCluster(sourceClient *redis.Client, targetClient *redis.ClusterClient, sourceKey, targetKey string) (err error) {
	batchSize := 1000

	var keys []string
	var cursor uint64
	totalCount := 0
	for {
		keys, cursor, err = sourceClient.HScan(context.Background(), sourceKey, cursor, "*", int64(batchSize)).Result()
		if err != nil {
			log.Errorf("Error scanning hash values, key:%s, error:%s", sourceKey, err.Error())
			return
		}

		pipeline := targetClient.Pipeline()
		for i := 0; i < len(keys); i += 2 {
			pipeline.HSet(context.Background(), targetKey, keys[i], keys[i+1])
			totalCount++
		}
		_, err = pipeline.Exec(context.Background())
		if err != nil {
			log.Errorf("Error setting hash values in target Redis, key:%s, error:%s", targetKey, err.Error())
			return
		}

		if cursor == 0 {
			break
		}
	}

	log.Infof("set target key:%s, totalCount:%d ok", targetKey, totalCount)
	return
}

func syncSSet(sourceClient *redis.Client, targetClient *redis.Client, sourceKey, targetKey string) (err error) {
	var keys []string
	var cursor uint64
	totalCount := 0
	for {
		keys, cursor, err = sourceClient.SScan(context.Background(), sourceKey, cursor, "", 0).Result()
		if err != nil {
			log.Errorf("Error scanning source set, key:%s, error:%s", sourceKey, err.Error())
			return
		}

		for _, key := range keys {
			_, err = targetClient.SAdd(context.Background(), targetKey, key).Result()
			if err != nil {
				log.Errorf("Error inserting member into destination set, key:%s, error:%s", targetKey, err.Error())
				return
			}
			totalCount++
		}

		if cursor == 0 {
			break
		}
	}

	log.Infof("set target key:%s, totalCount:%d ok", targetKey, totalCount)
	return
}

func syncSSetToCluster(sourceClient *redis.Client, targetClient *redis.ClusterClient, sourceKey, targetKey string) (err error) {
	var keys []string
	var cursor uint64
	totalCount := 0
	for {
		keys, cursor, err = sourceClient.SScan(context.Background(), sourceKey, cursor, "", 0).Result()
		if err != nil {
			log.Errorf("Error scanning source set, key:%s, error:%s", sourceKey, err.Error())
			return
		}

		for _, key := range keys {
			_, err = targetClient.SAdd(context.Background(), targetKey, key).Result()
			if err != nil {
				log.Errorf("Error inserting member into destination set, key:%s, error:%s", targetKey, err.Error())
				return
			}
			totalCount++
		}

		if cursor == 0 {
			break
		}
	}

	log.Infof("set target key:%s, totalCount:%d ok", targetKey, totalCount)
	return
}

func syncZSet(sourceClient *redis.Client, targetClient *redis.Client, sourceKey, targetKey string) (err error) {
	var keys []string
	var cursor uint64
	totalCount := 0
	for {
		keys, cursor, err = sourceClient.ZScan(context.Background(), sourceKey, cursor, "", 0).Result()
		if err != nil {
			log.Errorf("Error scanning source zset, key:%s, error:%s", sourceKey, err)
			return
		}

		for _, key := range keys {
			_, err = targetClient.ZAdd(context.Background(), targetKey, &redis.Z{Score: 0, Member: key}).Result()
			if err != nil {
				log.Errorf("Error inserting member into destination zset, key:%s, error:%s", key, err)
				return
			}

			totalCount++
		}

		if cursor == 0 {
			break
		}
	}

	log.Infof("set target key:%s, totalCount:%d ok", targetKey, totalCount)
	return
}

func syncZSetToCluster(sourceClient *redis.Client, targetClient *redis.ClusterClient, sourceKey, targetKey string) (err error) {
	var keys []string
	var cursor uint64
	totalCount := 0
	for {
		keys, cursor, err = sourceClient.ZScan(context.Background(), sourceKey, cursor, "", 0).Result()
		if err != nil {
			log.Errorf("Error scanning source zset, key:%s, error:%s", sourceKey, err)
			return
		}

		for _, key := range keys {
			_, err = targetClient.ZAdd(context.Background(), targetKey, &redis.Z{Score: 0, Member: key}).Result()
			if err != nil {
				log.Errorf("Error inserting member into destination zset, key:%s, error:%s", key, err)
				return
			}
			totalCount++
		}

		if cursor == 0 {
			break
		}
	}
	log.Infof("set target key:%s, totalCount:%d ok", targetKey, totalCount)
	return
}
