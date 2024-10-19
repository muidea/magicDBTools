# magicDBTools
mysql,mongo,redis数据库工具

## mysqlClone
mysql数据库同步工具，支持
1.对整个数据库实例进行数据同步
2.对指定数据库进行数据同步
3.支持排除数据库，支持排除数据表
4.支持同步数据表时指定过滤条件
```
Usage of ./mysqlClone:
  -batchSize int
        batch submit size (default 2000)
  -cloneInstance
        clone all databases in instance
  -concurrency int
        concurrency number for clone  (default 10)
  -excludeTables string
        exclude tables
  -excludes string
        exclude databases
  -filterConfig string
        filter config file
  -skipAutoIncrement
        skip auto increment
  -sourceDSN string
        source database info (default "root:rootkit@127.0.0.1:3306/srcDB")
  -targetDSN string
        target database info (default "root:rootkit@127.0.0.1:3306/dstDB")
  -truncateTable
        truncate table data before clone (default true)
```
过滤配置文件格式示例
```
{
    "aa_db":{
        "aa_table": "`provider`='x1001'",
        "bb_table": "`group`='dt' and id not in ('d42198488a','d9d1f11ece')"
    },
    "bb_db":{
        "task": "`id`=-1",
        "master": "`service_id`='x1001'"
    }
}
```


## mysqlExecute
mysqlExecute支持执行指定的sql语句
```
Usage of ./mysqlExecute:
  -configFile string
        config file (default "./config.json")
  -databaseDSN string
        database dsn (default "root:rootkit@127.0.0.1:3306/test_db")
```
config.json配置文件格式示例
```
{
    "sql":[
        "update `aa_db`.`aa_table` set `lessee`='_abc' where `lessee`='dt'",
        "update `bb_db`.`bb_table` set `group`='_abc' where `group`='dt'"
        ]
}
```


## mongoClone
mongoClone mongo数据库同步工具，支持
1.按指定collection进行数据同步
2.支持通过数据库实例里所有collection

可以通过DSN执行需要同步的collection
```
Usage of ./mongoClone:
  -batchSize int
        batch submit size (default 2000)
  -cloneInstance
        clone all schema in database
  -dropCollection
        empty collection data before copying
  -excludes string
        exclude databases
  -sourceDSN string
        source database info (default "root:mongoSupos@127.0.0.1:27018/")
  -targetDSN string
        target database info (default "root:rootkit@127.0.0.1:27017/")
```

## redisClone
redisClone redis数据同步工具，支持
1、按指定的key进行数据同步，实现将源key同步至目标key
```
Usage of ./redisClone:
  -configFile string
        config file (default "./config.json")
  -sourceDSN string
        source redis info (default "root:rootkit@127.0.0.1:6379/")
  -targetDSN string
        target redis info (default "root:redisPassword@192.168.236.163:6379,192.168.236.163:6380,192.168.236.164:6379,192.168.236.164:6380,192.168.236.165:6379,192.168.236.165:6380/")
```

config.json文件格式示例
```
{
    "keys": {
        "srcKey": "targetKey"
    }
}
```