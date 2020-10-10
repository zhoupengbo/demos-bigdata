##### BulkLoad 导入示例

###### 1. 语法说明：
```
$ spark-submit --conf spark.jars="" --conf spark.default.parallelism=4 \
             --master yarn-client \
             --class org.example.bulk.HiveSparkBulkToHBase \
             --name SparkSqlBulkToHbase \
             spark2-hbase2-bulkload.jar $1 $2 $3
```

###### 2. 参数说明：
```
$1: '指定rowkey字段'
$2: 'Hbase表名'
$3: 'hive sql (双引号括起来)'
```

###### 3. 举例如下：
```
$ spark-submit --conf spark.jars="" --conf spark.default.parallelism=4 \
             --master yarn-client \
             --class org.example.bulk.HiveSparkBulkToHBase \
             --name SparkSqlBulkToHbase \
             spark2-hbase2-bulkload.jar id test "select rowkey id,age,name from test.test_zpb"
```