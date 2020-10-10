package org.example.bulk

import java.time.LocalDate

import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.example.constant.Constants
import org.example.utils.SuperUtil

/**
 * 使用BulkLoad写入数据
 */
object HiveSparkBulkToHBase {

  def main(args: Array[String]) {

    val rowKeyField = args(0) // 指定rowkey列
    val hBaseTable = args(1) // 指定HBase Table
    val sql = args(2) // 传入SQL表达式

    // 临时目录
    val nowdate = LocalDate.now().toString.replaceAll("-", "_")
    val tmpHFilePath = "/tmp/bulkload/" + hBaseTable + "_" + nowdate

    // linux 下不生效，仅适用于windows调试
    //    System.setProperty("user.name", Constants.proxyUser)
    //    System.setProperty("HADOOP_USER_NAME", Constants.proxyUser)

    val spark = SparkSession.builder()
      .appName("HiveSparkSqlBulkToHbase-" + hBaseTable)
      //      .master("local[1]")
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext
    val hiveContext = spark.sqlContext

    //从hive表读取数据
    val datahiveDF = hiveContext.sql(sql)
    //获取表结构字段
    var fields = datahiveDF.columns
    // 类型转换
    import org.apache.spark.sql.functions._
    val cols = fields.map(f => col(f).cast(StringType))
    val df = datahiveDF.select(cols: _*)
    //去掉rowKey字段
    fields = fields.dropWhile(_ == rowKeyField)
    //将DataFrame转换bulkload需要的RDD格式---通用
    val rddnew = df.rdd.map(row => {
      val orowKey = row.getAs[String](rowKeyField)
      val rowKey = SuperUtil.getRowkey(orowKey, Constants.hashNums) // 拿到hash之后的rowkey
      fields.map(field => {
        val fieldValue = row.getAs[String](field)
        (Bytes.toBytes(rowKey), Bytes.toBytes(Constants.cf), Bytes.toBytes(field), Bytes.toBytes(fieldValue))
      })
    })

    HBaseRDDFunctions.rddToHBaseRDD(rddnew).hbaseBulkLoad(sc, hBaseTable, tmpHFilePath, SuperUtil.bulkTransfer)
    sc.stop()
    spark.stop()
  }
}
