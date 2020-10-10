package org.example.demo

import java.time.LocalDate

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.example.bulk.HBaseRDDFunctions
import org.example.constant.Constants
import org.example.utils.SuperUtil

object CsvSparkBulkToHBaseDemo {

  def main(args: Array[String]): Unit = {

    val rowKeyField = "id" // 指定rowkey列
    val hBaseTable = "test" // 指定HBase Table
    val path = "./data.csv" // 指定csv文件路径
    val seprator = "," // 指定分隔符

    // 临时目录
    val nowdate = LocalDate.now().toString.replaceAll("-","_")
    val tmpHFilePath = "/tmp/bulkload/" + hBaseTable + "_" + nowdate

    // linux 下不生效，仅适用于windows调试
    System.setProperty("user.name", Constants.proxyUser)
    System.setProperty("HADOOP_USER_NAME", Constants.proxyUser)
    System.setProperty("hadoop.home.dir", "Z:\\software\\hadoop")

    val spark = SparkSession.builder()
      .appName("CsvSparkBulkToHbase-"+hBaseTable)
      .master("local[1]")
      .getOrCreate()
    val sc = spark.sparkContext

    val df = spark.read.format("csv")
      .option("header", "true") // 是否有表头
      .option("inferSchema", "true")
      .option("delimiter", seprator)
      .load(path)

    df.show

    //获取表结构字段
    var columns = df.columns

    // 类型转换
    import org.apache.spark.sql.functions._
    val cols = columns.map(f => col(f).cast(StringType))
    val df2 = df.select(cols: _*)

    //去掉rowKey字段
    columns = columns.dropWhile(_ == rowKeyField)
    //将DataFrame转换bulkload需要的RDD格式---通用
    val rddnew = df2.rdd.map(row => {
      val orowKey = row.getAs[String](rowKeyField)
      val rowKey = SuperUtil.getRowkey(orowKey,Constants.hashNums) // 拿到hash之后的rowkey
      columns.map(field => {
        val fieldValue = row.getAs[String](field)
        (Bytes.toBytes(rowKey), Bytes.toBytes(Constants.cf), Bytes.toBytes(field), Bytes.toBytes(fieldValue))
      })
    })

    HBaseRDDFunctions.rddToHBaseRDD(rddnew).hbaseBulkLoad(sc, hBaseTable, tmpHFilePath, SuperUtil.bulkTransfer)
    sc.stop()
    spark.stop()
  }
}
