package org.example.bulk

import java.time.LocalDate

import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.example.constant.Constants
import org.example.utils.SuperUtil

object CsvSparkBulkToHBase {

  def main(args: Array[String]): Unit = {

    val rowKeyField = args(0) // 指定rowkey列
    val hBaseTable = args(1) // 指定HBase Table
    val path = args(2) // 指定csv文件路径
    val seprator = args(3) // 指定分隔符

    // 临时目录
    val nowdate = LocalDate.now().toString.replaceAll("-","_")
    val tmpHFilePath = "/tmp/bulkload/" + hBaseTable + "_" + nowdate

    val spark = SparkSession.builder()
      .appName("CsvSparkBulkToHbase-"+hBaseTable)
//      .master("local[1]")
      .getOrCreate()
    val sc = spark.sparkContext

    val df = spark.read.format("csv")
      .option("header", "true") // 是否有表头
      .option("inferSchema", "false")
      .option("delimiter", seprator)
      .load(path)

    //获取表结构字段
    var columns = df.columns

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
