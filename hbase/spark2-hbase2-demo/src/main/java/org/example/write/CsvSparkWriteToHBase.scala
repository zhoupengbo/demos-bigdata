package org.example.write

import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.example.constant.Constants
import org.example.utils.SuperUtil

/**
 * 使用saveAsNewAPIHadoopDataset写入数据
 * 直接入库建议使用这种写入方式
 */
object CsvSparkWriteToHBase {
  def main(args: Array[String]): Unit = {

    val rowKeyField = args(0) // 指定rowkey列
    val hBaseTable = args(1) // 指定HBase Table
    val path = args(2) // 指定csv文件路径
    val seprator = args(3) // 指定分隔符

    val spark = SparkSession.builder()
      //      .master("local[1]")
      .appName("SparkWriteToHbase-"+hBaseTable)
      .getOrCreate()

    val sc = spark.sparkContext

    sc.hadoopConfiguration.set("hbase.zookeeper.quorum", Constants.zkQuorum)
    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", Constants.zkPort)
    sc.hadoopConfiguration.set("zookeeper.znode.parent", Constants.zkParent)
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, hBaseTable)

    val job = Job.getInstance(sc.hadoopConfiguration)

    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    val df = spark.read.format("csv")
      .option("header", "true") // 是否有表头
      .option("inferSchema", "false")
      .option("delimiter", seprator)
      .load(path)

    //获取表结构字段
    var fields = df.columns

    // 类型转换
    import org.apache.spark.sql.functions._
    val cols = fields.map(f => col(f).cast(StringType))
    val df2 = df.select(cols: _*)

    //去掉rowKey字段
    fields = fields.dropWhile(_ == rowKeyField)
    //将DataFrame转换bulkload需要的RDD格式---通用
    val rdd: RDD[(ImmutableBytesWritable, Put)] = df2.rdd.flatMap(row => {
      val orowKey = row.getAs[String](rowKeyField)
      val rowKey = SuperUtil.getRowkey(orowKey, Constants.hashNums) // 拿到hash之后的rowkey
      fields.map(field => {
        val fieldValue = row.getAs[String](field)
        val put = new Put(Bytes.toBytes(rowKey))
        put.addColumn(Bytes.toBytes(Constants.cf), Bytes.toBytes(field), Bytes.toBytes(fieldValue))
        (new ImmutableBytesWritable, put)
      })
    })

    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration())
    sc.stop()
    spark.stop()
  }
}