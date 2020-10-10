package org.example.read

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession
import org.example.constant.Constants

object SparkReadHBaseDemo {

  val hbaseTable = "test"

  //   主函数
  def main(args: Array[String]) {

    // linux 下不生效，仅适用于windows调试
    System.setProperty("user.name", Constants.proxyUser)
    System.setProperty("HADOOP_USER_NAME", Constants.proxyUser)

    val spark = SparkSession.builder()
      .appName("SparkReadHBaseDemo-" + hbaseTable)
      .master("local[1]")
      .getOrCreate()
    val sc = spark.sparkContext

    // 获取HbaseRDD
    val hbaseRDD = sc.newAPIHadoopRDD(getHbaseConf(), classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    hbaseRDD.map(_._2).map(getRes(_)).foreach(println(_))
  }

  def getRes(result: org.apache.hadoop.hbase.client.Result): String = {
    val rowkey = Bytes.toString(result.getRow())
    val name = Bytes.toString(result.getValue("f".getBytes, "name".getBytes))
    val age = Bytes.toString(result.getValue("f".getBytes, "age".getBytes))
    rowkey + "--" + name + "--" + age
  }

  // 构造 Hbase 配置信息
  def getHbaseConf(): Configuration = {
    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", Constants.zkQuorum)
    conf.set("zookeeper.znode.parent", Constants.zkParent)
    conf.set("hbase.zookeeper.property.clientPort", Constants.zkPort)
    // 设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, hbaseTable)
    conf.set(TableInputFormat.SCAN, getScanStr())
    conf
  }

  // 获取扫描器
  def getScanStr(): String = {
    val scan = new Scan()
    // scan.set.....各种过滤
    TableMapReduceUtil.convertScanToString(scan)
  }
}
