package org.example.read

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableMapReduceUtil, TableSnapshotInputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession
import org.example.constant.Constants

object SparkReadHBaseSnapshotDemo {

  val hbaseTable = "test"

  def main(args: Array[String]) {

    val snapshotName = "test-snapshot"
    val tmpHdfsPath = "/user/tmp"

    // linux 下不生效，仅适用于windows调试
    System.setProperty("user.name", Constants.proxyUser)
    System.setProperty("HADOOP_USER_NAME", Constants.proxyUser)

    // 设置spark访问入口
    val spark = SparkSession.builder()
      .appName("SparkReadHBaseDemo-" + hbaseTable)
      .master("local[1]")
      .getOrCreate()
    val sc = spark.sparkContext
    // 获取HbaseRDD
    val job = Job.getInstance(getHbaseConf())
    TableSnapshotInputFormat.setInput(job, snapshotName, new Path(tmpHdfsPath))

    val hbaseRDD = sc.newAPIHadoopRDD(job.getConfiguration, classOf[TableSnapshotInputFormat],
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
    conf.set("hbase.rootdir", Constants.rootDir)
    // 设置查询的表名
    conf.set(TableInputFormat.INPUT_TABLE, hbaseTable)
    conf.set(TableInputFormat.SCAN, getScanStr())
    // 设置hdfs
    conf.setStrings("fs.defaultFS", Constants.fsDefaultFS)
    conf.setStrings("dfs.nameservices", Constants.dfsNameservices)
    conf.set("dfs.ha.namenodes." + Constants.dfsNameservices, Constants.dfsHaNamenodes)
    conf.set("dfs.namenode.rpc-address." + Constants.dfsNameservices + "." + Constants.dfsHaNamenodes.split(",")(0), Constants.dfsNamenodeNN1)
    conf.set("dfs.namenode.rpc-address." + Constants.dfsNameservices + "." + Constants.dfsHaNamenodes.split(",")(1), Constants.dfsNamenodeNN2)
    conf.set("dfs.client.failover.proxy.provider." + Constants.dfsNameservices, "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")

    conf
  }

  // 获取扫描器
  def getScanStr(): String = {
    val scan = new Scan()
    // scan.set....  各种过滤
    TableMapReduceUtil.convertScanToString(scan)
  }
}
