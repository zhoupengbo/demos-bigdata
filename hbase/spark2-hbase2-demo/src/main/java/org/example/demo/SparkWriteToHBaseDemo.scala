package org.example.demo

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession
import org.example.constant.Constants

/**
 * 使用saveAsHadoopDataset写入数据
 */
object SparkWriteToHBaseDemo {

  def main(args: Array[String]): Unit = {

    // HBase Table
    val tableName = "test"

    // linux 下不生效，仅适用于windows调试
    System.setProperty("user.name", Constants.proxyUser)
    System.setProperty("HADOOP_USER_NAME", Constants.proxyUser)

    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkWriteToHbase-"+tableName)
      .getOrCreate()
    val sc = spark.sparkContext

    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum",Constants.zkQuorum)
    hbaseConf.set("hbase.zookeeper.property.clientPort", Constants.zkPort)
    hbaseConf.set("zookeeper.znode.parent",Constants.zkParent)

    //初始化jobconf，TableOutputFormat必须是org.apache.hadoop.hbase.mapred包下的！
    val jobConf = new JobConf(hbaseConf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val indataRDD = sc.makeRDD(Array("1,jack,115","2,Lily,116","3,mike,117"))

    val rdd = indataRDD.map(_.split(',')).map{arr=>{
      val put = new Put(Bytes.toBytes(arr(0)))
      put.addColumn(Bytes.toBytes("f"),Bytes.toBytes("name"),Bytes.toBytes(arr(1)))
      put.addColumn(Bytes.toBytes("f"),Bytes.toBytes("age"),Bytes.toBytes(arr(2)))
      (new ImmutableBytesWritable, put)
    }}

    rdd.saveAsHadoopDataset(jobConf)

    sc.stop()
    spark.stop()
  }
}
