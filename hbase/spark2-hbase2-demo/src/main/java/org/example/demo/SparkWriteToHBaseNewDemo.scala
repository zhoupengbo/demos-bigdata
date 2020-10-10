package org.example.demo

import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.example.constant.Constants

/**
 * 使用saveAsNewAPIHadoopDataset写入数据
 * 直接入库建议使用这种写入方式
 */
object SparkWriteToHBaseNewDemo {
  def main(args: Array[String]): Unit = {

    val tableName = "test"

    // linux 下不生效，仅适用于windows调试
    System.setProperty("user.name", Constants.proxyUser)
    System.setProperty("HADOOP_USER_NAME", Constants.proxyUser)

    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkWriteToHbase-"+tableName)
      .getOrCreate()
    val sc = spark.sparkContext

    sc.hadoopConfiguration.set("hbase.zookeeper.quorum", Constants.zkQuorum)
    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", Constants.zkPort)
    sc.hadoopConfiguration.set("zookeeper.znode.parent", Constants.zkParent)
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val job = Job.getInstance(sc.hadoopConfiguration)

    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    val indataRDD = sc.makeRDD(Array("111,jack111,1", "222,Lily222,2", "333,mike333,3"))
    val rdd: RDD[(ImmutableBytesWritable, Put)] = indataRDD.map(_.split(',')).map { arr => {
      val put = new Put(Bytes.toBytes(arr(0)))
      put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("name"), Bytes.toBytes(arr(1)))
      put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("age"), Bytes.toBytes(arr(2)))
      (new ImmutableBytesWritable, put)
    }
    }

    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration())
    sc.stop()
    spark.stop()
  }
}