package org.example.bulk

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.KeyValue.Type
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.example.constant.Constants

import scala.language.implicitConversions

class HBaseRDDFunctions[T](self: RDD[T]) {

  /**
   * 保存至本地集群HBase
   * @param sc
   * @param tableName
   * @param hbaseRDD
   * @param tmpHFilePath
   */
  private def saveToHBase(sc: SparkContext, tableName: String, hbaseRDD: RDD[(ImmutableBytesWritable, KeyValue)], tmpHFilePath: String): Unit = {

    var conn: Connection = null
    var realTable: Table = null

    //    方法一：从配置文件获取 HBase 配置
    //    val url = HBaseRDDFunctions.getClass.getClassLoader().getResource("hbase-site.xml")
    //    sc.hadoopConfiguration.addResource(url)

    // 方法二：代码中指定配置
    val hbaseConf = HBaseConfiguration.create(sc.hadoopConfiguration)
    hbaseConf.set("hbase.zookeeper.quorum", Constants.zkQuorum)
    hbaseConf.set("hbase.zookeeper.property.clientPort", Constants.zkPort)
    hbaseConf.set("zookeeper.znode.parent", Constants.zkParent)
    hbaseConf.setInt("hbase.mapreduce.bulkload.max.hfiles.perRegion.perFamily", Constants.maxHfilesPerFamily)
    val hbTableName = TableName.valueOf(tableName)

    val job = Job.getInstance(hbaseConf)

    val hdfsconf = new Configuration(sc.hadoopConfiguration)
    val fs = FileSystem.get(hdfsconf)
    fs.deleteOnExit(new Path(tmpHFilePath))

    try {
      conn = ConnectionFactory.createConnection(hbaseConf)
      job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
      job.setMapOutputValueClass(classOf[KeyValue])
      realTable = conn.getTable(hbTableName)
      HFileOutputFormat2.configureIncrementalLoad(job, realTable, conn.getRegionLocator(hbTableName))
      hbaseRDD.saveAsNewAPIHadoopFile(tmpHFilePath, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], job.getConfiguration)
      val loader = new LoadIncrementalHFiles(hbaseConf)
      val regionLocator = new HRegionLocator(hbTableName, classOf[ClusterConnection].cast(conn))
      loader.doBulkLoad(new Path(tmpHFilePath), conn.getAdmin, realTable, regionLocator)

    } finally {
      fs.deleteOnExit(new Path(tmpHFilePath))
      if (realTable != null) {
        realTable.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }

  /**
   * 保存至远程集群HBase，需修改hdfs配置为远程hdfs
   * @param sc
   * @param tableName
   * @param hbaseRDD
   * @param tmpHFilePath
   */
  private def saveToRemoteHBase(sc: SparkContext, tableName: String, hbaseRDD: RDD[(ImmutableBytesWritable, KeyValue)], tmpHFilePath: String): Unit = {
    //  remote hdfs 需配置
    sc.hadoopConfiguration.setStrings("fs.defaultFS", Constants.fsDefaultFS)
    sc.hadoopConfiguration.setStrings("dfs.nameservices", Constants.dfsNameservices)
    sc.hadoopConfiguration.set("dfs.ha.namenodes." + Constants.dfsNameservices, Constants.dfsHaNamenodes)
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address." + Constants.dfsNameservices + "." + Constants.dfsHaNamenodes.split(",")(0), Constants.dfsNamenodeNN1)
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address." + Constants.dfsNameservices + "." + Constants.dfsHaNamenodes.split(",")(1), Constants.dfsNamenodeNN2)
    sc.hadoopConfiguration.set("dfs.client.failover.proxy.provider." + Constants.dfsNameservices, "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")

    saveToHBase(sc, tableName, hbaseRDD, tmpHFilePath)
  }

  /**
   * bulkload 导入：涉及数据排序、生成HFile、文件加载
   * @param sc
   * @param tableName
   * @param tmpHFilePath
   * @param transfer
   * @param kvType
   */
  def hbaseBulkLoad(sc: SparkContext, tableName: String, tmpHFilePath: String,
                    transfer: T => List[(Array[Byte], Array[Byte], Array[Byte], Array[Byte])], kvType: Type = Type.Put): Unit = {

    implicit val rowKeyOrding = new Ordering[Array[Byte]] {
      override def compare(left: Array[Byte], right: Array[Byte]) = {
        Bytes.compareTo(left, right)
      }
    }

    val ts = System.currentTimeMillis()
    // rowkey排序
    val hbaseRDD = self.flatMap(transfer).map(rd => {
      val rowKey = rd._1
      val columnFamily = rd._2
      val qualifier = rd._3
      val value = rd._4
      val key = new Array[Byte](rowKey.length + columnFamily.length + qualifier.length)
      System.arraycopy(rowKey, 0, key, 0, rowKey.length)
      System.arraycopy(columnFamily, 0, key, rowKey.length, columnFamily.length)
      System.arraycopy(qualifier, 0, key, rowKey.length + columnFamily.length, qualifier.length)
      (key, (rowKey.length, columnFamily.length, value))
    }).sortByKey().map(rd => {
      val rowKey = new Array[Byte](rd._2._1)
      val columnFamily = new Array[Byte](rd._2._2)
      val qualifier = new Array[Byte](rd._1.length - rd._2._1 - rd._2._2)
      System.arraycopy(rd._1, 0, rowKey, 0, rd._2._1)
      System.arraycopy(rd._1, rd._2._1, columnFamily, 0, rd._2._2)
      System.arraycopy(rd._1, rd._2._1 + rd._2._2, qualifier, 0, rd._1.length - rd._2._1 - rd._2._2)
      val kv = new KeyValue(rowKey, columnFamily, qualifier, ts, kvType, rd._2._3)
      (new ImmutableBytesWritable(rowKey), kv)
    })
    // 本地保存
//    saveToHBase(sc, tableName, hbaseRDD, tmpHFilePath)
    // 远端保存
    saveToRemoteHBase(sc, tableName, hbaseRDD, tmpHFilePath)
  }
}

object HBaseRDDFunctions {

  implicit def rddToHBaseRDD[T](rdd: RDD[T]): HBaseRDDFunctions[T] = {

    new HBaseRDDFunctions(rdd)
  }

}
