package org.example.constant

object Constants {

  // HBase Table
  val splitFormat = "%04x"
  val hashNums = 4096
  val cf = "f"

  // User Configuration
  val proxyUser = "hdfs"
  
  // Cluster Configuration
  val zkQuorum = "testhdp3.zpb.com,testhdp2.zpb.com,testhdp1.zpb.com"
  val zkParent = "/hbase-unsecure"
  val rootDir = "hdfs://mytestcluster/apps/hbase/data"
  val zkPort = "2181"
  val maxHfilesPerFamily = 256

  val fsDefaultFS = "hdfs://mytestcluster"
  val dfsNameservices = "mytestcluster"
  val dfsHaNamenodes = "nn1,nn2"
  val dfsNamenodeNN1 = "testhdp1.zpb.com:8020"
  val dfsNamenodeNN2 = "testhdp2.zpb.com:8020"

}
