package com.zpb.demos;

import com.zpb.utils.MyBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

public class ColumnPrefixFilterDemo {

    private static String tableName = "test";
    public static void main(String[] args) throws IOException {
        MyBase myBase = new MyBase();
        Connection connection = myBase.createConnection();
        myBase.deleteTable(connection,tableName);
        myBase.createTable(connection,tableName,"f");
    }
}
