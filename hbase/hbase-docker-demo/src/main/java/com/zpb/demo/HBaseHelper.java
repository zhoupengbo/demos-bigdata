package com.zpb.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;

public class HBaseHelper {

    public static String ZK_QUORUM = "docker-hbase";
    public static String ZK_ZNODE = "/hbase";
    public static String ZK_PORT = "2181";
    public static String SUPER_USER = "hbase";

    // 配置连接信息
    public Configuration getConfiguration() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", ZK_QUORUM);
        conf.set("zookeeper.znode.parent", ZK_ZNODE);
        conf.set("hbase.zookeeper.property.clientPort", ZK_PORT);
        return conf;
    }

    // 指定用户
    public Configuration setUser(Configuration conf, String user) {
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation romoteUser = UserGroupInformation.createRemoteUser(user);
        UserGroupInformation.setLoginUser(romoteUser);
        return conf;
    }

    // 创建连接
    public Connection createConnection() throws IOException {
        Configuration configuration = setUser(getConfiguration(), SUPER_USER);
        Connection connection = ConnectionFactory.createConnection(configuration);
        return connection;
    }

    // 建表 args-->指定列族
    public void createTable(Connection connection, String tableName, String... args) throws IOException {
        Admin admin = connection.getAdmin();
        TableDescriptorBuilder tdesBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
        ArrayList<ColumnFamilyDescriptor> cflist = new ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            ColumnFamilyDescriptor cf = ColumnFamilyDescriptorBuilder.of(args[i]);
            cflist.add(cf);
        }
        tdesBuilder.setColumnFamilies(cflist);
        TableDescriptor table = tdesBuilder.build();
        if (admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println("Table is exists.");
        } else {
            System.out.println("Creating table. ");
            admin.createTable(table);
        }
        admin.close();
    }

    // 删表
    public void deleteTable(Connection connection, String tableName) throws IOException {
        Admin admin = connection.getAdmin();
        if (admin.tableExists(TableName.valueOf(tableName))) {
            System.out.print("Delete table. ");
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
        }else {
            System.out.println("Table is not exists.");
        }
        admin.close();
    }

    /*
     *插入数据
     * args --> "r1:f1:c1:v1","r2:f2:c2:v2","r3:f1:c2:v3"
     */
    public void putRows(Connection connection, String tableName, String... args) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        ArrayList<Put> puts = new ArrayList<>();
        for (int i = 0; i < args.length; i++) {
            String[] row = args[i].split(":");
            Put put = new Put(Bytes.toBytes(row[0]));
            put.addColumn(Bytes.toBytes(row[1]), Bytes.toBytes(row[2]),
                    Bytes.toBytes(row[3]));
            puts.add(put);
        }
        table.put(puts);
        table.close();
    }

    public void getRows(Connection connection, String tableName, String rowkey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowkey));
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            byte[] rk = CellUtil.cloneRow(cell);
            byte[] family = CellUtil.cloneFamily(cell);
            byte[] column = CellUtil.cloneQualifier(cell);
            byte[] value = CellUtil.cloneValue(cell);
            String kv = Bytes.toString(rk) + ":" + Bytes.toString(family) + ":" + Bytes.toString(column) + ":" + Bytes.toString(value);
            System.out.println(kv);
        }
        table.close();
    }

    public static void main(String[] args) throws IOException {
        HBaseHelper myBase = new HBaseHelper();
        Connection connection = myBase.createConnection();
        myBase.deleteTable(connection,"test");
        myBase.createTable(connection,"test",new String[]{"f1","f2"});
        myBase.putRows(connection,"test",new String[]{"row-1:f1:c1:v1", "row-1:f2:c2:v2"});
        myBase.getRows(connection,"test","row-1");
    }
}
