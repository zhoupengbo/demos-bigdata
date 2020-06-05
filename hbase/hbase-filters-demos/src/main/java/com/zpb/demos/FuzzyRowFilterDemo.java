package com.zpb.demos;

import com.zpb.utils.MyBase;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FirstKeyValueMatchingQualifiersFilter;
import org.apache.hadoop.hbase.filter.FuzzyRowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.*;

/**
 * FuzzyRowFilter是模糊查询row（指定rowkey里面部分值可以不是前缀，​指定符合前缀的可以用PrefixFilter）的filter，能够在scan的时候快速向前扫描。
 * 举例：存到hbase里面的数据，很多时候rowkey是由多个部分组合而成，而各个部分又是固定长度。比如userId_actionId_year_month组成key，userId 是固定的4字节，actionId 是2字节，year 是4字节，month 是2字节。
 * 如果想查某个action类型比如99，并且在1月份的所有用户出来，
 * 那么我们需要获取的数据是rowkey为 “????_99_????_01” “?”代表任意值（一个字节长）。
 * 如果没有FuzzyRowFilter 的话，那么需要整表扫描过滤出来符合条件的数据，如果有FuzzyRowFilter 的话，在某些场景下会快很多了。
 * 它的原理是以上面的为例：符合????_99_????_01的rowkey ，起始是0000_99_0000_01 ，结束是1111_99_1111_01，
 * 根据这个条件就可以限制起始范围，另外在扫描的过程中可以从一个key跳到下一个key去，因为满足条件的key必须符合 ????_99_????_01，
 * 所以那么不符合这个条件的row不会进行匹配，直接跳过，seek到下一个符合条件的row去，这样就加快了查找速度。
 * 1的意思是该位置对应的值不用匹配，0表示该位置的值必须要匹配。
 */
public class FuzzyRowFilterDemo {
    private static boolean isok = false;
    private static String tableName = "test";
    private static String[] cfs = new String[]{"f1"};
    private static String[] data = new String[]{
            "test-10-abc-01:f1:p:v1", "deve-10-acb-01:f1:p:v2",
            "test-23-abc-01:f1:p:v3", "deve-23-acb-01:f1:p:v4"};
    public static void main(String[] args) throws IOException {

        MyBase myBase = new MyBase();
        Connection connection = myBase.createConnection();
        if (isok) {
            myBase.deleteTable(connection, tableName);
            myBase.createTable(connection, tableName, cfs);
            // 造数据
            myBase.putRows(connection, tableName, data);
        }
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        List<Pair<byte[], byte[]>> pairs = Arrays.asList(
                new Pair<byte[], byte[]>(
                        Bytes.toBytes("????-23-abc-??"),
                        new byte[]{1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1}),
                new Pair<byte[], byte[]>(
                        Bytes.toBytes("????-10-abc-??"),
                        new byte[]{1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1})
        );
        FuzzyRowFilter fuzzyRowFilter = new FuzzyRowFilter(pairs); // [test-10-abc-01:f1:p, test-23-abc-01:f1:p]
        scan.setFilter(fuzzyRowFilter);
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();
        LinkedList<String> keys = new LinkedList<>();
        while (iterator.hasNext()) {
            String key = "";
            Result result = iterator.next();
            for (Cell cell : result.rawCells()) {
                byte[] rowkey = CellUtil.cloneRow(cell);
                byte[] family = CellUtil.cloneFamily(cell);
                byte[] column = CellUtil.cloneQualifier(cell);
                byte[] value = CellUtil.cloneValue(cell);
                key = Bytes.toString(rowkey) + ":" + Bytes.toString(family) + ":" + Bytes.toString(column);
                keys.add(key);
            }
        }
        System.out.println(keys);
        scanner.close();
        table.close();
        connection.close();
    }
}
