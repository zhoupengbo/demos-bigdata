package com.zpb.demos;

import com.zpb.utils.MyBase;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FirstKeyValueMatchingQualifiersFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * 首次匹配列过滤器：通过设置一组需要匹配的列，只要匹配到任意一个列就会停止这一行的扫描操作进行下一行的扫描。
 * 只要匹配到任意一个列就会停止这一行的扫描，继续扫描下一行（针对scan）。假设存储结构上column_b位于column_a之后。
 * 扫描过程中会首先匹配到column_a，此时不会继续匹配下一个列，而是直接开始下一行的扫描。返回结果包括匹配到的列之前的所有列值（包含匹配列）。
 */
public class FirstKeyValueMatchingQualifiersFilterDemo {

    private static boolean isok = false;
    private static String tableName = "test";
    private static String[] cfs = new String[]{"f1","f2"};
    private static String[] data = new String[]{"row-1:f1:p:v1", "row-1:f1:pre:v2", "row-2:f2:pref:v3", "row-2:f2:prefix:v4"};
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
        HashSet<byte []> qualifiers = new HashSet<>();
        qualifiers.add(Bytes.toBytes("p"));  // 匹配到p不会继续扫描row-1行的pre列。
        FirstKeyValueMatchingQualifiersFilter fkvmqf = new FirstKeyValueMatchingQualifiersFilter(qualifiers); // [row-1:f1:p, row-2:f2:pref, row-2:f2:prefix]
        scan.setFilter(fkvmqf);
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
