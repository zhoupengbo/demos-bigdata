package com.zpb.demos;

import com.zpb.utils.MyBase;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * 基于列范围(不是行范围)过滤数据
 * 可用于获得一个范围的列，例如，如果你的一行中有百万个列，但是你只希望查看列名从bbbb到dddd的范围
 * 该方法从 HBase 0.92 版本开始引入
 * 一个列名是可以出现在多个列族中的，该过滤器将返回所有列族中匹配的列
 *
 * minColumn - 列范围的最小值，如果为空，则没有下限
 * minColumnInclusive - 列范围是否包含minColumn
 * maxColumn - 列范围最大值，如果为空，则没有上限
 * maxColumnInclusive - 列范围是否包含maxColumn
 *
 * scan 'test',{FILTER=>"ColumnRangeFilter('a',true,'c',false)"}
 */
public class ColumnRangeFilterDemo {
    private static boolean isok = false;
    private static String tableName = "test";
    private static String[] cfs = new String[]{"f1","f2"};
    private static String[] data = new String[]{"row-1:f1:p:v1", "row-1:f1:pre:v2", "row-1:f2:pref:v3", "row-1:f2:prefix:v4"};
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
//        ColumnRangeFilter columnRangeFilter = new ColumnRangeFilter(Bytes.toBytes("pre"),true,Bytes.toBytes("prefix"),false); // [row-1:f1:pre, row-1:f2:pref]
//        ColumnRangeFilter columnRangeFilter = new ColumnRangeFilter(Bytes.toBytes("pre"),true,Bytes.toBytes("prefix"),true); // [row-1:f1:pre, row-1:f2:pref, row-1:f2:prefix]
        ColumnRangeFilter columnRangeFilter = new ColumnRangeFilter(null,true,Bytes.toBytes("pref"),false); // [row-1:f1:p, row-1:f1:pre]
        scan.setFilter(columnRangeFilter);
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
