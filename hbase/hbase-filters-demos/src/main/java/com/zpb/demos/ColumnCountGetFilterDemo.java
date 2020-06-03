package com.zpb.demos;

import com.zpb.utils.MyBase;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnCountGetFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.NullComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * 简单过滤器，仅返回行的前N列. 此过滤器被编写为在Get中测试过滤器，并
 * 且一旦获得其列配额， filterAllRemaining()返回true.
 * 这使得此过滤器不适合作为"扫描"过滤器.
 */
public class ColumnCountGetFilterDemo {
    private static boolean isok = false;
    private static String tableName = "test";
    private static String[] cfs = new String[]{"f1","f2"};
    private static String[] data = new String[]{"row-1:f1:c1:v1", "row-1:f1:c2:v2", "row-1:f2:c3:v3", "row-1:f2:c4:v4"};
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
//        ColumnCountGetFilter columnCountGetFilter = new ColumnCountGetFilter(1); // [row-1:f1:c1]
        ColumnCountGetFilter columnCountGetFilter = new ColumnCountGetFilter(2); // [row-1:f1:c1, row-1:f1:c2]
        scan.setFilter(columnCountGetFilter);
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
