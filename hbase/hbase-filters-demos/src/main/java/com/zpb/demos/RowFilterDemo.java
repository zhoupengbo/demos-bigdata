package com.zpb.demos;

import com.zpb.utils.MyBase;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * 该过滤器用于根据rowkey进行过滤. 它需要一个运算符（等于，更大，不等于等）以及键的行和列限定符部分的byte []比较器.
 * 可以使用WhileMatchFilter封装此过滤器，以添加更多控件.
 * 可以使用FilterList组合多个过滤器.
 * 如果需要扫描已知的行范围，请直接使用CellScanner startrow和endrow，而不要使用过滤器.
 */
public class RowFilterDemo {

    private static boolean isok = true;
    private static String tableName = "test";
    private static String[] cfs = new String[]{"f"};
    private static String[] data = new String[]{"row-ac:f:c1:v1", "row-ab:f:c2:v2", "row-bc:f:c3:v3", "row-abc:f:c4:v4"};

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

        // BinaryComparator
        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("row-ac"))); // [row-ac]
//        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.NOT_EQUAL, new BinaryComparator(Bytes.toBytes("row-ac"))); // [row-ab, row-abc, row-bc]
//        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.GREATER, new BinaryComparator(Bytes.toBytes("row-ac"))); // [row-bc]
//        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes("row-ac"))); // [row-ac, row-bc]
//        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.LESS, new BinaryComparator(Bytes.toBytes("row-ac"))); // [row-ab, row-abc]
//        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("row-ac"))); // [row-ab, row-abc, row-ac]

        // BinaryPrefixComparator
//        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.toBytes("row-a"))); // [row-ab, row-abc, row-ac]
//        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.NOT_EQUAL, new BinaryPrefixComparator(Bytes.toBytes("row-a"))); // [row-bc]
//        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.GREATER, new BinaryPrefixComparator(Bytes.toBytes("row-a"))); // [row-bc]
//        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryPrefixComparator(Bytes.toBytes("row-a"))); // [row-ab, row-abc, row-ac, row-bc]
//        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.LESS, new BinaryPrefixComparator(Bytes.toBytes("row-a"))); // []
//        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryPrefixComparator(Bytes.toBytes("row-a"))); // [row-ab, row-abc, row-ac]

        // SubstringComparator
//        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("ab")); // [row-ab, row-abc]
//        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.NOT_EQUAL, new SubstringComparator("ab")); // [row-ac, row-bc]

        // RegexStringComparator
//        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.NOT_EQUAL, new RegexStringComparator("abc")); // [row-ab, row-ac, row-bc]
//        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("abc")); // [row-abc]
//        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("a")); // [row-ab, row-abc, row-ac]

        // RowFilter
//        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new NullComparator()); // []
//        RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.NOT_EQUAL, new NullComparator()); // [row-ab, row-abc, row-ac, row-bc]

        scan.setFilter(rowFilter);
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();
        LinkedList<String> rowkeys = new LinkedList<>();
        while (iterator.hasNext()) {
            Result result = iterator.next();
            String rowkey = Bytes.toString(result.getRow());
            rowkeys.add(rowkey);
        }
        System.out.println(rowkeys);
        scanner.close();
        table.close();
        connection.close();
    }
}
