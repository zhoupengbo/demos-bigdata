package com.zpb.demos;

import com.zpb.utils.MyBase;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * 用于过滤列族（通常在 Scan 过程中通过设定某些列族来实现该功能，而不是直接使用该过滤器）。
 */
public class ValueFilterDemo {
    private static boolean isok = false;
    private static String tableName = "test";
    private static String[] cfs = new String[]{"f1","f2"};
    private static String[] data = new String[]{
            "row-1:f1:c1:abcdefg", "row-2:f1:c2:abc", "row-3:f2:c3:abc123456", "row-4:f2:c4:1234abc567"
    };
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
//        ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("abc"))); // [row-2:f1:c2:abc]
//        ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.NOT_EQUAL, new BinaryComparator(Bytes.toBytes("abc"))); // [row-1:f1:c1:abcdefg, row-3:f2:c3:abc123456, row-4:f2:c4:1234abc567]
//        ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.GREATER, new BinaryComparator(Bytes.toBytes("abc"))); // [row-1:f1:c1:abcdefg, row-3:f2:c3:abc123456]
//        ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes("abc1"))); // [row-1:f1:c1:abcdefg, row-3:f2:c3:abc123456]
//        ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.LESS, new BinaryComparator(Bytes.toBytes("abc"))); // [row-4:f2:c4:1234abc567]
//        ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("abc"))); // [row-2:f1:c2:abc, row-4:f2:c4:1234abc567]

        // BinaryPrefixComparator
//        ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.toBytes("123"))); // [row-4:f2:c4:1234abc567]
//        ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.NOT_EQUAL, new BinaryPrefixComparator(Bytes.toBytes("ab"))); // [row-4:f2:c4:1234abc567]
//        ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.GREATER, new BinaryPrefixComparator(Bytes.toBytes("ab"))); // [] 只比较prefix长度的字节
//        ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryPrefixComparator(Bytes.toBytes("ab"))); // [row-1:f1:c1:abcdefg, row-2:f1:c2:abc, row-3:f2:c3:abc123456]
//        ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.LESS, new BinaryPrefixComparator(Bytes.toBytes("abc"))); // [row-4:f2:c4:1234abc567]
//        ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryPrefixComparator(Bytes.toBytes("abc"))); // [row-1:f1:c1:abcdefg, row-2:f1:c2:abc, row-3:f2:c3:abc123456, row-4:f2:c4:1234abc567]

        // SubstringComparator
//        ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("123")); // [row-3:f2:c3:abc123456, row-4:f2:c4:1234abc567]
//        ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.NOT_EQUAL, new SubstringComparator("def")); // [row-2:f1:c2:abc, row-3:f2:c3:abc123456, row-4:f2:c4:1234abc567]

        // RegexStringComparator
//        ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.NOT_EQUAL, new RegexStringComparator("4[a-z]")); // [row-1:f1:c1:abcdefg, row-2:f1:c2:abc, row-3:f2:c3:abc123456]
//        ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("4[a-z]")); // [row-4:f2:c4:1234abc567]
//        ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("abc")); // [row-1:f1:c1:abcdefg, row-2:f1:c2:abc, row-3:f2:c3:abc123456, row-4:f2:c4:1234abc567]

        // NullComparator
//        ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new NullComparator()); // []
        ValueFilter valueFilter = new ValueFilter(CompareFilter.CompareOp.NOT_EQUAL, new NullComparator()); // [row-1:f1:c1:abcdefg, row-2:f1:c2:abc, row-3:f2:c3:abc123456, row-4:f2:c4:1234abc567]


        scan.setFilter(valueFilter);
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
                key = Bytes.toString(rowkey) + ":" + Bytes.toString(family) + ":" + Bytes.toString(column) + ":" + Bytes.toString(value);
                keys.add(key);
            }
        }
        System.out.println(keys);
        scanner.close();
        table.close();
        connection.close();
    }
}
