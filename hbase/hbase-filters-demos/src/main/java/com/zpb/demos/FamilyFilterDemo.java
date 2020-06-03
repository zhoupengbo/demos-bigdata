package com.zpb.demos;

import com.zpb.utils.MyBase;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * 用于过滤列族（通常在 Scan 过程中通过设定某些列族来实现该功能，而不是直接使用该过滤器）。
 * 该过滤器用于基于列族进行过滤. 键的列族部分需要一个运算符（等于，更大，不等于等）和一个字节[]比较器.
 * 该过滤器可以使用WhileMatchFilter和SkipFilter进行包装，以添加更多控件.
 * 可以使用FilterList组合多个过滤器.
 * 如果要查找已知的列族，请直接使用Get.addFamily(byte[])而不是过滤器.
 */
public class FamilyFilterDemo {

    private static boolean isok = false;
    private static String tableName = "test";
    private static String[] cfs = new String[]{"f1","f2"};
    private static String[] data = new String[]{"row-1:f1:c1:v1", "row-2:f1:c2:v2", "row-3:f2:c3:v3", "row-4:f2:c4:v4"};
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
//        FamilyFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("f1"))); // [row-1, row-2]
//        FamilyFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.NOT_EQUAL, new BinaryComparator(Bytes.toBytes("f1"))); // [row-3, row-4]
//        FamilyFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.GREATER, new BinaryComparator(Bytes.toBytes("f1"))); // [row-3, row-4]
//        FamilyFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes("f1"))); // [row-1, row-2, row-3, row-4]
//        FamilyFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.LESS, new BinaryComparator(Bytes.toBytes("f2"))); // [row-1, row-2]
//        FamilyFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("f1"))); // [row-1, row-2]

        // BinaryPrefixComparator
//        FamilyFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.toBytes("f"))); // [row-1, row-2, row-3, row-4]
//        FamilyFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.NOT_EQUAL, new BinaryPrefixComparator(Bytes.toBytes("f"))); // []
//        FamilyFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.GREATER, new BinaryPrefixComparator(Bytes.toBytes("f123"))); // [row-3, row-4]
//        FamilyFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryPrefixComparator(Bytes.toBytes("f123"))); // [row-3, row-4]
//        FamilyFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.LESS, new BinaryPrefixComparator(Bytes.toBytes("f123"))); // [row-1, row-2]
//        FamilyFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryPrefixComparator(Bytes.toBytes("f123"))); // [row-1, row-2]

        // SubstringComparator
//        FamilyFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("1")); // [row-1, row-2]
//        FamilyFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.NOT_EQUAL, new SubstringComparator("f")); // []

        // RegexStringComparator
//        FamilyFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.NOT_EQUAL, new RegexStringComparator("f")); // []
//        FamilyFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("f")); // [row-1, row-2, row-3, row-4]
//        FamilyFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("2")); // [row-3, row-4]

        // NullComparator
//        FamilyFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new NullComparator()); // []
        FamilyFilter familyFilter = new FamilyFilter(CompareFilter.CompareOp.NOT_EQUAL, new NullComparator()); // [row-1, row-2, row-3, row-4]


        scan.setFilter(familyFilter);
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
