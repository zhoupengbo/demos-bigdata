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
 * 用于列名（Qualifier）过滤。
 * 该过滤器用于基于列限定符进行过滤. 它需要一个运算符（等于，更大，不等于等）和一个键的列限定符部分的byte []比较器.
 * 该过滤器可以使用WhileMatchFilter和SkipFilter进行包装，以添加更多控件.
 * 可以使用FilterList组合多个过滤器.
 * 如果要查找已知的列限定符，请直接使用Get.addColumn(byte[], byte[])而不是过滤器.
 */
public class QualifierFilterDemo {

    private static boolean isok = false;
    private static String tableName = "test";
    private static String[] cfs = new String[]{"f"};
    private static String[] data = new String[]{
            "row-1:f:name:Wang", "row-1:f:age:20",
            "row-2:f:name:Zhou", "row-2:f:age:10",
            "row-3:f:gender:男", "row-3:f:name:Li",
            "row-4:f:namana:xyz", "row-4:f:age:Zhao"
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
//        QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("age"))); // [row-1:f:age, row-2:f:age, row-4:f:age]
//        QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.NOT_EQUAL, new BinaryComparator(Bytes.toBytes("name"))); // [row-1:f:age, row-2:f:age, row-3:f:gender, row-4:f:age, row-4:f:namana]
//        QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.GREATER, new BinaryComparator(Bytes.toBytes("gender"))); // [row-1:f:name, row-2:f:name, row-3:f:name, row-4:f:namana]
//        QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes("name"))); // [row-1:f:name, row-2:f:name, row-3:f:name]
//        QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.LESS, new BinaryComparator(Bytes.toBytes("gender"))); // [row-1:f:age, row-2:f:age, row-4:f:age]
//        QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("gender"))); // [row-1:f:age, row-2:f:age, row-3:f:gender, row-4:f:age]

        // BinaryPrefixComparator
//        QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.toBytes("nam"))); // [row-1:f:name, row-2:f:name, row-3:f:name, row-4:f:namana]
//        QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.NOT_EQUAL, new BinaryPrefixComparator(Bytes.toBytes("nam"))); // [row-1:f:age, row-2:f:age, row-3:f:gender, row-4:f:age]
//        QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.GREATER, new BinaryPrefixComparator(Bytes.toBytes("g"))); // [row-1:f:name, row-2:f:name, row-3:f:name, row-4:f:namana]
//        QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL, new BinaryPrefixComparator(Bytes.toBytes("n"))); // [row-1:f:name, row-2:f:name, row-3:f:name, row-4:f:namana]
//        QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.LESS, new BinaryPrefixComparator(Bytes.toBytes("m"))); // [row-1:f:age, row-2:f:age, row-3:f:gender, row-4:f:age]
//        QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.LESS_OR_EQUAL, new BinaryPrefixComparator(Bytes.toBytes("a"))); // [row-1:f:age, row-2:f:age, row-4:f:age]

        // SubstringComparator
//        QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("g")); // [row-1:f:age, row-2:f:age, row-3:f:gender, row-4:f:age]
//        QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.NOT_EQUAL, new SubstringComparator("n")); // [row-1:f:age, row-2:f:age, row-4:f:age]

        // RegexStringComparator
//        QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.NOT_EQUAL, new RegexStringComparator("nam")); // [row-1:f:age, row-2:f:age, row-3:f:gender, row-4:f:age]
//        QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("nam")); // [row-1:f:name, row-2:f:name, row-3:f:name, row-4:f:namana]
//        QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("n[a-z]m")); // [row-1:f:name, row-2:f:name, row-3:f:name, row-4:f:namana]

        // NullComparator
//        QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new NullComparator()); // []
        QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.NOT_EQUAL, new NullComparator()); // [row-1:f:age, row-1:f:name, row-2:f:age, row-2:f:name, row-3:f:gender, row-3:f:name, row-4:f:age, row-4:f:namana]

        scan.setFilter(qualifierFilter);
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
