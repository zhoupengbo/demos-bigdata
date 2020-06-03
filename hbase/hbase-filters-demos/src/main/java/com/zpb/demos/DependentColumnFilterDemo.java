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
 * 参考例过滤器
 * 用于添加列间时间戳匹配的过滤器将仅保留目标列中具有相应时间戳的条目的单元格与Scan.setBatch不兼容，因为操作需要完整的行才能进行正确过滤
 * 一种允许用户指定一个参考列或引用列来过滤其他列的过滤器，过滤的原则是基于参考列的时间戳来进行筛选 。
 * 该过滤器尝试找到该列所在的每一行，并返回该行具有相同时间戳的全部键值对。
 * 如果某行不包含这个指定的列，则什么都不返回。
 * dropDependentColumn: 决定参考列被返回还是丢弃，为true时表示参考列被返回，为false时表示被丢弃。
 * 可以把DependentColumnFilter理解为一个valueFilter和一个时间戳过滤器的组合。
 *
 */
public class DependentColumnFilterDemo {

    private static boolean isok = false;
    private static String tableName = "test";
    private static String[] cfs = new String[]{"f1", "f2"};
    private static String[] data1 = new String[]{"row-1:f2:c3:1234abc56", "row-3:f1:c3:1234321"};
    private static String[] data2 = new String[]{
            "row-1:f1:c1:abcdefg", "row-1:f2:c2:abc", "row-2:f1:c1:abc123456", "row-2:f2:c2:1234abc567"
    };

    public static void main(String[] args) throws IOException, InterruptedException {

        MyBase myBase = new MyBase();
        Connection connection = myBase.createConnection();
        if (isok) {
            myBase.deleteTable(connection, tableName);
            myBase.createTable(connection, tableName, cfs);
            // 造数据
            myBase.putRows(connection, tableName, data1);  // 第一批数据
            Thread.sleep(10);
            myBase.putRows(connection, tableName, data2);  // 第二批数据
        }
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();

        // 构造方法一
        DependentColumnFilter filter = new DependentColumnFilter(Bytes.toBytes("f1"), Bytes.toBytes("c1"));  // [row-1:f1:c1:abcdefg, row-1:f2:c2:abc, row-2:f1:c1:abc123456, row-2:f2:c2:1234abc567]

        // 构造方法二 boolean dropDependentColumn=true
//        DependentColumnFilter filter = new DependentColumnFilter(Bytes.toBytes("f1"), Bytes.toBytes("c1"), true);  // [row-1:f2:c2:abc, row-2:f2:c2:1234abc567]

        // 构造方法二 boolean dropDependentColumn=false  默认为false
//        DependentColumnFilter filter = new DependentColumnFilter(Bytes.toBytes("f1"), Bytes.toBytes("c1"), false); // [row-1:f1:c1:abcdefg, row-1:f2:c2:abc, row-2:f1:c1:abc123456, row-2:f2:c2:1234abc567]

        // 构造方法三 + BinaryComparator 比较器过滤数据
//        DependentColumnFilter filter = new DependentColumnFilter(Bytes.toBytes("f1"), Bytes.toBytes("c1"), false,
//                CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("abcdefg"))); // [row-1:f1:c1:abcdefg, row-1:f2:c2:abc]

        // 构造方法三 + BinaryPrefixComparator 比较器过滤数据
//        DependentColumnFilter filter = new DependentColumnFilter(Bytes.toBytes("f1"), Bytes.toBytes("c1"), false,
//                CompareFilter.CompareOp.EQUAL, new BinaryPrefixComparator(Bytes.toBytes("abc")));  // [row-1:f1:c1:abcdefg, row-1:f2:c2:abc, row-2:f1:c1:abc123456, row-2:f2:c2:1234abc567]

        // 构造方法三 + SubstringComparator 比较器过滤数据
//        DependentColumnFilter filter = new DependentColumnFilter(Bytes.toBytes("f1"), Bytes.toBytes("c1"), false,
//                CompareFilter.CompareOp.EQUAL, new SubstringComparator("1234"));  // [row-2:f1:c1:abc123456, row-2:f2:c2:1234abc567]

        // 构造方法三 + RegexStringComparator 比较器过滤数据
//        DependentColumnFilter filter = new DependentColumnFilter(Bytes.toBytes("f1"), Bytes.toBytes("c1"), false,
//                CompareFilter.CompareOp.EQUAL, new RegexStringComparator("[a-z]"));  // [row-1:f1:c1:abcdefg, row-1:f2:c2:abc, row-2:f1:c1:abc123456, row-2:f2:c2:1234abc567]

        // 构造方法三 + RegexStringComparator 比较器过滤数据
//        DependentColumnFilter filter = new DependentColumnFilter(Bytes.toBytes("f1"), Bytes.toBytes("c1"), false,
//                CompareFilter.CompareOp.EQUAL, new RegexStringComparator("1234[a-z]"));  // []  思考题：与上例对比，想想为什么为空？

        scan.setFilter(filter);
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
