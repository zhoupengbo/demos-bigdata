package com.zpb.demos;

import com.zpb.utils.MyBase;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * 该过滤器仅仅返回每行的第一列，可以用于高效的执行行数统计操作。估计实战意义不大。
 *
 * import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
 * scan 'test',{FILTER=>FirstKeyOnlyFilter.new()}
 *
 * scan 'test',{FILTER=>'FirstKeyOnlyFilter()'}
 */
public class FirstKeyOnlyFilterDemo {

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
        FirstKeyOnlyFilter firstKeyOnlyFilter = new FirstKeyOnlyFilter(); // [row-1:f1:p]
        scan.setFilter(firstKeyOnlyFilter);
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
