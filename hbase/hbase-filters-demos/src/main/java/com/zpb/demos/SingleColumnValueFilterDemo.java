package com.zpb.demos;

import com.zpb.utils.MyBase;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;

public class SingleColumnValueFilterDemo {

    private static String tableName = "test";
    private static String[] cfs = new String[]{"f1","f2"};
    public static void main(String[] args) throws IOException {
        MyBase myBase = new MyBase();
        Connection connection = myBase.createConnection();
        myBase.createTable(connection,tableName,cfs);
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        BinaryPrefixComparator comp = new BinaryPrefixComparator(Bytes.toBytes("zpb1"));
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes("f1"), Bytes.toBytes("c1"), CompareFilter.CompareOp.EQUAL, comp);
        filter.setFilterIfMissing(true);
        scan.setFilter(filter);
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();
        while (iterator.hasNext()){
            Result result = iterator.next();
            for (Cell cell : result.listCells()) {
                System.out.println("qualifier:" +Bytes.toString(CellUtil.cloneQualifier(cell)) );
                System.out.println("value:" +Bytes.toString(CellUtil.cloneValue(cell)) );
                System.out.println("-------------------------------");
            }
        }
    }
}
