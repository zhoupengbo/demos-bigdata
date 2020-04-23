package com.zpb.demos;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class FilterListDemo {

    public static void main(String[] args) {
        Scan scan = new Scan();
        // FilterList.Operator.MUST_PASS_ALL
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(
                Bytes.toBytes("cf"),
                Bytes.toBytes("column"),
                CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes("my value")
        );
        list.addFilter(filter1);
        SingleColumnValueFilter filter2 = new SingleColumnValueFilter(
                Bytes.toBytes("cf"),
                Bytes.toBytes("column"),
                CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes("my other value")
        );
        list.addFilter(filter2);
        scan.setFilter(list);
    }
}
