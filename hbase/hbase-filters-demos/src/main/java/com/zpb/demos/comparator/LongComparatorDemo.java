package com.zpb.demos.comparator;

import org.apache.hadoop.hbase.filter.LongComparator;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Long 型专用比较器，返回值：0 -1 1
 */
public class LongComparatorDemo {

    public static void main(String[] args) {
        LongComparator longComparator = new LongComparator(1000L);
        int i = longComparator.compareTo(Bytes.toBytes(1000L), 0, 8);
        System.out.println(i); // 0
        int i2 = longComparator.compareTo(Bytes.toBytes(1001L), 0, 8);
        System.out.println(i2); // -1
        int i3 = longComparator.compareTo(Bytes.toBytes(998L), 0, 8);
        System.out.println(i3); // 1
    }
}
