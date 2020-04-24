package com.zpb.demos.comparator;

import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 二进制比较器，只比较前缀是否与指定字节数组相同。返回值：0，-1，1
 */
public class BinaryPrefixComparatorDemo {

    public static void main(String[] args) {

        BinaryPrefixComparator bc = new BinaryPrefixComparator(Bytes.toBytes("b"));

        int code1 = bc.compareTo(Bytes.toBytes("bbb"), 0, 3);
        System.out.println(code1); // 0
        int code2 = bc.compareTo(Bytes.toBytes("aaa"), 0, 3);
        System.out.println(code2); // 1
        int code3 = bc.compareTo(Bytes.toBytes("ccc"), 0, 3);
        System.out.println(code3); // -1
        int code4 = bc.compareTo(Bytes.toBytes("bbf"), 0, 3);
        System.out.println(code4); // 0
        int code5 = bc.compareTo(Bytes.toBytes("bbbedf"), 0, 6);
        System.out.println(code5); // 0
    }
}
