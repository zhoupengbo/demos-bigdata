package com.zpb.demos.comparator;

import org.apache.hadoop.hbase.filter.NullComparator;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 控制比较式，判断当前值是不是为null。
 * 是null返回0，不是null返回1，仅支持 EQUAL 和非 EQUAL。
 */
public class NullComparatorDemo {

    public static void main(String[] args) {
        NullComparator nc = new NullComparator();
        int i1 = nc.compareTo(Bytes.toBytes("abc"));
        int i2 = nc.compareTo(Bytes.toBytes(""));
        int i3 = nc.compareTo(null);
        System.out.println(i1); // 1
        System.out.println(i2); // 1
        System.out.println(i3); // 0
    }
}
