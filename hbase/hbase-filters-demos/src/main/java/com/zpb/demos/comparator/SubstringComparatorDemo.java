package com.zpb.demos.comparator;

import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 判断提供的子串是否出现在value中，并且不区分大小写。
 * 包含字串返回0，不包含返回1，仅支持 EQUAL 和非 EQUAL。
 *
 */
public class SubstringComparatorDemo {

    public static void main(String[] args) {
        String value = "aslfjllkabcxxljsl";
        SubstringComparator sc = new SubstringComparator("abc");
        int i = sc.compareTo(Bytes.toBytes(value), 0, value.length());
        System.out.println(i); // 0

        SubstringComparator sc2 = new SubstringComparator("abd");
        int i2 = sc2.compareTo(Bytes.toBytes(value), 0, value.length());
        System.out.println(i2); // 1

        SubstringComparator sc3 = new SubstringComparator("ABC");
        int i3 = sc3.compareTo(Bytes.toBytes(value), 0, value.length());
        System.out.println(i3); // 0
    }
}
