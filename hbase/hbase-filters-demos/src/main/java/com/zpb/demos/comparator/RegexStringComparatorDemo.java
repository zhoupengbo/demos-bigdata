package com.zpb.demos.comparator;

import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 提供一个正则的比较器，支持正则表达式的值比较，仅支持 EQUAL 和非 EQUAL。
 * 匹配成功返回0，匹配失败返回1
 */
public class RegexStringComparatorDemo {

    public static void main(String[] args) {
        RegexStringComparator rsc = new RegexStringComparator("abc");
        int abc = rsc.compareTo(Bytes.toBytes("abcd"), 0, 3);
        System.out.println(abc); // 0
        int bcd = rsc.compareTo(Bytes.toBytes("bcd"), 0, 3);
        System.out.println(bcd); // 1

        String check = "^([a-z0-9A-Z]+[-|\\.]?)+[a-z0-9A-Z]@([a-z0-9A-Z]+(-[a-z0-9A-Z]+)?\\.)+[a-zA-Z]{2,}$";
        RegexStringComparator rsc2 = new RegexStringComparator(check);
        int code = rsc2.compareTo(Bytes.toBytes("zpb@163.com"), 0, "zpb@163.com".length());
        System.out.println(code); // 0
        int code2 = rsc2.compareTo(Bytes.toBytes("zpb#163.com"), 0, "zpb#163.com".length());
        System.out.println(code2); // 1
    }
}
