package com.zpb.demos.comparator;

import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 二进制比较器，用于按字典顺序比较指定字节数组。
 * Bytes.compareTo:
 * 两个字符串首字母不同，则该方法返回首字母的asc码的差值
 * 参与比较的两个字符串如果首字符相同，则比较下一个字符，直到有不同的为止，返回该不同的字符的asc码差值
 * 两个字符串不一样长，可以参与比较的字符又完全一样，则返回两个字符串的长度差值
 * 返回值：0，-1，1
 */
public class BinaryComparatorDemo {

    public static void main(String[] args) {

        BinaryComparator bc = new BinaryComparator(Bytes.toBytes("bbb"));

        int code1 = bc.compareTo(Bytes.toBytes("bbb"), 0, 3);
        System.out.println(code1); // 0
        int code2 = bc.compareTo(Bytes.toBytes("aaa"), 0, 3);
        System.out.println(code2); // 1
        int code3 = bc.compareTo(Bytes.toBytes("ccc"), 0, 3);
        System.out.println(code3); // -1
        int code4 = bc.compareTo(Bytes.toBytes("bbf"), 0, 3);
        System.out.println(code4); // -4
        int code5 = bc.compareTo(Bytes.toBytes("bbbedf"), 0, 6);
        System.out.println(code5); // -3
    }
}
