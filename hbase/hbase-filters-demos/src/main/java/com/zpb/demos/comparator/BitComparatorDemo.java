package com.zpb.demos.comparator;

import org.apache.hadoop.hbase.filter.BitComparator;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 位比价器，通过BitwiseOp提供的AND（与）、OR（或）、NOT（非）进行比较。
 * 返回结果要么为1要么为0，仅支持 EQUAL 和非 EQUAL。
 */
public class BitComparatorDemo {

    public static void main(String[] args) {

        BitComparator bc1 = new BitComparator(Bytes.toBytes("20200412"), BitComparator.BitwiseOp.OR);
        int i = bc1.compareTo(Bytes.toBytes("20200412"), 0, 8);
        System.out.println(i); // 0
        BitComparator bc2 = new BitComparator(Bytes.toBytes("20200413"), BitComparator.BitwiseOp.AND);
        int j = bc2.compareTo(Bytes.toBytes("20200413"), 0, 8);
        System.out.println(j); // 0
        BitComparator bc3 = new BitComparator(Bytes.toBytes("20200413"), BitComparator.BitwiseOp.XOR);
        int x = bc3.compareTo(Bytes.toBytes("20200414"), 0, 8);
        System.out.println(x); // 0

    }



}
