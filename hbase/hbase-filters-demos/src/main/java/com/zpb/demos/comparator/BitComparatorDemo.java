package com.zpb.demos.comparator;

import org.apache.hadoop.hbase.filter.BitComparator;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 位比价器，通过BitwiseOp提供的AND（与）、OR（或）、NOT（非）进行比较。
 * 返回结果要么为1要么为0，仅支持 EQUAL 和非 EQUAL。
 */
public class BitComparatorDemo {

    public static void main(String[] args) {

        // 长度相同按位或比较：由低位起逐位比较，每一位按位或比较都为0，则返回1，否则返回0。
        BitComparator bc1 = new BitComparator(new byte[]{0,0,0,0}, BitComparator.BitwiseOp.OR);
        int i = bc1.compareTo(new byte[]{0,0,0,0}, 0, 4);
        System.out.println(i); // 1
        // 长度相同按位与比较：由低位起逐位比较，每一位按位与比较都为0，则返回1，否则返回0。
        BitComparator bc2 = new BitComparator(new byte[]{1,0,1,0}, BitComparator.BitwiseOp.AND);
        int j = bc2.compareTo(new byte[]{0,1,0,1}, 0, 4);
        System.out.println(j); // 1
        // 长度相同按位异或比较：由低位起逐位比较，每一位按位异或比较都为0，则返回1，否则返回0。
        BitComparator bc3 = new BitComparator(new byte[]{1,0,1,0}, BitComparator.BitwiseOp.XOR);
        int x = bc3.compareTo(new byte[]{1,0,1,0}, 0, 4);
        System.out.println(x); // 1
        // 长度不同，返回1，否则按位比较
        BitComparator bc4 = new BitComparator(new byte[]{1,0,1,0}, BitComparator.BitwiseOp.XOR);
        int y = bc4.compareTo(new byte[]{1,0,1}, 0, 3);
        System.out.println(y); // 1
    }



}
