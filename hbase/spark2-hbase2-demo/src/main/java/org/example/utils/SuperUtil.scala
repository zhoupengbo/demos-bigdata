package org.example.utils

import org.apache.commons.codec.digest.DigestUtils
import org.example.constant.Constants

object SuperUtil {

  /**
   * rowkey hash 散列
   * @param orowKey
   * @param hashNums
   * @return
   */
  def getRowkey(orowKey: String,hashNums: Int): String = {
    val md5Str = DigestUtils.md5Hex(orowKey) // 拿到32位小写MD5值
    val hashCode = md5Str.hashCode()
    String.format(Constants.splitFormat, new Integer(Math.abs(hashCode) % (hashNums + 1))) + "_" + orowKey
  }

  // 转换RDD格式使其满足要求
  def bulkTransfer(array: Array[(Array[Byte], Array[Byte], Array[Byte], Array[Byte])]): List[(Array[Byte], Array[Byte], Array[Byte], Array[Byte])] = {
    array.toList
  }

}
