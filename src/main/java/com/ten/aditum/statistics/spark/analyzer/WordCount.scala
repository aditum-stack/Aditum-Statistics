package com.ten.aditum.statistics.spark.analyzer

import com.ten.aditum.statistics.spark.sparkutil.Util
import org.apache.spark.SparkContext

/**
  * 需求：单词计数
  * 思路：
  * 1、先把所有单词分割
  * 2、再给所有单词赋值(key,1)
  * 3、再用reduceByKey进行汇总
  * Created by Administrator on 2016/12/15.
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    var sc: SparkContext = Util.sc
    sc.textFile("D:/1.txt").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).foreach(println(_))
  }
}
