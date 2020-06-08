package cn.cosette.spark.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * cn.cosette.spark.day01
 *
 * @Auther: Cosette
 * @Date: 2020/6/8 11:59
 * @Description:
 * * 1.创建SparkContext
 * * 2.创建RDD
 * * 3.调用RDD的Transformation方法
 * * 4.调用Action
 * * 5.释放资源
 */
object WordCount {
  def main(args: Array[String]): Unit = {

    //编写Spark程序 就是对抽象的神奇大集合【rdd】进行编程
    //调用它高度封装的API

    val conf: SparkConf = new SparkConf().setAppName("WordCount")

    //创建SparkContext，使用SparkContext来创建RDD
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile(args(0))

    ////////////Transformation开始////////////
    //切分压平
    val words: RDD[String] = lines.flatMap(_.split(" "))

    //将单词和1组合放在元组中
    val wordAndOne: RDD[(String, Int)] = words.map((_,1))

    //分组聚合，reduceByKey可以先局部聚合再全局聚合
    val reduced: RDD[(String, Int)] = wordAndOne.reduceByKey(_+_)

    //排序
    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2,false)

    //调用Action将计算结果保存到HDFS中
    sorted.saveAsTextFile(args(1))

    //释放资源
    sc.stop()





  }

}
