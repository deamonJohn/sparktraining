package org.training.spark.training

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mobike on 2018/2/26.
  */
object TestWorldCount {
def main(args:Array[String]): Unit ={
  val sparkConf=new SparkConf().setAppName("test")

  if(args.length<2){
    println("请输入参数--------hdfs文件路径  hdfsFile  svaeFile")
   // System.exit(1);
  }
  if(args.length==0){
    sparkConf.setMaster("local")
  }
  println("ceshi")
  val ssc=new SparkContext(sparkConf)
  val rdd=ssc.textFile("data/textfile/stopword.txt")
 // val rdd=ssc.textFile(args(0))
  val words =rdd.flatMap(_.split(" ")).map((_,1))
  val rdd2= words.reduceByKey(_+_).collect().foreach(println(_))
  ssc.stop();
}
}
