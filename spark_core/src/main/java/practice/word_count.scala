package practice

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object word_count {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("Operator")
        val sc = new SparkContext(sparkConf)
        val words = sc.makeRDD(List("Hello Java", "Hello Spark", "Hi Java", "hi Spark"))
        // word_count1(words)
        word_count4(words)
    }

    def word_count1(words: RDD[String]): Unit = {
        val map_rdd = words.flatMap(_.split(" "))
        val map_rdd2 = map_rdd.map(word => (word, 1))
        val group = map_rdd2.groupByKey()
        val res = group.mapValues(iter => iter.size)
        res.collect().foreach(println)
    }

    def word_count2(words: RDD[String]): Unit =  {
        val map_rdd = words.flatMap(_.split(" "))
        val group = map_rdd.groupBy(word => word)
        val res = group.mapValues(iter => iter.size)
        res.collect().foreach(println)
    }

    def word_count3(words: RDD[String]): Unit =  {
        val map_rdd = words.flatMap(_.split(" "))
        val map_rdd2 = map_rdd.map(word => (word, 1))
        val res = map_rdd2.reduceByKey(_+_)
        res.collect().foreach(println)
    }

    def word_count4(words: RDD[String]): Unit =  {
        val map_rdd = words.flatMap(_.split(" "))
        val map_rdd2 = map_rdd.map(word => (word, 1))
        val res = map_rdd2.countByKey()
        res .foreach(println)
    }

    def word_count5(words: RDD[String]): Unit =  {
        val map_rdd = words.flatMap(_.split(" "))
        val res = map_rdd.countByValue()
        res.foreach(println)
    }


}

