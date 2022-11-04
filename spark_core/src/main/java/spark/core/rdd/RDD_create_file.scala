package spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_create_file {
    def main(args: Array[String]): Unit = {
        val spark_conf = new SparkConf().setMaster("local").setAppName("RDD")
        val sc = new SparkContext(spark_conf)
        // val rdd = sc.textFile("./data/spark_core_data")
        // val rdd = sc.textFile("./data/spark_core_data/1.txt")
        val rdd1: RDD[String] = sc.textFile("./data/spark_core_data/*.txt")
        //rdd1.collect().foreach(println)
        val rdd2 = sc.wholeTextFiles("./data/spark_core_data/1.txt")
        rdd2.collect().foreach(println)
        sc.stop()
    }

}
