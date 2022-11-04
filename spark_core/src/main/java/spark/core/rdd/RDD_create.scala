package spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_create {
    def main(args: Array[String]): Unit = {
        val spark_conf = new SparkConf().setMaster("local").setAppName("RDD")
        val sc = new SparkContext(spark_conf)
        val seq = Seq[Int](1, 2, 3, 4)
        // val rdd: RDD[Int] = sc.parallelize(seq)
        val rdd: RDD[Int] = sc.makeRDD(seq)
        rdd.collect().foreach(println)
        sc.stop()
    }

}
