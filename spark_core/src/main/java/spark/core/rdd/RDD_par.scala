package spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_par {
    def main(args: Array[String]): Unit = {
        val spark_conf = new SparkConf().setMaster("local").setAppName("RDD")
        val sc = new SparkContext(spark_conf)
        val rdd =  sc.makeRDD(List(1, 2, 3, 4), 2)
        rdd.collect().foreach(println)
        sc.stop()
    }

}
