package spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_OP_flat {
    def main(args: Array[String]): Unit = {
        val spark_conf = new SparkConf().setMaster("local").setAppName("RDD")
        val sc = new SparkContext(spark_conf)
        val rdd = sc.makeRDD(List(List(1, 2), List(3, 4)))
        val flat_rdd = rdd.flatMap(mess => mess)
        flat_rdd.collect().foreach(println)
    }

}
