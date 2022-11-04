package spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

import scala.math.Ordering.ordered

class RDD_OP_tran_test {
    def main(args: Array[String]): Unit = {
        val spark_conf = new SparkConf().setMaster("local").setAppName("RDD")
        val sc = new SparkContext(spark_conf)
        val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)
        val res_rdd = rdd.mapPartitions(
            iter => {
                List(iter.max).iterator
            }
        )
        val  res_rdd2 = rdd.mapPartitions(get_max)
        res_rdd2.collect().foreach(println)
        sc.stop()
    }
    def get_max (iter: Iterator[Int]) : Iterator[Int] = {
        List(iter.max).iterator
    }
}
