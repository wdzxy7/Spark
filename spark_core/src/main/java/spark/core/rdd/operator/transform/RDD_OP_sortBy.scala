package spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object RDD_OP_sortBy {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("Operator")
        val sc = new SparkContext(sparkConf)
        val rdd = sc.makeRDD(List(2, 1, 6, 4, 3, 5), 4)
        var sort_rdd = rdd.sortBy(num=>num)
        val rdd2 = sc.makeRDD(List(("1", 1), ("11", 2), ("2", 3)))
        rdd2.collect().foreach(println)
        val sort_rdd2 = rdd2.sortBy(t=>t._1)
    }
}
