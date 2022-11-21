package spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object RDD_OP_coalesce {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("Operator")
        val sc = new SparkContext(sparkConf)
        val rdd = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 4)
        val rdd1 = rdd.coalesce(2)
        val rdd2 = rdd.coalesce(2, shuffle = true)

    }
}
