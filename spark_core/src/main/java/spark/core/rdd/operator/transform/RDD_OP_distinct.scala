package spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object RDD_OP_distinct {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("Operator")
        val sc = new SparkContext(sparkConf)
        val rdd = sc.makeRDD(List(1, 2, 3, 4, 1, 2, 3))
        val dis_rdd = rdd.distinct()
        dis_rdd.collect().foreach(println)
        sc.stop()
    }
}
