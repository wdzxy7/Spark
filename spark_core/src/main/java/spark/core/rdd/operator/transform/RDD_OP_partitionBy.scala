package spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object RDD_OP_partitionBy {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("Operator")
        val sc = new SparkContext(sparkConf)
        val rdd = sc.makeRDD(List("1", "1", "3", "4"), 4)
        val map_rdd = rdd.map(num=>(num, num.toInt * 2))
        val red_rdd = map_rdd.reduceByKey((x, y) => {x + y})
        red_rdd.collect().foreach(println)
    }
}
