package spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object RDD_OP_aggregateByKey {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("Operator")
        val sc = new SparkContext(sparkConf)
        val rdd = sc.makeRDD(List(("a", 1), ("b", 2), ("a", 3), ("a", 4)))
        val agg_rdd = rdd.aggregateByKey(5)(
            (x, y) => math.max(x, y),
            (x, y) => x + y
        )
        // agg_rdd.collect().foreach(println)
        rdd.foldByKey(0)(
            (x, y) => {
                println(x, y)
                x + y
            }
        ).collect().foreach(println)
    }
}
