package spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object RDD_OP_combineByKey {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("Operator")
        val sc = new SparkContext(sparkConf)
        val rdd = sc.makeRDD(List(("a", 1), ("b", 2), ("a", 3), ("a", 4), ("b", 5)))
        val com_rdd = rdd.combineByKey(
            v => (v, 1),
            (t:(Int, Int), v) => {
                println(t, v)
                (t._1 + v, t._2 + 1)
            },
            (t:(Int, Int), v:(Int, Int)) => {
                (t._1 + v._1, t._2 + v._2)
            }
        ).collect()
    }
}
