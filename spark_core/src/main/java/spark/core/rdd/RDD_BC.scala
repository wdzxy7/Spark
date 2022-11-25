package spark.core.rdd

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object RDD_BC {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("Operator")
        val sc = new SparkContext(sparkConf)
        val rdd = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
        val map = mutable.Map(("a", 4), ("b", 5), ("c", 6))
        val bc = sc.broadcast(map)

        rdd.map{
            case (word, count) =>
                val l = bc.value.getOrElse(word, 0)
                (word, (count, l))
        }.collect().foreach(println)
    }

}
