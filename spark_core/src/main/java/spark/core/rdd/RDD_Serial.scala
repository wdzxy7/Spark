package spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Serial {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("Operator")
        val sc = new SparkContext(sparkConf)
        val words = sc.makeRDD(Array("Hello Java", "Hello Spark", "Hi Java", "hi Spark"))
        val sear = new Search("h")
        sear.getMatch(words)
    }

    case class Search(query: String) {
        def isMatch(s: String): Boolean = {
            s.contains(query)
        }

        def getMatch(rdd: RDD[String]): RDD[String] = {
            rdd.filter(isMatch)
        }

        def getMatch2(rdd: RDD[String]): RDD[String] = {
            val s = query
            rdd.filter(words => words.contains(s))
        }
    }
}
