package practice

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object agent_test {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("Operator")
        val sc = new SparkContext(sparkConf)
        val data = sc.textFile("./data/data/agent.log")
        val map_data = data.map(
            line => {
                val temp = line.split(" ")
                ((temp(1), temp(4)), 1)
            }
        )
        val red_rdd = map_data.reduceByKey(
            (v, t) => v + t
        )
        val map_rdd2 = red_rdd.map{
            case ((prv, ad), sum) => (prv, (ad, sum))
        }
        val gru_rdd = map_rdd2.groupByKey()
        val sort_rdd = gru_rdd.mapValues(
            iter => {
                iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
            }
        )
        sort_rdd.collect().foreach(println)
    }
}
