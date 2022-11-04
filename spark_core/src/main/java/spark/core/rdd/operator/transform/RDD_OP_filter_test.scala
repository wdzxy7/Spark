package spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat

object RDD_OP_filter_test {
    def main(args: Array[String]): Unit = {
        val spark_conf = new SparkConf().setMaster("local").setAppName("RDD")
        val sc = new SparkContext(spark_conf)
        val mess = sc.textFile("./data/data/apache.log")
        val map_res = mess.map(
            mess => {
                val temp = mess.split(" ")
                (temp(3), temp(6))
            }
        )
        val filter_res = map_res.filter(filter_fun)
        val res = filter_res.map(mess => mess._2)
        res.collect().foreach(println)
        sc.stop()
    }
    def filter_fun (data: (String, String)): Boolean = {
        if (data._1.startsWith("17/05/2015"))
            true
        else
            false

    }
}
