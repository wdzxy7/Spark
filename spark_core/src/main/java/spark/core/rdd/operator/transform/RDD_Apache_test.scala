package spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Apache_test {
    def main(args: Array[String]): Unit = {
        val spark_conf = new SparkConf().setMaster("local").setAppName("RDD")
        val sc = new SparkContext(spark_conf)
        val raw_data = sc.textFile("./data/data/apache.log")
        // raw_data.collect().foreach(println)
        val mess_rdd = raw_data.map(
            line => {
                val data = line.split(" ")
                data(6)
            }
        )
        mess_rdd.collect().foreach(println)
        sc.stop()
    }
    def change(line: String): String = {
        val temp = line.split(" ")
        temp(6)
    }
}
