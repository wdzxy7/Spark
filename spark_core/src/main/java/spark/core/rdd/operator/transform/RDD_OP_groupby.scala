package spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

class RDD_OP_groupby {
    def main(args: Array[String]): Unit = {
        val spark_conf = new SparkConf().setMaster("local").setAppName("RDD")
        val sc = new SparkContext(spark_conf)
        val rdd = sc.makeRDD(List("hello", "spark", "scala", "hadoop"), 2)
        val group = rdd.groupBy(_.charAt(0))
        group.collect().foreach(println)
    }

}
