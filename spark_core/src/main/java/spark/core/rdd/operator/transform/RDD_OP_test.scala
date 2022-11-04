package spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_OP_test {
    def main(args: Array[String]): Unit = {
        val spark_conf = new SparkConf().setMaster("local").setAppName("RDD")
        val sc = new SparkContext(spark_conf)
    }

}
