package spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_OP_tran_par {
    def main(args: Array[String]): Unit = {
        val spark_conf = new SparkConf().setMaster("local").setAppName("RDD")
        val sc = new SparkContext(spark_conf)
        val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)
        val map_rdd = rdd.mapPartitions(
            iter => {
                iter.map(_ * 2)
                // iter.map(num => num * 2)
            }
        )
        map_rdd.collect().foreach(println)
        sc.stop()
    }

}
