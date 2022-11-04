package spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_OP_glom {
    def main(args: Array[String]): Unit = {
        val spark_conf = new SparkConf().setMaster("local").setAppName("RDD")
        val sc = new SparkContext(spark_conf)
        val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)
        val glom_rdd = rdd.glom()
        val max_rdd = glom_rdd.map(
            array => array.max
        )
        val temp = max_rdd.collect()
        for (i <- temp)
            println(i)
        println(max_rdd.collect().sum)
    }

}
