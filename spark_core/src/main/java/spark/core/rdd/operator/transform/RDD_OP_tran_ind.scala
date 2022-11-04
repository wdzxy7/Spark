package spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

object RDD_OP_tran_ind {
    def main(args: Array[String]): Unit = {
        val spark_conf = new SparkConf().setMaster("local").setAppName("RDD")
        val sc = new SparkContext(spark_conf)
        val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)
        val mpi_rdd = rdd.mapPartitionsWithIndex(
            (index, iter) => {
                if (index == 1)
                    iter
                else
                    Nil.iterator
            }
        )
        mpi_rdd.collect().foreach(println)
        val w_rdd = rdd.mapPartitionsWithIndex(
            (index, iter) => {
                iter.map(mess => (index, mess))
            }
        )
        w_rdd.collect().foreach(println)
    }

}
