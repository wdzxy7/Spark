package spark.core.rdd

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object RDD_Partion {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("Operator")
        val sc = new SparkContext(sparkConf)
        val rdd = sc.makeRDD(List(
            ("nba", "asdasdasd"),
            ("cba", "sadsadsad"),
            ("nba", "dadasdasd"),
            ("wwe", "sadasdasd"),
        ))
        var my_rdd = rdd.partitionBy(new MyPartitioner)
    }
    class MyPartitioner extends Partitioner{
        override def numPartitions: Int = 3

        override def getPartition(key: Any): Int = {
            key match {
                case "nba" => 0
                case "cba" => 1
                case _ => 2
            }
        }
    }
}
