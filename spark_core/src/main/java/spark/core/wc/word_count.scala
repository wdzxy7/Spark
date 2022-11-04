package spark.core.wc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object word_count {
    def main(args: Array[String]): Unit = {
        val  sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparkConf)
        val lines: RDD[String] = sc.textFile("./data/spark_core_data/1.txt")
        val words: RDD[String] = lines.flatMap(_.split(" "))
        val word_to_one = words.map(
            word => (word, 1)
        )
        val word_group: RDD[(String, Iterable[(String, Int)])] = word_to_one.groupBy(t => t._1)
        val word_count = word_group.map{
            case (word, list) => {
                list.reduce(
                    (t1, t2) => {
                        (t1._1, t1._2 + t2._2)
                    }
                )
            }
        }
        val array: Array[(String, Int)] = word_count.collect()
        array.foreach(println)
        sc.stop()

    }

}
