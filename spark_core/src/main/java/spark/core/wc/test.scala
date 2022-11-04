package spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object test {
    def main(args: Array[String]): Unit = {
        val  sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(sparkConf)
        val lines: RDD[String] = sc.textFile("./data/spark_core_data/1.txt")
        val words: RDD[String] = lines.flatMap(_.split(" "))
        val word_group: RDD[(String, Iterable[String])] = words.groupBy(word=>word)
        val word_count = word_group.map{
            case (word, list) => {
                (word, list.size)
            }
        }
        val array: Array[(String, Int)] = word_count.collect()
        array.foreach(println)
        sc.stop()

    }
}
