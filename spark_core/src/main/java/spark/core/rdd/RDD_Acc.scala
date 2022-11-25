package spark.core.rdd
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


object RDD_Acc {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("Operator")
        val sc = new SparkContext(sparkConf)
        val words = sc.makeRDD(Array("Java", "Spark", "Java", "Spark"))
        val acc = new MyAcc()
        sc.register(acc, "acc")
        words.foreach(
            word => {
                acc.add(word)
            }
        )
        println(acc)
    }

    class MyAcc extends AccumulatorV2[String, mutable.Map[String, Long]] {

        private val wc_map = mutable.Map[String, Long]()

        override def isZero: Boolean = {
            wc_map.isEmpty
        }

        override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
            new MyAcc()
        }

        override def reset(): Unit = {
            wc_map.clear()
        }

        override def add(v: String): Unit = {
            val new_count = wc_map.getOrElse(v, 0L) + 1
            wc_map.update(v, new_count)
        }

        override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
            val map1 = this.wc_map
            val map2 = other.value
            map2.foreach{
                case (word, count) =>
                    val new_count = map1.getOrElse(word, 0L) + count
                    map1.update(word, new_count)
            }
        }

        override def value: mutable.Map[String, Long] = wc_map

    }
}
