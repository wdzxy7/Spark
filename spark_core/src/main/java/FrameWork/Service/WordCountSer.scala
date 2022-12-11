package FrameWork.Service

import FrameWork.Common.WordSerT
import FrameWork.Dao.WordCountDao
import org.apache.spark.rdd.RDD
/*
进行运算操作
 */
class WordCountSer extends WordSerT{

    private val WordCountDao = new WordCountDao()

    def dataAnalysis(): Array[(String, Int)] = {
        val lines = WordCountDao.readFile("./data/spark_core_data/1.txt")
        val words: RDD[String] = lines.flatMap(_.split(" "))
        val word_to_one = words.map(
            word => (word, 1)
        )
        val word_group: RDD[(String, Iterable[(String, Int)])] = word_to_one.groupBy(t => t._1)
        val word_count = word_group.map{
            case (word, list) =>
                list.reduce(
                    (t1, t2) => {
                        (t1._1, t1._2 + t2._2)
                    }
                )
        }
        val array: Array[(String, Int)] = word_count.collect()
        return array
    }
}
