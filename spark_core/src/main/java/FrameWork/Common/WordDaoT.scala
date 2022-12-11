package FrameWork.Common

import FrameWork.Utils.WordUtil
import org.apache.spark.rdd.RDD

trait WordDaoT {

    def readFile(path: String): RDD[String] = {
        val sc = WordUtil.get()
        val lines: RDD[String] = sc.textFile(path)
        return lines
    }
}
