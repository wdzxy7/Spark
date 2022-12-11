package FrameWork.Application

import FrameWork.Common.WordAppT
import FrameWork.Controller.WordCountCon
import org.apache.spark.{SparkConf, SparkContext}

object WordCountApp extends App with WordAppT {

    start() {
        val controller = new WordCountCon()
        controller.execute()
    }
}
