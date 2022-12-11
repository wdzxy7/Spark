package FrameWork.Common

import org.apache.spark.{SparkConf, SparkContext}
import FrameWork.Utils.WordUtil

trait WordAppT {

    def start (master: String="local[*]", app: String="Application") (op : => Unit): Unit = {
        val sparkConf = new SparkConf().setMaster(master).setAppName(app)
        val sc = new SparkContext(sparkConf)
        WordUtil.put(sc)
        try {
            op
        }
        catch {
            case ex => println(ex.getMessage)
        }
        WordUtil.clear()
    }

}
