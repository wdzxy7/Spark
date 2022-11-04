package spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Date

object RDD_OP_tran {
    def main(args: Array[String]): Unit = {
        val spark_conf = new SparkConf().setMaster("local").setAppName("RDD")
        val sc = new SparkContext(spark_conf)
        val mess = sc.textFile("./data/data/apache.log")
        // mess.collect()
        var map_mess = mess.map(mess => {
            val temp = mess.split(" ")
            temp(3)
        }
        )
        // map_mess.collect()
        val day_mess = map_mess.map(
            mess => {
                var sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
                var time: Date = sdf.parse(mess)
                sdf = new SimpleDateFormat("HH")
                val hour = sdf.format(time)
                (hour, 1)
            }
        )
        // day_mess.collect()
        val res = day_mess.groupBy(_._1)
        //res.collect()
        res.map {
            case (day, t) => {
                (day, t.size)
            }
        }.collect().foreach(println)
        sc.stop()
    }
}
