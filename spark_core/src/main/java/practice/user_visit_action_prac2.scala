package practice

import org.apache.spark.{SparkConf, SparkContext}

import java.lang

object user_visit_action_prac2 {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("Operator")
        val sc = new SparkContext(sparkConf)
        val temp_data = sc.textFile("./data/spark_core_data/user_visit_action.txt")
        temp_data.cache()
        val data = temp_data.map(
            line => {
                val t = line.split("_")
                User_Action(
                    t(0),
                    t(1).toLong,
                    t(2),
                    t(3).toLong,
                    t(4),
                    t(5),
                    t(6).toLong,
                    t(7).toLong,
                    t(8),
                    t(9),
                    t(10),
                    t(11),
                    t(12).toLong,
                )
            }
        )
        data.cache()
        val click_res = data.map(i => (i.page_id, 1L)).reduceByKey(_+_)
        val session_group = data.groupBy(i => i.session_id)
        //session_group.take(10).foreach(println)
        val session_data = session_group.mapValues(i => {
            val t = i.toList.sortBy(_.action_time)
            val page_list = t.map(_.page_id)
            val res = page_list.zip(page_list.tail)
            res.map((_, 1D))
        }).map(_._2).flatMap(t => t)
        val click_map = click_res.collect().toMap
        /*
        val session_res = session_data.reduceByKey(_+_).foreach{
            case ((pg1, pg2), sum) =>
                val click_sum = click_map.getOrElse(pg1, 0L)
                val rate = sum / click_sum
                println(s"pg:${pg1} to pg:${pg2} rate is :${rate}")
        }

        val res_data = session_data.reduceByKey(_+_).map{
            case ((pg1, pg2), sum) =>
                val click_sum = click_map.getOrElse(pg1, 0L)
                val rate = sum / click_sum
                ((pg1, pg2), rate)
        }
        val test = res_data.map(i => (i._1._1, (i._1._2, i._2))).collect()
        test.foreach(println)
        */

        val res_data = session_data.reduceByKey(_+_)
        val res = res_data.map(
            iter => {
                val click_sum = click_map.getOrElse(iter._1._1, 0L)
                val rate = iter._2 / click_sum
                ((iter._1._1, iter._1._2), rate)
            }
        )
        res.collect().foreach(println)
        sc.stop()
    }

    case class User_Action(
        // date, user, session_id, page_id, action_time, key_word, click_class_id, click_product_id, order_class_id,
        // order_product_id, pay_class_id, pay_product_id
        date: String,
        user: Long,
        session_id: String,
        page_id: Long,
        action_time: String,
        key_word: String,
        click_class_id: Long,
        click_product_id: Long,
        order_class_id: String,
        order_product_id: String,
        pay_class_id: String,
        pay_product_id: String,
        city_id: Long
    )
}

