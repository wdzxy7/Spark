package practice

import org.apache.spark.{SparkConf, SparkContext}

object user_visit_action_prac {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("Operator")
        val sc = new SparkContext(sparkConf)
        val temp = sc.textFile("./data/spark_core_data/user_visit_action.txt")
        // date, user, session_id, page_id, action_time, key_word, click_class_id, click_product_id, order_class_id,
        // order_product_id, pay_class_id, pay_product_id
        val data = temp.map(
            line => {
                val t = line.split("_")
                (t(6), t(7), t(8), t(9), t(10), t(11))
            }
        )

    }

}
