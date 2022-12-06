package practice

import org.apache.spark.{SparkConf, SparkContext}

object user_visit_action_prac {
    def main(args: Array[String]): Unit = {
        solution3()

    }

    def solution1(): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("Operator")
        val sc = new SparkContext(sparkConf)
        val temp_data = sc.textFile("./data/spark_core_data/user_visit_action.txt")
        temp_data.cache()
        // date, user, session_id, page_id, action_time, key_word, click_class_id, click_product_id, order_class_id,
        // order_product_id, pay_class_id, pay_product_id
        val click_data = temp_data.filter(
            line => {
                val t = line.split("_")
                t(6) != "-1"
            }
        )
        val click_res = click_data.map(
            line => {
                val t = line.split("_")
                (t(6), 1)
            }
        ).reduceByKey((i, j) => i + j)

        val oder_data = temp_data.filter(
            line => {
                val t = line.split("_")
                t(8) != "null"
            }
        )
        val oder_res =  oder_data.flatMap(
            line => {
                val t = line.split("_")
                val ord = t(8)
                val ords = ord.split(",")
                ords.map(i => (i, 1))
            }
        ).reduceByKey((i, j) => i + j)

        val pay_data = temp_data.filter(
            line => {
                val t = line.split("_")
                t(10) != "null"
            }
        )
        val pay_res = pay_data.flatMap(
            line => {
                val t = line.split("_")
                val pay = t(10)
                val pays = pay.split(",")
                pays.map(i => (i, 1))
            }
        ).reduceByKey((i, j) => (i + j))
        oder_res.cache()
        click_res.cache()
        pay_res.cache()
        /*
        click_res.collect().foreach(println)
        println("----------------------")
        oder_res.collect().foreach(println)
        println("----------------------")
        pay_res.collect().foreach(println*/
        val all_data = click_res.cogroup(oder_res, pay_res)
        val res_data = all_data.mapValues{
            case (cli_iter, ord_iter, pay_iter) => {
                var cli_count = 0
                if (cli_iter.iterator.hasNext)
                    cli_count = cli_iter.iterator.next()
                var ord_count = 0
                if (ord_iter.iterator.hasNext)
                    ord_count = ord_iter.iterator.next()
                var pay_count = 0
                if (pay_iter.iterator.hasNext)
                    pay_count = pay_iter.iterator.next()
                (cli_count, ord_count, pay_count)
            }
        }
        val sort_res = res_data.sortBy(_._2, false).take(10)
        sort_res.foreach(println)
        //多对多的拆分
        oder_data.flatMap(
            line => {
                val t = line.split("_")
                val order1 = t(8).split(",")
                val order2 = t(9)
                val map_ord = order1.map(i => (i, order2))
                map_ord.flatMap(
                    tup => {
                        val t = tup._2.split(",")
                        t.map(i => (tup._1, i))
                    }
                )
            }
        )
    }

    def solution2(): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("Operator")
        val sc = new SparkContext(sparkConf)
        val temp_data = sc.textFile("./data/spark_core_data/user_visit_action.txt")
        temp_data.cache()
        // date, user, session_id, page_id, action_time, key_word, click_class_id, click_product_id, order_class_id,
        // order_product_id, pay_class_id, pay_product_id
        val click_data = temp_data.filter(
            line => {
                val t = line.split("_")
                t(6) != "-1"
            }
        )
        val click_res = click_data.map(
            line => {
                val t = line.split("_")
                (t(6), (1, 0, 0))
            }
        ).reduceByKey((i, j) => (i._1 + j._1, 0, 0))

        val oder_data = temp_data.filter(
            line => {
                val t = line.split("_")
                t(8) != "null"
            }
        )
        val oder_res =  oder_data.flatMap(
            line => {
                val t = line.split("_")
                val ord = t(8)
                val ords = ord.split(",")
                ords.map(i => (i, (0, 1, 0)))
            }
        ).reduceByKey((i, j) => (0, i._2 + j._2, 0))

        val pay_data = temp_data.filter(
            line => {
                val t = line.split("_")
                t(10) != "null"
            }
        )
        val pay_res = pay_data.flatMap(
            line => {
                val t = line.split("_")
                val pay = t(10)
                val pays = pay.split(",")
                pays.map(i => (i, (0, 0, 1)))
            }
        ).reduceByKey((i, j) => (0, 0, i._3 + j._3))

        val all_data = click_res.union(oder_res).union(pay_res)

        val res_data = all_data.reduceByKey(
            (i, j) => {
                (i._1 + j._1, i._2 + j._2, i._3 + j._3)
            }
        )
        val sort_res = res_data.sortBy(_._2, false).take(10)
        sort_res.foreach(println)
        }

    def solution3(): Unit = {
        val sparkConf = new SparkConf().setMaster("local").setAppName("Operator")
        val sc = new SparkContext(sparkConf)
        val temp_data = sc.textFile("./data/spark_core_data/user_visit_action.txt")
        temp_data.cache()
        val all_data = temp_data.flatMap(
            line => {
                val t = line.split("_")
                if (t(6) != "-1")
                    List((t(6), (1, 0, 0)))
                else if (t(8) != "null") {
                    val id = t(8).split(",")
                    id.map(i => (i, (0, 1, 0)))

                } else if (t(10) != "null") {
                    val id = t(10).split(",")
                    id.map(i => (i, (0, 0, 1)))
                }
                else
                    Nil
            }
        )
        val res_data = all_data.reduceByKey(
            (i, j) => {
                (i._1 + j._1, i._2 + j._2, i._3 + j._3)
            }
        )
        val sort_res = res_data.sortBy(_._2, false).take(10)
        sort_res.foreach(println)
    }
}
