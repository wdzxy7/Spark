package practice

import org.apache.spark.{SparkConf, SparkContext}

object group_prac {
    def main(args: Array[String]): Unit = {
        val spark_conf = new SparkConf().setMaster("local").setAppName("RDD")
        val sc = new SparkContext(spark_conf)
        val list = List(("连衣裙",1234, 22, 13),("牛仔裤",2768, 34, 7),("连衣裙",1673,45, 9)
            ,("衬衣",3468,67, 12),("牛仔裤",2754, 68, 20),("连衣裙",1976,93, 29))
        val data = sc.makeRDD(list)
        val rdd1 = data.groupBy(_._1)
        val res = rdd1.map(
            data => {
                val list = data._2.toList
                val sort_list1 = list.sortBy(x => (x._3, x._4))(Ordering.Tuple2(Ordering[Int].reverse, Ordering[Int])).take(2)
                sort_list1
            }
        )
        res.collect().foreach(println)
        sc.stop()
    }

}
