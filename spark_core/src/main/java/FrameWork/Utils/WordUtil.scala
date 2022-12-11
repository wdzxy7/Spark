package FrameWork.Utils

import org.apache.spark.SparkContext

object WordUtil {

    private val scLocal = new ThreadLocal[SparkContext]()

    def put (sc: SparkContext): Unit = {
        scLocal.set(sc)
    }

    def get (): SparkContext = {
        scLocal.get()
    }

    def clear(): Unit = {
        scLocal.remove()
    }
}
