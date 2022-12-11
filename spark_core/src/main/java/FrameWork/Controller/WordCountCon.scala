package FrameWork.Controller

import FrameWork.Service.WordCountSer
import FrameWork.Common.WordConT

/*
调用service函数进行数据操作
 */
class WordCountCon extends WordConT{
    private val wordCountSer = new  WordCountSer()
    //实例化抽象方法
    def execute(): Unit = {
        val array = wordCountSer.dataAnalysis()
        array.foreach(println)
    }

}
