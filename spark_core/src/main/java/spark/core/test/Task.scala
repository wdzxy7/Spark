package spark.core.test

class Task extends Serializable {
    val datas = List(1, 2, 3, 4)
    val logic = (num: Int) => {num * 2}
    def compute(): List[Int] = {
        datas.map(logic)
    }
}
