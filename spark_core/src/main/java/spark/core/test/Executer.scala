package spark.core.test

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

object Executer {
    def main(args: Array[String]): Unit = {
        val server = new ServerSocket(9999)

        val client: Socket = server.accept()
        val get: InputStream = client.getInputStream
        val objIn = new ObjectInputStream(get)
        val task: Task = objIn.readObject().asInstanceOf[Task]
        val ints: List[Int] = task.compute()
        println(ints)
        objIn.close()
        client.close()
        server.close()
    }
}
