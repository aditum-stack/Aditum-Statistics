import java.io.{File, InputStreamReader}

import com.sun.imageio.plugins.common.ReaderUtil

object HelloWorld {

  def main(args: Array[String]): Unit = {
    println("Hello World")
    println(hello(3))
  }

  def hello(value: Int): String = {
    s"hello" + value
  }
}


