object HelloWorld {
  def main(args: Array[String]) {
    println("Hello World")
    hello(3)
  }

  def hello(value: Int) {
     s"hello"+value
  }
}