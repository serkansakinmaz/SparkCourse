package org.example

object Hello1 {


  def main(args: Array[String]): Unit = {

    println(matchA(2))

  }

  def matchA(i: Int): String = {

    i match {
      case 1 => return "one"
      case 2 => return "two"
      case _ => return "else"
    }

  }

  def add(x:Int, y:Int): Int = {
    val count = 3
    x + y + count
  }

  def variablesArguments(args: Int*): Int = {
    var n = 0
    for (arg <- args) {
      n += arg
    }
    n
  }

  def addThreeNumber(first: Int, second: Int, third: Int): Int = {
    def addTwoNumber(second: Int, third: Int): Int = {
      second + third
    }
    first + addTwoNumber(second,third)
  }

  def printVal() = {
    println("Print without value")
  }

  def printVal(i: Int) = {
    println("Print with value : " + i)
  }

}
