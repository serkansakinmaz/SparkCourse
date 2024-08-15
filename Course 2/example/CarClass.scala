package org.example

class CarClass extends Car {
  val color: String = ""

  def drive(): Unit ={
    println(s"Drive $color car")
  }
}
