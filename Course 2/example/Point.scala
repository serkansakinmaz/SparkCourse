package org.example

class Point (var x: Int, var y: Int) {
  def move(dx: Int, dy: Int): Unit = {
    x += dx
    y += dy
    println(s"$x $y")
  }
}
