package org.example

import java.io.FileReader

object FileRead {

  def main(args: Array[String]): Unit = {

    try{
      val n = new FileReader("/Users/serkan/Desktop/Training3/data/foo").read()
      println(s"Success : $n")
    }catch {
      case e: Exception=>
        println("HATA")
        e.printStackTrace()
    }





  }

}
