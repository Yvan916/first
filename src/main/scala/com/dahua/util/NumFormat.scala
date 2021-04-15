package com.dahua.util

object NumFormat {
  def toInt(arg: String):Int={
    try{
        arg.toInt
    }catch {
      case _:Exception=>0
    }
  }

  def toDouble(arg: String):Double={
    try{
      arg.toDouble
    }catch {
      case _:Exception=>0
    }
  }



}
