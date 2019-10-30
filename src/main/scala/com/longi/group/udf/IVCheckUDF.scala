package com.longi.group.udf

import java.text.SimpleDateFormat

object IVCheckUDF {
  def timeGrading(testTime:String) ={
    val newTime :Long= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(testTime).getTime
    val newTimes = newTime/1000
    newTimes/300
  }

}
