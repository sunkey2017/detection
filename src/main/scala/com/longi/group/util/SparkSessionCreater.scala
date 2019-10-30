package com.longi.group.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSessionCreater {
   def getSparkSession: SparkSession = {
     var spark: SparkSession = null
     val conf = new SparkConf()
       .set("spark.driver.extraJavaOptions", "-Dfile.encoding=utf-8")
       .set("spark.executor.extraJavaOptions", "-Dfile.encoding=UTF-8 -Dsun.jnu.encoding=UTF-8")
       if(null == spark){
         spark = SparkSession.builder()
           .appName("ivcheck")
           .master("local[6]")
           .config(conf)
           //.enableHiveSupport()
           .getOrCreate()
       }
       spark
   }

}
