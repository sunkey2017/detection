package com.longi.group.util

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

object OracleUtil {

  val dbProperties = PropertiesUtil.loadProperties("oracle")


  val dbMap = Map("url" -> dbProperties.get("url").toString,
    "user" -> dbProperties.get("user").toString,
    "password" -> dbProperties.get("password").toString,
    "driver" -> dbProperties.get("driver").toString)

  def getFromOracle(spark:SparkSession,sqlStr: String)  = {
    val sqlStrRes = s"($sqlStr) t" //格式上需要传入一个子句，必须这么写
    spark.read.format("jdbc")
      .options(dbMap)
      .option("dbtable", sqlStrRes)
      .load()
  }

  def saveOracle(spark:SparkSession,dataFrame:DataFrame,tableName:String): Unit = {
    //df.write.mode(saveMode).jdbc(oracleUrl, tableName, prop)
    dataFrame.write.mode(SaveMode.Append).jdbc(dbProperties.get("url").toString,tableName,dbProperties)
  }

}
