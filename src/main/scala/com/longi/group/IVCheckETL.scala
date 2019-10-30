package com.longi.group

import com.longi.group.common.AggregateData
import com.longi.group.udf.IVCheckUDF
import com.longi.group.util.{OracleUtil, SparkSessionCreater}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._

object IVCheckETL {

  case class IVTestEfficiencyGear(factory_id: String, machine_id: String, class_number: String,product_type: String,product_model: String,product_level: String,test_time_date: String,qty: Int,eta: Double,uoc: Double,isc: Double,ff: Double,rs: Double, rsh: Double, irev2: Double, tcell: Double,currentYear:String,beginTime:String,endTime:String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSessionCreater.getSparkSession
    import spark.implicits._
    spark.udf.register("timeGroups",IVCheckUDF.timeGrading _)

    //第二天凌晨处理前一天数据根据A，B1+B3 分2次计算
    val querySql =
      """
        |SELECT FACTORY_ID,MACHINE_ID,CLASS_NUMBER,PRODUCT_TYPE,PRODUCT_LEVEL,PRODUCT_MODEL,BATCH_ID,
        |to_char(TEST_TIME_DATE,'yyyy-MM-dd hh24:mi:ss') as TEST_TIME_DATE, BIN,BIN_COMMENT,ETA,UOC,ISC,FF,
        |RS,RSH,IREV2,ORQ,OFQ,TCELL,INSOL
        | FROM IV_TEST_INFO
        | WHERE trunc(TEST_TIME_DATE) = trunc(sysdate - 1)
        | AND ETA > (SELECT INDEX_VALUE FROM IV_INDEX_THRESHOLD T WHERE T.INDEX_ID = 'ETA')
      """.stripMargin
    val ivCheckDetailDF = OracleUtil.getFromOracle(spark,querySql)
    val groupDetailRDD: RDD[Row] = ivCheckDetailDF.selectExpr("TEST_TIME_DATE","timeGroups(TEST_TIME_DATE) as tmGroup","FACTORY_ID","MACHINE_ID","CLASS_NUMBER","PRODUCT_TYPE",
      "UPPER(PRODUCT_LEVEL) as PRODUCT_LEVEL","UPPER(PRODUCT_MODEL) as PRODUCT_MODEL","BATCH_ID","BIN","BIN_COMMENT","ETA","UOC","ISC","FF","RS","RSH","IREV2","TCELL")
      .filter("PRODUCT_LEVEL IN('A','B1','B3')")
      .orderBy("TEST_TIME_DATE")
      .rdd

    val aGroupRow = AggregateData.polymerizationData(groupDetailRDD,"A","EVT","A")
    val bGroupRow = AggregateData.polymerizationData(groupDetailRDD,"B1","B3","B1-B3")

    val aDataDF = aGroupRow.map(tuple => IVTestEfficiencyGear(tuple._1,tuple._2,tuple._3,tuple._4,tuple._5,tuple._6,tuple._7,tuple._8,tuple._9,tuple._10,tuple._11,tuple._12,tuple._13,tuple._14,tuple._15,tuple._16,tuple._17,tuple._18,tuple._19))
      .toDF("factory_id","machine_id","class_number","product_type","product_model","product_level","test_time_date","qty","eta","uoc","isc","ff","rs","rsh","irev2","tcell","begin_date","begin_time","end_time")

    val bDataDF = bGroupRow.map(tuple => IVTestEfficiencyGear(tuple._1,tuple._2,tuple._3,tuple._4,tuple._5,tuple._6,tuple._7,tuple._8,tuple._9,tuple._10,tuple._11,tuple._12,tuple._13,tuple._14,tuple._15,tuple._16,tuple._17,tuple._18,tuple._19))
      .toDF("factory_id","machine_id","class_number","product_type","product_model","product_level","test_time_date","qty","eta","uoc","isc","ff","rs","rsh","irev2","tcell","begin_date","begin_time","end_time")

    bGroupRow.take(1000).foreach(println)

    val allDataDF = aDataDF.union(bDataDF)
    OracleUtil.saveOracle(spark,allDataDF,"IV_TEST_EFFICIENCY_GEAR")
    //OracleUtil.saveOracle(spark,bDataDF,"IV_TEST_EFFICIENCY_GEAR")



  }

}
