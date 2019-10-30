package com.longi.group.common

import com.longi.group.util.CommonUtil
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object AggregateData {

  val log = Logger.getLogger(AggregateData.getClass)

  def polymerizationData(groupRDD:RDD[Row],gearStr1:String,gearStr2:String,gearName:String) = groupRDD
    .filter(ro => ro.getString(6).equals(gearStr1) || ro.getString(6).equals(gearStr2))
    .groupBy((rs: Row) => (rs.getString(2),rs.getString(3),rs.getLong(1),rs.getString(4),rs.getString(5),rs.getString(7))).map(gp => {
    val factoryName = gp._1._1
    val machineName = gp._1._2
    val timeGroupName = gp._1._3
    val classNumber = gp._1._4
    val productType = gp._1._5
    val productModel = gp._1._6

    val groupRows: Iterable[Row] = gp._2
    val groupCount: Int = groupRows.size
    val timeBegin = groupRows.map(_.getString(0)).min
    val timeEnd = groupRows.map(_.getString(0)).max

    val currentYear: String = CommonUtil.getHourMinuteByStr("yyyy-MM-dd", timeBegin)
    val beginTime: String = CommonUtil.getHourMinuteByStr("HH:mm:ss", timeBegin)
    val endTime: String = CommonUtil.getHourMinuteByStr("HH:mm:ss", timeEnd)

    val mGroup = groupRows.map(row => (row.getDecimal(11).doubleValue(),row.getDecimal(12).doubleValue(),row.getDecimal(13).doubleValue(),row.getDecimal(14).doubleValue(),
        row.getDecimal(15).doubleValue(),row.getDecimal(16).doubleValue(),row.getDecimal(17).doubleValue(),row.getDecimal(18).doubleValue()
      )).toSeq

    val QTY = groupCount
    val ETA = mGroup.map(_._1).sum / groupCount
    val UOC = mGroup.map(_._2).sum / groupCount
    val ISC = mGroup.map(_._3).sum / groupCount
    val FF = mGroup.map(_._4).sum / groupCount
    val RS = mGroup.map(_._5).sum / groupCount
    val RSH = mGroup.map(_._6).sum / groupCount
    val IREV2 = mGroup.map(_._7).sum / groupCount
    val TCELL = mGroup.map(_._8).sum / groupCount

    log.info("out==="+factoryName,machineName,classNumber,productType,productModel,QTY,ETA,UOC,ISC,FF,RS,RSH,IREV2,TCELL,timeBegin,currentYear,beginTime,endTime)
    /* val levelRate = groupRows.groupBy((rd: Row) => rd.getString(6)).map((re: (String, Iterable[Row])) => {
       val level = re._1
       val rate = re._2.size.toFloat / groupCount
       (level,rate)
     } )*/
    (factoryName,machineName,classNumber,productType,productModel,gearName,timeBegin,QTY,ETA,UOC,ISC,FF,RS,RSH,IREV2,TCELL,currentYear,beginTime,endTime)

  })

}
