package com.longi.group.util

import java.io.FileInputStream
import java.util.Properties

object PropertiesUtil {

  def loadProperties(dbName:String):Properties = {
    //val properties = new Properties()
    val returnProp = new Properties()
    //val path = Thread.currentThread().getContextClassLoader.getResource("detection.properties").getPath //文件要放到resource文件夹下
    //properties.load(new FileInputStream(path))
    val properties = PropertyUtil.getProp("detection.properties")
    if(dbName == "oracle"){
      returnProp.setProperty("driver", properties.get("ora.driver").toString)
      returnProp.setProperty("url", properties.get("ora.url").toString)
      returnProp.setProperty("user", properties.get("ora.user").toString)
      returnProp.setProperty("password", properties.get("ora.password").toString)
    }else if(dbName == "mysql"){
      returnProp.setProperty("driver", properties.get("mysql.driver").toString)
      returnProp.setProperty("url", properties.get("mysql.url").toString)
      returnProp.setProperty("user", properties.get("mysql.user").toString)
      returnProp.setProperty("password", properties.get("mysql.password").toString)
    }
    returnProp
  }

}
