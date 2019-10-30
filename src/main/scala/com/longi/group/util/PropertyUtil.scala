package com.longi.group.util

import java.util.Properties

object PropertyUtil {
  /**
    * 借贷模式打开文件
    *
    * @param resource
    * @param f
    * @tparam A
    * @tparam B
    * @return
    */
  def using[A <: {def close() : Unit}, B](resource: A)(f: A => B): B =
    try {
      f(resource)
    }
    finally {
      if (resource != null)
        resource.close()
    }

  /**
    * properties文件转换为map
    *
    * @return
    */
  def getProp(path:String): Properties = {
    val prop = new Properties()

    using(this.getClass.getClassLoader.getResourceAsStream(path)) {
      source => {
        prop.load(source)
        prop
      }
    }
  }
}
