package com.ten.aditum.statistics.scalautils

import com.ten.aditum.statistics.conf.ConfigurationManager
import com.ten.aditum.statistics.constants.Constants
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
  * 初始化spark环境工具对象
  *
  * Created by Wanghan on 2017/3/12.
  * Copyright © Wanghan SCU. All Rights Reserved
  */
object InitUnits {

    /**
      * 初始化spark、sql环境
      *
      * @return
      */
    def initSparkContext(): (SparkContext, SQLContext) = {
        // spark配置文件
        val conf = getSparkConf()
        // spark上下文环境
        val sc = new SparkContext(conf)
        // SQL上下文环境
        val sqlContext = getSQLContext(sc)
        // 设置Log等级
        Logger.getRootLogger.setLevel(Level.OFF)

        (sc, sqlContext)
    }

    /**
      * 加载spark配置，如果在本地使用2核，如果在集群，则提交作业时候指定
      *
      * @return
      */
    def getSparkConf(): SparkConf = {
        val local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
        if (local)
            new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION).setMaster(Constants.SPARK_MASTER)
        else
            new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION)
    }

    /**
      * 加载SQL上下环境，如果在本地生成sql环境
      *
      * @param sc
      * @return
      */
    def getSQLContext(sc: SparkContext): SQLContext = {
        val local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
        if (local)
            new SQLContext(sc)
        else
            new HiveContext(sc)
    }
}
