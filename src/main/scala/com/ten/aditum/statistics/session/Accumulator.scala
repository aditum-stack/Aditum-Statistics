package com.ten.aditum.statistics.session

import com.ten.aditum.statistics.spark.constants.Constants
import com.ten.aditum.statistics.spark.javautils.StringUtils
import org.apache.spark.AccumulatorParam

object Accumulator extends AccumulatorParam[String] {
  private val INITIAL_VALUE: String = Constants.SESSION_COUNT + "=0" + Constants.VALUE_SEPARATOR +
    Constants.TIME_PERIOD_1s_3s + "=0" + Constants.VALUE_SEPARATOR +
    Constants.TIME_PERIOD_4s_6s + "=0" + Constants.VALUE_SEPARATOR +
    Constants.TIME_PERIOD_7s_9s + "=0" + Constants.VALUE_SEPARATOR +
    Constants.TIME_PERIOD_10s_30s + "=0" + Constants.VALUE_SEPARATOR +
    Constants.TIME_PERIOD_30s_60s + "=0" + Constants.VALUE_SEPARATOR +
    Constants.TIME_PERIOD_1m_3m + "=0" + Constants.VALUE_SEPARATOR +
    Constants.TIME_PERIOD_3m_10m + "=0" + Constants.VALUE_SEPARATOR +
    Constants.TIME_PERIOD_10m_30m + "=0" + Constants.VALUE_SEPARATOR +
    Constants.TIME_PERIOD_30m + "=0" + Constants.VALUE_SEPARATOR +
    Constants.STEP_PERIOD_1_3 + "=0" + Constants.VALUE_SEPARATOR +
    Constants.STEP_PERIOD_4_6 + "=0" + Constants.VALUE_SEPARATOR +
    Constants.STEP_PERIOD_7_9 + "=0" + Constants.VALUE_SEPARATOR +
    Constants.STEP_PERIOD_10_30 + "=0" + Constants.VALUE_SEPARATOR +
    Constants.STEP_PERIOD_30_60 + "=0" + Constants.VALUE_SEPARATOR +
    Constants.STEP_PERIOD_60 + "=0"

  override def zero(initialValue: String): String = {
    INITIAL_VALUE
  }

  override def addInPlace(r1: String, r2: String): String = {
    if (r1 == "")
      r2
    else {
      try {
        //累加1
        val oldValue = StringUtils.getFieldFromConcatString(r1, Constants.REGULAR_VALUE_SEPARATOR, r2)
        val newValue = oldValue.toInt + 1
        //更新连接串
        StringUtils.setFieldInConcatString(r1, Constants.REGULAR_VALUE_SEPARATOR, r2, newValue.toString)
      } catch {
        case e: Exception => r2
      }
    }
  }

}
