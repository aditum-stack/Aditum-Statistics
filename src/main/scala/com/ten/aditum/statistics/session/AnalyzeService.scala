package com.ten.aditum.statistics.session

import java.text.SimpleDateFormat
import java.util.Date

import com.ten.aditum.statistics.scalautils.{AnalyzeHelperUnits, InitUnits, SparkUtils}
import com.ten.aditum.statistics.spark.constants.Constants
import com.ten.aditum.statistics.spark.dao.factory.DAOFactory
import com.ten.aditum.statistics.spark.exception.TaskException
import com.ten.aditum.statistics.spark.javautils.{ParamUtils, StringUtils}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.json.JSONObject


/**
  * 用户访问分析类
  */
object AnalyzeService {

  def main(args: Array[String]): Unit = {
    val context = InitUnits.initSparkContext()
    val sc = context._1
    val sQLContext = context._2

    SparkUtils.loadLocalTestDataToTmpTable(sc, sQLContext)

    val taskDao = DAOFactory.getTaskDAO

    val taskId = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_SESSION_TASKID).longValue()
    val task = if (taskId > 0) taskDao.findById(taskId) else null

    if (task == null) {
      throw new TaskException("Can't find task by id: " + taskId)
    }

    val taskParam = new JSONObject(task.getTaskParam)
    println(taskParam)

    // val param1 = new JSONObject("{\"startDate\":[\"2017-03-06\"],\"endDate\":[\"2017-03-06\"],\"startAge\":[\"40\"],\"endAge\":[\"42\"],\"citys\":[\"city14\"],\"searchWords\":[\"小米5\"]}")

    try {
      // 执行需求
      taskId match {
        /**
          * 需求2
          */
        case 2L => {
          val sql = AnalyzeHelperUnits.getSQL(taskParam)
          val aggUserVisitAction = sQLContext.sql(sql._2).rdd
          val aggUserInfo = sQLContext.sql(sql._1).rdd
          val actionRddByDateRange = displaySession(aggUserInfo, aggUserVisitAction)
          print(actionRddByDateRange.collect().toBuffer)
        }

        /**
          * 需求3session时间范围是必选的。返回的结果RDD元素格式同上（Spark RDD + Sql）
          */
        case 3L => {
          val actionRddByRequirement = sessionAggregateByRequirement(sQLContext, taskParam).collect().toBuffer
          println(actionRddByRequirement)
        }

        /** 需求4
          */
        case 4L => {
          val res = getVisitLengthAndStepLength(sc, sQLContext, taskParam)
          println(res)
        }

        /** 需求5
          */
        case 5L => {
          val session = getSessionByRequirement(sQLContext, taskParam)
          val hotProducts = getHotCategory(session).iterator
          for (i <- hotProducts)
            print(i)
        }
      }
    } catch {
      case e: Exception => println("没有匹配的需求编号")
    }

    sc.stop()
  }

  def displaySession(aggUserInfo: RDD[Row], aggUserVisitAction: RDD[Row]): RDD[(String, String)] = {
    // sessionidRddWithAction 形为(session_id,RDD[Row])
    val sessionIdRddWithAction = aggUserVisitAction.map(tuple => (tuple.getString(2), tuple)).groupByKey()
    // userIdRddWithSearchWordsAndClickCategoryIds 形为(user_id,session_id|searchWords|clickCategoryIds)
    val userIdRddWithSearchWordsAndClickCategoryIds = sessionIdRddWithAction.map(f = s => {
      val session_id: String = s._1
      // 用户ID
      var user_id: Long = 0L
      // 搜索关键字的集合
      var searchWords: String = ""
      // 点击分类ID的集合
      var clickCategoryIds: String = ""
      var startTime: Date = null
      var endTime: Date = null
      var stepLength = 0

      val iterator = s._2.iterator
      while (iterator.hasNext) {
        val row = iterator.next()
        user_id = row.getLong(1)
        val searchWord = row.getString(6).trim
        val clickCategoryId = row.getString(7).trim
        if (searchWord != "null" && !searchWords.contains(searchWord)) {
          searchWords += (searchWord + ",")
        }
        if (clickCategoryId != "null" && !clickCategoryIds.contains(clickCategoryId)) {
          clickCategoryIds += (clickCategoryId + ",")
        }
        stepLength += 1

        val TIME_FORMAT: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val actionTime = TIME_FORMAT.parse(row.getString(4) + " " + row.getString(5))

        if (startTime == null && endTime == null) {
          startTime = actionTime
          endTime = actionTime
        } else if (actionTime.before(startTime)) {
          startTime = actionTime
        } else if (actionTime.after(endTime)) {
          endTime = actionTime
        }
      }
      // 访问时常
      val visitLength = (endTime.getTime - startTime.getTime) / 1000
      //val visitLength = 0
      searchWords = StringUtils.trimComma(searchWords)
      clickCategoryIds = StringUtils.trimComma(clickCategoryIds)
      val userAggregateInfo = Constants.FIELD_SESSION_ID + "=" + session_id + Constants.VALUE_SEPARATOR +
        Constants.FIELD_SEARCH_KEYWORDS + "=" + searchWords + Constants.VALUE_SEPARATOR +
        Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + Constants.VALUE_SEPARATOR +
        Constants.FIELD_VISIT_LENGTH + "=" + visitLength + Constants.VALUE_SEPARATOR +
        Constants.FIELD_STEP_LENGTH + "=" + stepLength
      (user_id, userAggregateInfo)
    })

    // userInfo形如(user_id,RDD[Row])
    val userInfo = aggUserInfo.map(tuple => (tuple.getLong(0), tuple))

    val userWithSessionInfoRdd = userInfo.join(userIdRddWithSearchWordsAndClickCategoryIds)

    userWithSessionInfoRdd.map(t => {
      val userAggregateInfo = t._2._2
      val userInfo = t._2._1
      val session_id = StringUtils.getFieldFromConcatString(userAggregateInfo, Constants.REGULAR_VALUE_SEPARATOR,
        Constants.FIELD_SESSION_ID)
      val age = userInfo.getInt(3)
      val professional = userInfo.getString(4)
      val city = userInfo.getString(5)
      val sex = userInfo.getString(6)

      val aggregateInfo = userAggregateInfo + Constants.VALUE_SEPARATOR +
        Constants.FIELD_AGE + "=" + age + Constants.VALUE_SEPARATOR +
        Constants.FIELD_PROFESSIONAL + "=" + professional + Constants.VALUE_SEPARATOR +
        Constants.FIELD_CITY + "=" + city + Constants.VALUE_SEPARATOR +
        Constants.FIELD_SEX + "=" + sex
      (session_id, aggregateInfo)
    })
  }

  def sessionAggregateByRequirement(sQLContext: SQLContext, json: JSONObject): RDD[(String, String)] = {
    val sql = AnalyzeHelperUnits.getSQL(json)
    val sqlUserInfo = sql._1
    val sqlUserVisitAction = sql._2
    val aggUserInfo = sQLContext.sql(sqlUserInfo).rdd
    // 形如(sessionId,Row)
    val partialVisitAction = sQLContext.sql(sqlUserVisitAction).rdd.map(t => (t.getString(2), t))
    val fullVisitAction = AnalyzeHelperUnits.getFullSession(sQLContext).map(t => (t.getString(2), t))
    val aggUserVisitAction = partialVisitAction.join(fullVisitAction).map(t => t._2._2)
    displaySession(aggUserInfo, aggUserVisitAction)
  }

  def getVisitLengthAndStepLength(sc: SparkContext, sQLContext: SQLContext, json: JSONObject): String = {

    val sessionAggAccumulator = sc.accumulator("")(Accumulator)
    // 获取session聚合信息
    val rdd = sessionAggregateByRequirement(sQLContext, json)
    rdd.foreach(tuple => {
      val currentInfo = tuple._2
      // 获取访问时长
      val visitLength = StringUtils.getFieldFromConcatString(currentInfo, Constants.REGULAR_VALUE_SEPARATOR, Constants.FIELD_VISIT_LENGTH).toLong
      // 获取访问步长
      val stepLength = StringUtils.getFieldFromConcatString(currentInfo, Constants.REGULAR_VALUE_SEPARATOR, Constants.FIELD_STEP_LENGTH).toLong

      sessionAggAccumulator.add(Constants.SESSION_COUNT)
      // 根据访问时长，更新累加器
      if (visitLength >= 1 && visitLength <= 3)
        sessionAggAccumulator.add(Constants.TIME_PERIOD_1s_3s)
      else if (visitLength >= 4 && visitLength <= 6)
        sessionAggAccumulator.add(Constants.TIME_PERIOD_4s_6s)
      else if (visitLength >= 7 && visitLength <= 9)
        sessionAggAccumulator.add(Constants.TIME_PERIOD_7s_9s)
      else if (visitLength >= 10 && visitLength <= 30)
        sessionAggAccumulator.add(Constants.TIME_PERIOD_10s_30s)
      else if (visitLength >= 31 && visitLength <= 60)
        sessionAggAccumulator.add(Constants.TIME_PERIOD_30s_60s)
      else if (visitLength >= 61 && visitLength <= 180)
        sessionAggAccumulator.add(Constants.TIME_PERIOD_1m_3m)
      else if (visitLength >= 181 && visitLength <= 600)
        sessionAggAccumulator.add(Constants.TIME_PERIOD_3m_10m)
      else if (visitLength >= 601 && visitLength <= 1800)
        sessionAggAccumulator.add(Constants.TIME_PERIOD_10m_30m)
      else if (visitLength >= 1801)
        sessionAggAccumulator.add(Constants.TIME_PERIOD_30m)

      // 根据访问步长，更新累加器
      if (stepLength >= 1 && stepLength <= 3)
        sessionAggAccumulator.add(Constants.STEP_PERIOD_1_3)
      else if (stepLength >= 4 && stepLength >= 6)
        sessionAggAccumulator.add(Constants.STEP_PERIOD_4_6)
      else if (stepLength >= 7 && stepLength <= 9)
        sessionAggAccumulator.add(Constants.STEP_PERIOD_7_9)
      else if (stepLength >= 10 && stepLength <= 30)
        sessionAggAccumulator.add(Constants.STEP_PERIOD_10_30)
      else if (stepLength >= 31 && stepLength <= 60)
        sessionAggAccumulator.add(Constants.STEP_PERIOD_30_60)
      else if (stepLength >= 61)
        sessionAggAccumulator.add(Constants.STEP_PERIOD_60)

    })
    sessionAggAccumulator.value
  }

  def getSessionByRequirement(sQLContext: SQLContext, jSONObject: JSONObject): RDD[Row] = {
    val sql = AnalyzeHelperUnits.getSQL(jSONObject)
    val sqlUserInfo = sql._1
    val sqlUserVisitAction = sql._2
    // 形如(user_id,RDD)
    val aggUserInfo = sQLContext.sql(sqlUserInfo).rdd.map(t => (t.getLong(0), t))
    // 形如(sessionId,Row)
    val partialVisitAction = sQLContext.sql(sqlUserVisitAction).rdd.map(t => (t.getString(2), t))
    // 形如(sessionId,Row)
    val fullVisitAction = AnalyzeHelperUnits.getFullSession(sQLContext).map(t => (t.getString(2), t))
    // 形如(user_id,RDD)
    val aggUserVisitAction = partialVisitAction.join(fullVisitAction).map(t => (t._2._2.getLong(1), t._2._2))
    aggUserInfo.join(aggUserVisitAction).map(t => {
      t._2._2
    })
  }

  def getHotCategory(rDD: RDD[Row]): Array[(String, Int, Int, Int)] = {

    class SessionPair(category_id: String, click_times: Int, order_times: Int, pay_times: Int) extends Ordered[SessionPair] with Serializable {
      val categoryId = category_id
      val click = click_times
      val order = order_times
      val pay = pay_times

      override def compare(that: SessionPair): Int = {
        if (this.click == that.click) {
          if (this.order == that.order) {
            return this.click - that.click
          }
          return this.order - that.order
        }
        return this.click - that.click
      }
    }

    def getCategoryIdAndTimes(index: Int, rDD: RDD[Row]): RDD[(String, Int)] = {
      // 类聚
      val aggByIndex = rDD.groupBy(x => x.get(index))
      // 统计每一品类的数量
      val count = aggByIndex.filter(x => !x._1.equals("null")).map(x => (x._1.toString, x._2.size))
      count
    }

    val rdd_click = getCategoryIdAndTimes(7, rDD)
    val rdd_order = getCategoryIdAndTimes(9, rDD)
    val rdd_pay = getCategoryIdAndTimes(11, rDD)
    val fullRdd = rdd_click.fullOuterJoin(rdd_order).fullOuterJoin(rdd_pay)
    val sortedFullRdd = fullRdd.map(tuple => {
      val categoryId: String = tuple._1
      val click: Int = if (tuple._2._1.isEmpty) 0 else if (tuple._2._1.get._1.isEmpty) 0 else tuple._2._1.get._1.get
      val order: Int = if (tuple._2._1.isEmpty) 0 else if (tuple._2._1.get._2.isEmpty) 0 else tuple._2._1.get._2.get
      val pay: Int = if (tuple._2._2.isEmpty) 0 else tuple._2._2.get

      (new SessionPair(categoryId, click, order, pay), tuple)
    }).sortByKey(false)
    sortedFullRdd.map(tuple => (tuple._1.categoryId, tuple._1.click, tuple._1.order, tuple._1.pay)).take(10)
  }


}
