package com.yw.traffic.util

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

/**
 * 从数据库中读取数据，返回的类型可以是各种各样
 */
class JdbcReadDataSource[T](classType: Class[_ <: T]) extends RichSourceFunction[T] {
  var flag = true;
  var conn: Connection = _
  var pst: PreparedStatement = _
  var set: ResultSet = _

  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://node2/traffic_monitor", "root", "123456")
    var sql = "select 1"
    if (classType.getName.equals(classOf[MonitorInfo].getName)) {
      sql = "select monitor_id,road_id,speed_limit,area_id from t_monitor_info where speed_limit > 0"
    }
    if (classType.getName.equals(classOf[ViolationInfo].getName)) {
      sql = " select car ,violation, create_time from t_violation_list"
    }
    pst = conn.prepareStatement(sql)
  }

  override def run(ctx: SourceFunction.SourceContext[T]): Unit = {
    while (flag) {
      set = pst.executeQuery()
      while (set.next()) {
        if (classType.getName.equals(classOf[MonitorInfo].getName)) {
          val info = MonitorInfo(set.getString(1), set.getString(2), set.getInt(3), set.getString(4))
          ctx.collect(info.asInstanceOf[T])
        }
        if (classType.getName.equals(classOf[ViolationInfo].getName)) {
          val info = ViolationInfo(set.getString(1), set.getString(2), set.getLong(3))
          ctx.collect(info.asInstanceOf[T])
        }
      }
      Thread.sleep(2000)
      set.close()
    }
  }

  override def cancel(): Unit = {
    flag = false
  }

  override def close(): Unit = {
    pst.close()
    conn.close()
  }
}
