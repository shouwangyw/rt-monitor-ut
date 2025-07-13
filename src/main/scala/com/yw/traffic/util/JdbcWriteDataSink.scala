package com.yw.traffic.util

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

class JdbcWriteDataSink[T](classType: Class[_ <: T]) extends RichSinkFunction[T] {
  var conn: Connection = _
  var pst: PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://node2/traffic_monitor", "root", "123456")
    var sql = "select 1"
    if (classType == classOf[OutOfLimitSpeedInfo]) {
      sql = "insert into t_speeding_info (car,monitor_id,road_id,real_speed,limit_speed,action_time) values (?,?,?,?,?,?)"
    }
    if (classType == classOf[AvgSpeedInfo]) {
      sql = "insert into t_average_speed (start_time,end_time,monitor_id,avg_speed,car_count) values (?,?,?,?,?)"
    }
    if (classType == classOf[RepetitionCarWarning] || classType == classOf[DangerousDrivingWarning]) {
      sql = "insert into t_violation_list (car,violation,create_time) values (?,?,?)"
    }
    if (classType == classOf[TrackInfo]) {
      sql = "insert into t_track_info (car,action_time,monitor_id,road_id,area_id,speed) values (?,?,?,?,?,?)"
    }
    pst = conn.prepareStatement(sql)
  }

  override def close(): Unit = {
    pst.close()
    conn.close()
  }

//  override def invoke(value: T, context: SinkFunction.Context[_]): Unit = {
  override def invoke(value: T, context: SinkFunction.Context): Unit = {
    if (classType == classOf[OutOfLimitSpeedInfo]) {
      val info = value.asInstanceOf[OutOfLimitSpeedInfo]
      pst.setString(1, info.car)
      pst.setString(2, info.monitorId)
      pst.setString(3, info.roadId)
      pst.setDouble(4, info.realSpeed)
      pst.setInt(5, info.limitSpeed)
      pst.setLong(6, info.actionTime)
      pst.executeUpdate()
    }
    if (classType == classOf[AvgSpeedInfo]) {
      val info = value.asInstanceOf[AvgSpeedInfo]
      pst.setLong(1, info.start)
      pst.setLong(2, info.end)
      pst.setString(3, info.monitorId)
      pst.setDouble(4, info.avgSpeed)
      pst.setInt(5, info.carCount)
      pst.executeUpdate()
    }
    if (classType == classOf[RepetitionCarWarning] || classType == classOf[DangerousDrivingWarning]) {
      var actionTime = 0L
      var car = ""
      var msg = ""
      if (classType == classOf[RepetitionCarWarning]) {
        val warning = value.asInstanceOf[RepetitionCarWarning]
        actionTime = warning.action_time
        car = warning.car
        msg = warning.msg
      } else {
        val warning = value.asInstanceOf[DangerousDrivingWarning]
        actionTime = warning.create_time
        car = warning.car
        msg = warning.msg
      }
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val day: Date = sdf.parse(sdf.format(new Date(actionTime))) //当前的0时0分0秒
      //在一天中，每个车牌只写入一次
      val selectPst = conn.prepareStatement("select count(1) from t_violation_list where car=? and create_time between ? and ?")
      selectPst.setString(1, car)
      selectPst.setLong(2, day.getTime)
      selectPst.setLong(3, day.getTime + (24 * 60 * 60 * 1000))
      val selectSet: ResultSet = selectPst.executeQuery()
      if (selectSet.next() && selectSet.getInt(1) == 0) { //当天没有该车牌号,插入
        pst.setString(1, car)
        pst.setString(2, msg)
        pst.setLong(3, actionTime)
        pst.executeUpdate()
      }
      selectSet.close()
      selectPst.close()
    }
    if (classType == classOf[TrackInfo]) {
      val info = value.asInstanceOf[TrackInfo]
      pst.setString(1, info.car)
      pst.setLong(2, info.actionTime)
      pst.setString(3, info.monitorId)
      pst.setString(4, info.roadId)
      pst.setString(5, info.areaId)
      pst.setDouble(6, info.speed)
      pst.executeUpdate()
    }
  }
}