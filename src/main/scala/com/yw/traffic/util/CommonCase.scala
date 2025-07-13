package com.yw.traffic.util

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer

object GlobalConstants {

  lazy val MONITOR_STATE_DESCRIPTOR = new MapStateDescriptor[String, MonitorInfo]("monitor_info", classOf[String], classOf[MonitorInfo])
  lazy val VIOLATION_STATE_DESCRIPTOR = new MapStateDescriptor[String, ViolationInfo]("violation_info", classOf[String], classOf[ViolationInfo])

  lazy val KAFKA_SOURCE = KafkaSource.builder[String]()
    .setBootstrapServers("node1:9092,node2:9092,node3:9092")
    .setTopics("traffic_topic")
    .setGroupId("traffic_speed_monitor")
    .setStartingOffsets(OffsetsInitializer.latest())
    .setValueOnlyDeserializer(new SimpleStringSchema())
    .build()
}

/**
 * 样例类
 */
// 从Kafka中读取的数据，车辆经过卡口的信息
case class TrafficInfo(actionTime: Long, monitorId: String, cameraId: String, car: String, speed: Double, roadId: String, areaId: String)

// 卡口信息的样例类
case class MonitorInfo(monitorId: String, roadId: String, limitSpeed: Int, areaId: String)

// 车辆超速的信息
case class OutOfLimitSpeedInfo(car: String, monitorId: String, roadId: String, realSpeed: Double, limitSpeed: Int, actionTime: Long)

// 某个时间范围内卡口的平均车速和通过的车辆数量
case class AvgSpeedInfo(start: Long, end: Long, monitorId: String, avgSpeed: Double, carCount: Int)

// 套牌车辆告警信息对象
case class RepetitionCarWarning(car: String, firstMonitor: String, secondMonitor: String, msg: String, action_time: Long)

// 危险驾驶的信息
case class DangerousDrivingWarning(car: String, msg: String, create_time: Long, avgSpeed: Double)

// 违法车辆信息对象
case class ViolationInfo(car: String, msg: String, createTime: Long)

// 车辆轨迹数据样例类
case class TrackInfo(car: String, actionTime: Long, monitorId: String, roadId: String, areaId: String, speed: Double)
