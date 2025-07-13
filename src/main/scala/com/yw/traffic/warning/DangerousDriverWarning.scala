package com.yw.traffic.warning

import com.yw.traffic.util.GlobalConstants.KAFKA_SOURCE
import com.yw.traffic.util.{DangerousDrivingWarning, JdbcWriteDataSink, MonitorInfo, OutOfLimitSpeedInfo, TrafficInfo}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

import java.sql.DriverManager
import java.time.Duration
import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * 实时危险驾驶分析
 * 规定：如果一辆机动车在 2 分钟内，超速通过卡口超过 3 次以上；
 * 而且每次超速的超过了规定速度的 20%以上；这样的机动车涉嫌危险驾驶。
 * 系统需要实时找出这些机动车，并报警，追踪这些车辆的轨迹。
 * 注意：如果有些卡口没有设置限速值，可以设置一个城市默认限速。
 *
 * @author yangwei
 */
object DangerousDriverWarning {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.fromSource(KAFKA_SOURCE, WatermarkStrategy.noWatermarks(), "KafkaSource")
      .map(line => {
//        println(s"$line")
        val slices = line.split(",")
        TrafficInfo(slices(0).toLong, slices(1), slices(2), slices(3), slices(4).toDouble, slices(5), slices(6))
      }).assignTimestampsAndWatermarks(
      // 设置Watermark
      WatermarkStrategy.forBoundedOutOfOrderness[TrafficInfo](Duration.ofSeconds(5))
        .withTimestampAssigner(new SerializableTimestampAssigner[TrafficInfo] {
          override def extractTimestamp(t: TrafficInfo, l: Long): Long = t.actionTime
        }).withIdleness(Duration.ofSeconds(5))
    )

    // 定义Pattern
    val pattern = Pattern.begin[OutOfLimitSpeedInfo]("first")
      .where(t => {
        // 超速20%
        t.realSpeed >= t.limitSpeed * 1.2
      }).timesOrMore(3) // 超过三次以上
      .greedy // 尽可能多的匹配次数
      .within(Time.minutes(2)) // 2分钟以内

    // 没有设置限速的卡口默认是限速是 80
    val outOfSpeedStream = stream.map(new TrafficOutOfSpeedFunction(80))

    CEP.pattern(outOfSpeedStream.keyBy(_.car), pattern)
      .select(new PatternSelectFunction[OutOfLimitSpeedInfo, DangerousDrivingWarning] {
        override def select(map: util.Map[String, util.List[OutOfLimitSpeedInfo]]): DangerousDrivingWarning = {
          val list = map.get("first")
          val first = list.get(0)
          val msg = "涉嫌危险驾驶"
          val warning = DangerousDrivingWarning(first.car, msg, System.currentTimeMillis(), 0)
          println(first.car + "" + msg + "，经过的卡口有：" + list.asScala.map(_.monitorId).mkString("->"))
          warning
        }
      }).addSink(new JdbcWriteDataSink[DangerousDrivingWarning](classOf[DangerousDrivingWarning]))

    env.execute(this.getClass.getSimpleName)
  }

  class TrafficOutOfSpeedFunction(defaultLimitSpeed: Double) extends RichMapFunction[TrafficInfo, OutOfLimitSpeedInfo] {
    private val monitorMap = new mutable.HashMap[String, MonitorInfo]()

    override def open(params: Configuration): Unit = {
      val conn = DriverManager.getConnection("jdbc:mysql://node2/traffic_monitor", "root", "123456")
      val sql = "select monitor_id,road_id,speed_limit,area_id from t_monitor_info where speed_limit > 0"
      val pst = conn.prepareStatement(sql)
      val res = pst.executeQuery()
      while (res.next()) {
        val monitorInfo = MonitorInfo(res.getString(1), res.getString(2), res.getInt(3), res.getString(4))
        monitorMap.put(monitorInfo.monitorId, monitorInfo)
      }
      res.close()
      pst.close()
      conn.close()
    }

    override def map(value: TrafficInfo): OutOfLimitSpeedInfo = {
      val monitorInfo = monitorMap.getOrElse(value.monitorId, MonitorInfo(value.monitorId, value.roadId, defaultLimitSpeed.toInt, value.areaId))

      OutOfLimitSpeedInfo(value.car, value.monitorId, value.roadId, value.speed, monitorInfo.limitSpeed, value.actionTime)
    }
  }
}
