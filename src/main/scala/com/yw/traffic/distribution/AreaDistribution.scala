package com.yw.traffic.distribution

import com.yw.traffic.util.GlobalConstants.KAFKA_SOURCE
import com.yw.traffic.util.TrafficInfo
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration

/**
 * 实时车辆分布情况统计
 * @author yangwei
 */
object AreaDistribution {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.fromSource(KAFKA_SOURCE, WatermarkStrategy.noWatermarks(), "KafkaSource")
      .map(line => {
//        println(s"$line")
        val slices = line.split(",")
        TrafficInfo(slices(0).toLong, slices(1), slices(2), slices(3), slices(4).toDouble, slices(5), slices(6))
      }).assignTimestampsAndWatermarks(
      // 设置watermark
      WatermarkStrategy.forBoundedOutOfOrderness[TrafficInfo](Duration.ofSeconds(5))
        .withTimestampAssigner(new SerializableTimestampAssigner[TrafficInfo] {
          override def extractTimestamp(t: TrafficInfo, l: Long): Long = t.actionTime
        }).withIdleness(Duration.ofSeconds(5))
    )

    stream.keyBy(_.areaId)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .apply(new WindowFunction[TrafficInfo, (String, Long), String, TimeWindow] {
        override def apply(key: String, window: TimeWindow, in: Iterable[TrafficInfo], out: Collector[(String, Long)]): Unit = {
          // 使用Set集合去重
          var ids = Set[String]()
          for (t <- in) {
            ids += t.car
          }
          val msg = s"区域ID: ${key}, 在${window.getStart} - ${window.getEnd} 的时间范围中，一共有上路车辆: ${ids.size}"
          println(msg)
          out.collect(msg, ids.size)
        }
      })

    env.execute(this.getClass.getSimpleName)
  }
}
