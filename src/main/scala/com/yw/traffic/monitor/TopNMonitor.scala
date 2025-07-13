package com.yw.traffic.monitor

import com.yw.traffic.util.GlobalConstants.KAFKA_SOURCE
import com.yw.traffic.util.{AvgSpeedInfo, JdbcWriteDataSink, TrafficInfo}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.function.{ProcessAllWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration
import java.util.Date

/**
 * 实时统计最通畅的 TopN 卡口
 *
 * @author yangwei
 */
object TopNMonitor {
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

    // 统计每个卡口的平均速度，设置滑动窗口
    stream.keyBy(_.monitorId).window(
//        SlidingEventTimeWindows.of(Time.minutes(1), Time.minutes(5))
      // 为了测试方便，滑动步长10秒，窗口长度30秒
      SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(10))
    ).aggregate(new SpeedAggregate(), new AverageSpeedFunction)
//      .addSink(new JdbcWriteDataSink[AvgSpeedInfo](classOf[AvgSpeedInfo]))
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(30)))
      .process(new TopNProcessFunction(3))
      .print()

    env.execute(this.getClass.getSimpleName)
  }

  // 当前需求中，累加器需要同时计算车辆的数量，还需要累加所有车速之后，使用二元组（累加车速之后，车的数量）
  class SpeedAggregate() extends AggregateFunction[TrafficInfo, (Double, Long), (Double, Long)] {
    override def createAccumulator(): (Double, Long) = (0.0, 0)

    override def add(value: TrafficInfo, acc: (Double, Long)): (Double, Long) = {
      (acc._1 + value.speed, acc._2 + 1)
    }

    override def getResult(acc: (Double, Long)): (Double, Long) = acc

    override def merge(a: (Double, Long), b: (Double, Long)): (Double, Long) = {
      (a._1 + b._1, a._2 + b._2)
    }
  }

  // 计算平均速度，然后输出
  class AverageSpeedFunction extends WindowFunction[(Double, Long), AvgSpeedInfo, String, TimeWindow] {
    // 计算平均速度
    override def apply(key: String, window: TimeWindow, input: Iterable[(Double, Long)], out: Collector[AvgSpeedInfo]): Unit = {
      val t: (Double, Long) = input.iterator.next()
      val avgSpeed = "%.2f".format(t._1 / t._2).toDouble
      out.collect(AvgSpeedInfo(window.getStart, window.getEnd, key, avgSpeed, t._2.toInt))
    }
  }

  // 降序排序，并取 topN，其中 n 可以通过参数传入
  class TopNProcessFunction(topN: Int) extends ProcessAllWindowFunction[AvgSpeedInfo, String, TimeWindow] {
    override def process(context: Context, elements: Iterable[AvgSpeedInfo], out: Collector[String]): Unit = {
      // 从 Iterable 得到数据按照访问的次数降序排序
      val list = elements.toList.sortBy(_.avgSpeed)(Ordering.Double.reverse).take(topN)

      val sb = new StringBuilder
      sb.append(s"最通畅Top${topN}个时间窗口时间: ${new Date(context.window.getStart)} ~ ${new Date(context.window.getEnd)}\n")
      list.foreach(t => {
        sb.append(s"卡口ID: ${t.monitorId}，平均车速是: ${t.avgSpeed}\n")
      })
      sb.append("----------------------------------\n")
      out.collect(sb.toString())
    }
  }
}
