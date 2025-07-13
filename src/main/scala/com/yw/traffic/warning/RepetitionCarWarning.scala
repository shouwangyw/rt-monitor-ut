package com.yw.traffic.warning

import com.yw.traffic.util.GlobalConstants.KAFKA_SOURCE
import com.yw.traffic.util.{JdbcWriteDataSink, RepetitionCarWarning, TrafficInfo}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import java.time.Duration

/**
 * 实时套牌车报警：数据不考虑乱序的情况，因为一般情况下不可能有车辆在 10 秒内经过两个卡口
 *
 * @author yangwei
 */
object RepetitionCarWarning {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.fromSource(KAFKA_SOURCE, WatermarkStrategy.noWatermarks(), "KafkaSource")
      .map(line => {
        println(s"$line")
        val slices = line.split(",")
        TrafficInfo(slices(0).toLong, slices(1), slices(2), slices(3), slices(4).toDouble, slices(5), slices(6))
      }).assignTimestampsAndWatermarks(
      // 设置watermark
      WatermarkStrategy.forBoundedOutOfOrderness[TrafficInfo](Duration.ofSeconds(5))
        .withTimestampAssigner(new SerializableTimestampAssigner[TrafficInfo] {
          override def extractTimestamp(t: TrafficInfo, l: Long): Long = t.actionTime
        }).withIdleness(Duration.ofSeconds(5))
    )

    stream.keyBy(_.car)
      .process(new RepetitionCarWarningProcess)
      .addSink(new JdbcWriteDataSink[RepetitionCarWarning](classOf[RepetitionCarWarning]))

    env.execute(this.getClass.getSimpleName)
  }

  // 当前业务需要判断在10秒内，不同卡口或同一卡口出现同一个车牌
  class RepetitionCarWarningProcess extends KeyedProcessFunction[String, TrafficInfo, RepetitionCarWarning] {
    // 使用状态保存该车辆第一次通过卡口的时间
    lazy val firstState = getRuntimeContext.getState(new ValueStateDescriptor[TrafficInfo]("first", classOf[TrafficInfo]))

    override def processElement(value: TrafficInfo,
                                ctx: KeyedProcessFunction[String, TrafficInfo, RepetitionCarWarning]#Context, out: Collector[RepetitionCarWarning]): Unit = {
      val first = firstState.value()
      if (first == null) {
        // 当前车辆第一次经过某个路口
        firstState.update(value)
      } else {
        // 当前车辆之前已经出现在某个卡口，现在要判断时间间隔是否超过10秒
        // 超过10秒则认为不是套牌车的概率大，没超过10秒时套牌车的概率大
        val nowTime = value.actionTime
        val firstTime = first.actionTime
        val diffSec = (nowTime - firstTime).abs / 1000
        if (diffSec <= 10) {
          val warning = new RepetitionCarWarning(value.car,
            if (nowTime > firstTime) first.monitorId else value.monitorId,
            if (nowTime < firstTime) first.monitorId else value.monitorId,
            "涉嫌套牌车", System.currentTimeMillis())
          firstState.update(null)
          out.collect(warning)
        } else if (nowTime > firstTime) {
          firstState.update(value)
        }
      }
    }
  }
}
