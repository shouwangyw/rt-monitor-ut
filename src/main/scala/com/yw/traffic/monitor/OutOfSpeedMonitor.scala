package com.yw.traffic.monitor

import com.yw.traffic.util.GlobalConstants.KAFKA_SOURCE
import com.yw.traffic.util._
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.util.Collector

/**
 * 实时车辆超速监控
 *
 * @author yangwei
 */
object OutOfSpeedMonitor {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val mainStream = env.fromSource(KAFKA_SOURCE, WatermarkStrategy.noWatermarks(), "KafkaSource")
      .map(line => {
        val slices = line.split(",")
        TrafficInfo(slices(0).toLong, slices(1), slices(2), slices(3), slices(4).toDouble, slices(5), slices(6))
      })

    // 读取广播状态的Source，并广播出去
    val broadcastStream = env.addSource(new JdbcReadDataSource[MonitorInfo](classOf[MonitorInfo]))
      .broadcast(GlobalConstants.MONITOR_STATE_DESCRIPTOR)

    // 主流.connect 算子，再调用 Process 底层API
    mainStream.connect(broadcastStream)
      .process(new ProcessOutOfSpeedFunction)
      // 写入MySQL表中
      .addSink(new JdbcWriteDataSink[OutOfLimitSpeedInfo](classOf[OutOfLimitSpeedInfo]))

    env.execute(this.getClass.getSimpleName)
  }

  class ProcessOutOfSpeedFunction extends BroadcastProcessFunction[TrafficInfo, MonitorInfo, OutOfLimitSpeedInfo] {
    // 处理主流中的数据
    override def processElement(value: TrafficInfo,
                                ctx: BroadcastProcessFunction[TrafficInfo, MonitorInfo, OutOfLimitSpeedInfo]#ReadOnlyContext,
                                out: Collector[OutOfLimitSpeedInfo]): Unit = {
      // 根据卡口ID，从广播状态中得到当前卡口的限速对象
      val monitorInfo = ctx.getBroadcastState(GlobalConstants.MONITOR_STATE_DESCRIPTOR).get(value.monitorId)
      if (monitorInfo != null) {
        // 有限速设置
        val limitSpeed = monitorInfo.limitSpeed
        val realSpeed = value.speed
        println(s"limitSpeed: ${limitSpeed}, realSpeed: ${realSpeed}")
        if (limitSpeed * 1.1 <= realSpeed) {
          // 超速通过卡口
          val outOfLimitSpeedInfo = OutOfLimitSpeedInfo(value.car, value.monitorId, value.roadId, realSpeed, limitSpeed, value.actionTime)
          // 只要超速就对外输出
          out.collect(outOfLimitSpeedInfo)
        }
      }
    }
    // 处理广播状态流中的数据，从数据流中得到数据，放入广播状态中
    override def processBroadcastElement(value: MonitorInfo,
                                         ctx: BroadcastProcessFunction[TrafficInfo, MonitorInfo, OutOfLimitSpeedInfo]#Context,
                                         collector: Collector[OutOfLimitSpeedInfo]): Unit = {
      val bcState = ctx.getBroadcastState(GlobalConstants.MONITOR_STATE_DESCRIPTOR)
      bcState.put(value.monitorId, value)
    }
  }
}
