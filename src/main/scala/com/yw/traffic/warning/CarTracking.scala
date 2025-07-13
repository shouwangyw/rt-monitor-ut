package com.yw.traffic.warning

import com.yw.traffic.util.GlobalConstants.KAFKA_SOURCE
import com.yw.traffic.util.{GlobalConstants, HbaseWriterDataSink, JdbcReadDataSource, JdbcWriteDataSink, TrackInfo, TrafficInfo, ViolationInfo}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes

import java.time.Duration

/**
 * 违法车辆轨迹跟踪
 *
 * @author yangwei
 */
object CarTracking {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val mainStream = env.fromSource(KAFKA_SOURCE, WatermarkStrategy.noWatermarks(), "KafkaSource")
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

    val broadcastStream = env.addSource(new JdbcReadDataSource[ViolationInfo](classOf[ViolationInfo]))
      // 获取违法车辆信息，并广播出去
      .broadcast(GlobalConstants.VIOLATION_STATE_DESCRIPTOR)

    mainStream.connect(broadcastStream)
      .process(new ProcessCarTracingFunction)
      // 实时写入MySQL数据库中
      .addSink(new JdbcWriteDataSink[TrackInfo](classOf[TrackInfo]))
//      // 批量写入数据到Hbase表中，启动CountWindow 来完成批量插入，批量的条数由窗口大小决定
//      .countWindowAll(10)
//      .apply((win: GlobalWindow, in: Iterable[TrackInfo], out: Collector[java.util.List[Put]]) => {
//        val list = new java.util.ArrayList[Put]()
//        for (info <- in) {
//          // 在hbase表中为了方便查询每辆车最近的车辆轨迹，根据车辆通行的时间降序排序
//          // rowKey: 车牌号 + (Long.maxValue - actionTime)
//          val put = new Put(Bytes.toBytes(info.car + "_" + (Long.MaxValue - info.actionTime)))
//          put.add("cf1".getBytes(), "car".getBytes(), Bytes.toBytes(info.car))
//          put.add("cf1".getBytes(), "actionTime".getBytes(), Bytes.toBytes(info.actionTime))
//          put.add("cf1".getBytes(), "monitorId".getBytes(), Bytes.toBytes(info.monitorId))
//          put.add("cf1".getBytes(), "roadId".getBytes(), Bytes.toBytes(info.roadId))
//          put.add("cf1".getBytes(), "areaId".getBytes(), Bytes.toBytes(info.areaId))
//          put.add("cf1".getBytes(), "speed".getBytes(), Bytes.toBytes(info.speed))
//          list.add(put)
//        }
//        print(list.size() + ",")
//        out.collect(list)
//      }).addSink(new HbaseWriterDataSink[TrackInfo](classOf[TrackInfo]))

    env.execute(this.getClass.getSimpleName)
  }

  class ProcessCarTracingFunction extends BroadcastProcessFunction[TrafficInfo, ViolationInfo, TrackInfo] {
    override def processElement(value: TrafficInfo,
                                ctx: BroadcastProcessFunction[TrafficInfo, ViolationInfo, TrackInfo]#ReadOnlyContext,
                                out: Collector[TrackInfo]): Unit = {
      val violationInfo = ctx.getBroadcastState(GlobalConstants.VIOLATION_STATE_DESCRIPTOR).get(value.car)
      if (violationInfo != null) {
        // 需要跟踪的车辆
        val trackInfo = TrackInfo(value.car, value.actionTime, value.monitorId, value.roadId, value.areaId, value.speed)
        out.collect(trackInfo)
      }
    }

    override def processBroadcastElement(value: ViolationInfo,
                                         ctx: BroadcastProcessFunction[TrafficInfo, ViolationInfo, TrackInfo]#Context,
                                         out: Collector[TrackInfo]): Unit = {
      val state = ctx.getBroadcastState(GlobalConstants.VIOLATION_STATE_DESCRIPTOR)
      state.put(value.car, value)
    }
  }
}
