package com.yw.traffic.distribution

import com.google.common.hash.Hashing
import com.yw.traffic.util.GlobalConstants.KAFKA_SOURCE
import com.yw.traffic.util.TrafficInfo
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

import java.nio.charset.Charset
import java.time.Duration
import scala.collection.mutable
import scala.util.control.Breaks

/**
 * 实时车辆分布情况统计，使用布隆过滤器去重
 *
 * @author yangwei
 */
object AreaDistributionByBloomFilter {
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

    val map = new mutable.HashMap[String, Long]()
    stream.keyBy(_.areaId)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      // 采用布隆过滤器处理海量数据去重的问题，同时采用 Trigger 来解决默认窗口状态中数据过大
      .trigger(new MyTrigger) // 设置窗口触发的时机和状态是否清除的问题
      .process(new BloomFilterProcess(map))
      .print()

    env.execute(this.getClass.getSimpleName)
  }
  // 自定义Trigger
  class MyTrigger extends Trigger[TrafficInfo, TimeWindow] {
    // 当前窗口进入一条数据的回调函数
    override def onElement(t: TrafficInfo, l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE
    // 当前窗口已经结束的回调函数(基于运行时间)
    override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE
    // 当前窗口已经结束的回调函数(基于事件时间)
    override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE
    // 当前窗口对象销毁
    override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {}
  }
  // 采用布隆过滤器来去重，ProcessWindowFunction 是全量窗口函数
  class BloomFilterProcess(map: mutable.HashMap[String, Long]) extends ProcessWindowFunction[TrafficInfo, String, String, TimeWindow] {
    // 定义redis连接
    var jedis: Jedis = _
    var bloomFilter: MyBloomFilter = _

    override def open(parameters: Configuration): Unit = {
      jedis = new Jedis("node4")
      jedis.select(0)
      bloomFilter = new MyBloomFilter(1 << 27, 2)
    }

    // 当有一条数据进入窗口的时候就必须先使用布隆过滤器去重，然后在累加。
    // 当这条数据累加之后把它从状态中清除
    override def process(key: String,
                         context: Context,
                         elements: Iterable[TrafficInfo],
                         out: Collector[String]): Unit = {
      val windowStart = context.window.getStart
      val windowEnd = context.window.getEnd
      val car = elements.last.car
      // 1. redis 数据来负责 bitmap 计算(返回 1 或者 0)
      // 2. redis 存储每一个区域中每个窗口的累加器
      var count = 0L
      // 先从 redis 中取得累加器的值
      // 布隆过滤器的 key 有:区域 ID+窗口时间
      val bloomKey = key + "_" + windowEnd;
      if (map.contains(bloomKey)) {
        // 第一次从 Map 集合中判断有没有这个累加器
        count = map.getOrElse(bloomKey, 0)
      }

      val offsets = bloomFilter.getOffsets2(car)
      // 为了计算准确，一区域中的一个窗口对应一个布隆过滤器
      // 初始化一个判断是否重复车牌
      var repeated = true // 默认所有车牌都重复
      val loop = new Breaks
      loop.breakable {
        for (offset <- offsets) { // 遍历下标的列表
          // isContain = true(1)，代表可能车牌重复，如果 isContain=false(0)代表当前车牌
          // 绝对不可能重复
          val isContain = jedis.getbit(bloomKey, offset)
          if (!isContain) {
            repeated = false
            loop.break()
          }
        }
      }
      // 当前车牌号已经重复的，所以不用累加，直接输出
      if (!repeated) {
        // 当前车辆号没有出现过重复的，所以要累加
        count += 1
        for (offset <- offsets) { // 把当前的车牌号写入布隆过滤器
          jedis.setbit(bloomKey, offset, true)
        }
        // 把 redis 中保存的累加器更新一下
        map.put(bloomKey, count)
      }
      out.collect(s"区域ID: ${key}, 在窗口起始时间: ${windowStart} --- 窗口结束时间: ${windowEnd}, 一共有上路车辆为: ${count}")
    }
  }
  // 自定义布隆过滤器
  class MyBloomFilter(numBits: Long, numFuncs: Int) extends Serializable {
    // 自定义hash函数，采用google的hash函数
    def myHash(car: String): Long = {
      Hashing.murmur3_128().hashString(car, Charset.forName("UTF-8")).asLong()
    }

    // 根据车牌，去布隆过滤器中计算得到该车牌对应的下标
    def getOffsets(car: String): Array[Long] = {
      var firstHash = myHash(car)
      // 无符号右移，左边没有0且没有符号
      //      val firstHash2Hash = firstHash >> 16
      var secondHash = car.hashCode.toLong
      // 数组长度和hash函数的个数一样
      val result = new Array[Long](numFuncs)
      for (i <- 1 to numFuncs) {
        if (i == 1) {
          //          val combineHash = firstHash + i * firstHash2Hash
          if (firstHash < 0) {
            firstHash = ~firstHash //取反计算
          }
          // 得到一个下标保存到数组
          result(0) = firstHash % numBits
        }
        if (i == 2) {
          if (secondHash < 0) {
            secondHash = ~secondHash
          }
          result(1) = secondHash % numBits
        }
      }
      result
    }

    // 根据车牌，去布隆过滤器中计算得到该车牌对应的下标
    def getOffsets2(car: String): Array[Long] = {
      var firstHash = myHash(car)
      if (firstHash < 0) {
        firstHash = ~firstHash
      }
      val result = new Array[Long](numFuncs)
      result(0) = firstHash % numBits

      result
    }
  }
}
