package com.yw.traffic.util

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._

import java.util

class HbaseWriterDataSink[T](classType: Class[_ <: T]) extends RichSinkFunction[java.util.List[Put]]{
  var conn: HConnection = _
  var conf: Configuration = _

  override def open(params: Configuration): Unit = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "node3:2181,node4:2181,node3:2181")
    conn = HConnectionManager.createConnection(conf)
  }

  override def close(): Unit = conn.close()

  override def invoke(value: util.List[Put], context: SinkFunction.Context): Unit = {
    var tableName = "1"
    if (classType.getName.equals(classOf[TrackInfo].getName)) {
      tableName = "t_track_info"
    }
    val table = conn.getTable(tableName)
    table.put(value)
    table.close()
  }
}
