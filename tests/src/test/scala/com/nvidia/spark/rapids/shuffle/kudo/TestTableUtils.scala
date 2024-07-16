package com.nvidia.spark.rapids.shuffle.kudo

import ai.rapids.cudf.HostColumnVector
import java.util.{List => JList}
import scala.collection.JavaConverters._

object TestTableUtils {
  def structID(value1: Int, value2: Float) = new HostColumnVector.StructData(
    Array[AnyRef](Integer.valueOf(value1), java.lang.Float.valueOf(value2)):_*)

  def structII(value1: Int, value2: Int) = new HostColumnVector.StructData(
    Array[AnyRef](Integer.valueOf(value1), Integer.valueOf(value2)):_*)

  def struct(values: java.lang.Object*) = new HostColumnVector.StructData(values:_*)

  def structs(values: Seq[HostColumnVector.StructData]): JList[HostColumnVector.StructData] = {
    values.toList.asJava
  }

  def strings(values: String*): Array[String] = values.toArray

  def integers(values: Integer*): Array[Integer] = values.toArray

  def longs(values: java.lang.Long*): Array[java.lang.Long] = values.toArray

  def doubles(values: java.lang.Double*): Array[java.lang.Double] = values.toArray
}
