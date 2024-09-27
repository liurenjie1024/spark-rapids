package com.nvidia.spark.rapids

import ai.rapids.cudf.HostColumnVector

object TestTableUtils {
  def struct(values: AnyRef*) = new HostColumnVector.StructData(values)

  def structs(values: HostColumnVector.StructData*): Array[HostColumnVector.StructData] = {
    values.toArray
  }

  def strings(values: String*): Array[String] = values.toArray

  def integers(values: Integer*): Array[Integer] = values.toArray
}
