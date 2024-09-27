package com.nvidia.spark.rapids

import org.scalatest.funsuite.AnyFunSuite

class KudoSerializerSuite extends AnyFunSuite {
  test("test") {
    println("Hello")
  }

//  private def buildTestTable(): Table = {
//    val listMapType = new HostColumnVector.ListType(true,
//      new HostColumnVector.ListType(true,
//        new HostColumnVector.StructType(true,
//          new HostColumnVector.BasicType(false, DType.STRING),
//          new HostColumnVector.BasicType(true, DType.STRING))))
//    val mapStructType = new HostColumnVector.StructType(true,
//      new HostColumnVector.BasicType(false, DType.STRING),
//      new HostColumnVector.BasicType(false, DType.STRING))
//    val structType = new HostColumnVector.StructType(true,
//      new HostColumnVector.BasicType(true, DType.INT32),
//      new HostColumnVector.BasicType(false, DType.FLOAT32))
//    val listDateType = new HostColumnVector.ListType(true,
//      new HostColumnVector.StructType(false,
//        new HostColumnVector.BasicType(false, DType.INT32),
//        new HostColumnVector.BasicType(true, DType.INT32)))
//    new Table.TestBuilder()
//      .column(Array[java.lang.Integer](100, 202, 3003, 40004, 5, -60, 1, null, 3, null, 5, null,
  //      7,
//        null, 9, null, 11, null, 13, null, 15))
//      .column(true, true, false, false, true, null, true, true, null, false, false, null, true,
//        true, null, false, false, null, true, true, null)
//      .column(Array[java.lang.Byte](1.toByte, 2.toByte, null, 4.toByte, 5.toByte, 6.toByte,
//        1.toByte, 2.toByte,
//        3.toByte, null, 5.toByte, 6.toByte, 7.toByte, null, 9.toByte, 10.toByte, 11.toByte, null,
//        13.toByte, 14.toByte, 15.toByte))
//      .column(Array[java.lang.Short](6.toShort, 5.toShort, 4.toShort, null, 2.toShort, 1.toShort,
//        1.toShort, 2.toShort, 3.toShort, null, 5.toShort, 6.toShort, 7.toShort, null, 9.toShort,
//        10.toShort, null, 12.toShort, 13.toShort, 14.toShort, null))
//      .column(Array[java.lang.Long](1L, null, 1001L, 50L, -2000L, null, 1L, 2L, 3L, 4L, null, 6L,
//        7L, 8L, 9L, null, 11L, 12L, 13L, 14L, null))
//      .column(Array[java.lang.Float](10.1f, 20f, Float.NaN, 3.1415f, -60f, null, 1f, 2f, 3f, 4f,
//        5f, null, 7f, 8f, 9f, 10f, 11f, null, 13f, 14f, 15f))
//      .column(Array[java.lang.Float](10.1f, 20f, Float.NaN, 3.1415f, -60f, -50f, 1f, 2f, 3f, 4f,
//        5f, 6f, 7f, 8f, 9f, 10f, 11f, 12f, 13f, 14f, 15f))
//      .column(Array[java.lang.Double](10.1, 20.0, 33.1, 3.1415, -60.5, null, 1d, 2.0, 3.0, 4.0, 5.0,
//        6.0, null, 8.0, 9.0, 10.0, 11.0, 12.0, null, 14.0, 15.0))
//      .column(Array[java.lang.Float](null, null, null, null, null, null, null, null, null, null,
//    null, null, null, null, null, null, null, null, null, null, null))
//      .timestampDayColumn(Array[java.lang.Integer](99, 100, 101, 102, 103, 104, 1, 2, 3, 4, 5, 6,
//        7, null, 9, 10, 11, 12, 13, null, 15))
//      .timestampMillisecondsColumn(Array[java.lang.Long](9L, 1006L, 101L, 5092L, null, 88L, 1L,
//        2L, 3L, 4L, 5L, 6L, 7L, 8L, null, 10L, 11L, 12L, 13L, 14L, 15L))
//      .timestampSecondsColumn(Array[java.lang.Long](1L, null, 3L, 4L, 5L, 6L, 1L, 2L, 3L, 4L, 5L,
//        6L, 7L, 8L, 9L, null, 11L, 12L, 13L, 14L, 15L))
//      .decimal32Column(-3, Array[java.lang.Integer](100, 202, 3003, 40004, 5, -60, 1, null, 3,
//        null, 5, null, 7, null, 9, null, 11, null, 13, null, 15))
//      .decimal64Column(-8, Array[java.lang.Long](1L, null, 1001L, 50L, -2000L, null, 1L, 2L, 3L,
//        4L, null, 6L, 7L, 8L, 9L, null, 11L, 12L, 13L, 14L, null))
//      .column(Array[java.lang.String]("A", "B", "C", "D", null, "TESTING", "1", "2", "3", "4",
//        "5", "6", "7", null, "9", "10", "11", "12", "13", null, "15"))
//      .column(Array[java.lang.String]("A", "A", "C", "C", "E", "TESTING", "1", "2", "3", "4", "5",
//        "6", "7", "", "9", "10", "11", "12", "13", "", "15"))
//      .column(Array[java.lang.String]("", "", "", "", "", "", "", "", "", "", "", "", "", "", "",
//        "", "", "", "", "", ""))
//      .column(Array[java.lang.String]("", null, "", "", null, "", "", "", "", "", "", "", "", "",
//        "", "", "", "", "", "", ""))
//      .column(Array[java.lang.String](null, null, null, null, null, null, null, null, null, null,
//        null, null, null, null, null, null, null, null, null, null, null))
//      .column(mapStructType, structs(struct("1", "2")), structs(struct("3", "4")), null, null,
//        structs(struct("key", "value"), struct("a", "b")), null, null,
//        structs(struct("3", "4"), struct("1", "2")), structs(),
//        structs(null, struct("foo", "bar")),
//        structs(null, null, null), null, null, null, null, null, null, null, null, null,
//        structs(struct("the", "end")))
//      .column(structType, struct(1, 1f), null, struct(2, 3f), null, struct(8, 7f),
//        struct(0, 0f), null, null, struct(-1, -1f), struct(-100, -100f), struct(Integer
//          .MAX_VALUE, Float.MaxValue), null, null, null, null, null, null, null, null, null,
//        struct(Integer.MIN_VALUE, Float.MinValue))
//      .column(integers(1, 2), null, integers(3, 4, null, 5, null), null, null, integers(6, 7, 8),
//        integers(null, null, null), integers(1, 2, 3), integers(4, 5, 6), integers(7, 8, 9),
//        integers(10, 11, 12), integers(null.asInstanceOf[Integer]), integers(14, null),
//        integers(14, 15, null, 16, 17, 18), integers(19, 20, 21), integers(22, 23, 24),
//        integers(25, 26, 27), integers(28, 29, 30), integers(31, 32, 33), null,
//        integers(37, 38, 39))
//      .column(integers(), integers(), integers(), integers(), integers(), integers(), integers(),
//        integers(), integers(), integers(), integers(), integers(), integers(), integers(),
//        integers(), integers(), integers(), integers(), integers(), integers(), integers())
//      .column(integers(null, null), integers(null, null, null, null), integers(),
//        integers(null, null, null), integers(), integers(null, null, null, null, null),
//        integers(null.asInstanceOf[Integer]), integers(null, null, null), integers(null, null),
//        integers(null, null, null, null), integers(null, null, null, null, null), integers(),
//        integers(null, null, null, null), integers(null, null, null), integers(null, null),
//        integers(null, null, null), integers(null, null), integers(null.asInstanceOf[Integer]),
//        integers(null.asInstanceOf[Integer]), integers(null, null),
//        integers(null, null, null, null, null))
//      .column(Array[java.lang.Integer](null, null, null, null, null, null, null, null, null, null,
//        null, null, null, null, null, null, null, null, null, null, null))
//      .column(strings("1", "2", "3"), strings("4"), strings("5"), strings("6, 7"),
//        strings("", "9", null), strings("11"), strings(""), strings(null, null),
//        strings("15", null), null, null, strings("18", "19", "20"), null, strings("22"),
//        strings("23", ""), null, null, null, null, strings(), strings("the end"))
//      .column(strings(), strings(), strings(), strings(), strings(), strings(), strings(),
//        strings(), strings(), strings(), strings(), strings(), strings(), strings(), strings(),
//        strings(), strings(), strings(), strings(), strings(), strings())
//      .column(strings(null, null), strings(null, null, null, null), strings(),
//        strings(null, null, null), strings(), strings(null, null, null, null, null),
//        strings(null.asInstanceOf[String]), strings(null, null, null), strings(null, null),
//        strings(null, null, null, null), strings(null, null, null, null, null), strings(),
//        strings(null, null, null, null), strings(null, null, null), strings(null, null),
//        strings(null, null, null), strings(null, null), strings(null.asInstanceOf[String]),
//        strings(null.asInstanceOf[String]), strings(null, null),
//        strings(null, null, null, null, null))
//      .column(Array[java.lang.String](null, null, null, null, null, null, null, null, null, null,
//        null, null, null, null, null, null, null, null, null, null, null))
//      .column(listMapType, asList(asList(struct("k1", "v1"), struct("k2", "v2")),
//        singletonList(struct("k3", "v3"))),
//        asList(asList(struct("k4", "v4"), struct("k5", "v5"), struct("k6", "v6")),
//          singletonList(struct("k7", "v7"))),
//        null, null, null, asList(asList(struct("k8", "v8"), struct("k9", "v9")),
//          asList(struct("k10", "v10"), struct("k11", "v11"), struct("k12", "v12"),
//            struct("k13", "v13"))),
//        singletonList(asList(struct("k14", "v14"), struct("k15", "v15"))), null, null, null, null,
//        asList(asList(struct("k16", "v16"), struct("k17", "v17")),
//          singletonList(struct("k18", "v18"))),
//        asList(asList(struct("k19", "v19"), struct("k20", "v20")),
//          singletonList(struct("k21", "v21"))),
//        asList(singletonList(struct("k22", "v22")), singletonList(struct("k23", "v23"))),
//        asList(null, null, null),
//        asList(singletonList(struct("k22", null)), singletonList(struct("k23", null))),
//        null, null, null, null, null)
//      .column(listDateType, asList(struct(-210, 293), struct(-719, 205), struct(-509, 183),
//        struct(174, 122), struct(647, 683)),
//        asList(struct(311, 992), struct(-169, 482), struct(166, 525)),
//        asList(struct(156, 197), struct(926, 134), struct(747, 312), struct(293, 801)),
//        asList(struct(647, null), struct(293, 387)), emptyList, null, emptyList, null,
//        asList(struct(-210, 293), struct(-719, 205), struct(-509, 183), struct(174, 122),
//          struct(647, 683)),
//        asList(struct(311, 992), struct(-169, 482), struct(166, 525)),
//        asList(struct(156, 197), struct(926, 134), struct(747, 312), struct(293, 801)),
//        asList(struct(647, null), struct(293, 387)), emptyList, null, emptyList, null,
//        singletonList(struct(778, 765)), asList(struct(7, 87), struct(8, 96)),
//        asList(struct(9, 56), struct(10, 532), struct(11, 456)), null, emptyList)
//      .build
//  }
}
