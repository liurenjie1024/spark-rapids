package com.nvidia.spark.rapids.shuffle.kudo

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.lang.{Double => JDouble, Long => JLong}
import java.util.{List => JList}
import java.util.Arrays.asList
import java.util.Collections
import java.util.Collections.{emptyList, singletonList}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import ai.rapids.cudf.{ColumnVector, DType, HostColumnVector, Table}
import ai.rapids.cudf.HostColumnVector.StructData
import com.nvidia.spark.rapids.Arm.withResource
import com.nvidia.spark.rapids.TestUtils
import com.nvidia.spark.rapids.shuffle.SlicedTable
import com.nvidia.spark.rapids.shuffle.TableUtils.schemaOf
import com.nvidia.spark.rapids.shuffle.kudo.TestTableUtils.{doubles, integers, longs, strings, struct, structID, structII, structs}
import org.scalatest.funsuite.AnyFunSuite

class KudoSerializerSuite extends AnyFunSuite {
  test("Test serialize table") {
    withResource(buildTestTable()) { table =>
      for (sliceStep <- 1 to table.getRowCount.toInt) {
        val slicedTables = (0 until table.getRowCount.toInt by sliceStep).map { start =>
          if (start + sliceStep >= table.getRowCount.toInt) {
            SlicedTable.from(table, start, table.getRowCount.toInt - start)
          } else {
            SlicedTable.from(table, start, sliceStep)
          }
        }

        checkMergeTable(table, slicedTables)
      }
    }
  }

  test("SerializationRoundTripEmpty") {
    withResource(ColumnVector.fromInts()) { emptyInt =>
      withResource(ColumnVector.fromDoubles()) { emptyDouble =>
        withResource(ColumnVector.fromStrings()) { emptyString =>
          withResource(new Table(emptyInt, emptyInt, emptyDouble, emptyString)) { t =>
            val bout = new ByteArrayOutputStream
            val serializer = new KudoSerializer
            serializer.writeToStream(t, bout, 0, 0)
            bout.flush()
            val bytes = bout.toByteArray
            assert(bytes.isEmpty)
          }
        }
      }
    }
  }

  test("Serialize Zero Columns") {
    val bout = new ByteArrayOutputStream
    val serializer = new KudoSerializer
    serializer.writeRowsToStream(bout, 10)
    bout.flush()
    val bin = new ByteArrayInputStream(bout.toByteArray)
    val obj = serializer.readOneTableBuffer(bin)
    assert(obj.isInstanceOf[SerializedTable])
    val serializedBatch = obj.asInstanceOf[SerializedTable]
    assert(10 == serializedBatch.getHeader.getNumRows)
  }

  test("MergeTableWithDifferentValidity") {
    withResource(new ArrayBuffer[Table]()) { tables =>
      tables += new Table.TestBuilder()
        .column(longs(-83182L, 5822L, 3389L, 7384L, 7297L):_*)
        .column(doubles(-2.06, -2.14, 8.04, 1.16, -1.0):_*)
        .build()

      tables += new Table.TestBuilder()
        .column(longs(-47L, null.asInstanceOf[JLong], -83L, -166L, -220L, 470L, 619L, 803L,
          661L):_*)
        .column(doubles(-6.08, 1.6, 1.78, -8.01, 1.22, 1.43, 2.13, -1.65, null
          .asInstanceOf[JDouble]):_*)
        .build()

      tables += new Table.TestBuilder()
        .column(longs(8722L, 8733L):_*)
        .column(doubles(2.51, 0.0):_*)
        .build()

      tables += new Table.TestBuilder()
        .column(longs(7384L, 7297L, 803L, 661L, 8733L):_*)
        .column(doubles(1.16, -1.0, -1.65, null.asInstanceOf[JDouble], 0.0):_*)
        .build()

      checkMergeTable(tables(3), Seq(
        SlicedTable.from(tables(0), 3, 2),
        SlicedTable.from(tables(1), 7, 2),
        SlicedTable.from(tables(2), 1, 1)))
    }
  }

  test("Merge List") {
    withResource(new ArrayBuffer[Table]()) { tables =>
      tables += new Table.TestBuilder()
        .column(longs(-881L, 482L, 660L, 896L, -129L, -108L, -428L, 0L, 617L, 782L):_*)
        .column(integers(665), integers(-267), integers(398), integers(-314),
          integers(-370), integers(181), integers(665, 544), integers(222), integers(-587),
          integers(544))
        .build()

      tables += new Table.TestBuilder()
        .column(longs(-881L, 482L, 660L, 896L, 122L, 241L, 281L, 680L, 783L,
          null.asInstanceOf[JLong]):_*)
        .column(integers(-370), integers(398), integers(-587, 398), integers(-314),
          integers(307), integers(-397, -633), integers(-314, 307), integers(-633), integers(-397),
          integers(181, -919, -175))
        .build()

      tables += new Table.TestBuilder()
        .column(longs(896L, -129L, -108L, -428L, 0L, 617L, 782L, 482L, 660L, 896L, 122L, 241L,
          281L, 680L, 783L, null.asInstanceOf[JLong]):_*)
        .column(integers(-314), integers(-370), integers(181), integers(665, 544), integers(222),
          integers(-587), integers(544), integers(398), integers(-587, 398), integers(-314),
          integers(307), integers(-397, -633), integers(-314, 307), integers(-633), integers(-397),
          integers(181, -919, -175))
        .build()

      checkMergeTable(tables(2), Seq(
        SlicedTable.from(tables(0), 3, 7),
        SlicedTable.from(tables(1), 1, 9)))
    }
  }

  test("Serialize Validity") {
    withResource(new ArrayBuffer[Table]()) { tables =>
      val values = integers(Seq(null.asInstanceOf[Integer], null.asInstanceOf[Integer]) ++
        (2 until 512).map(Integer.valueOf): _*)

      tables += new Table.TestBuilder()
        .column(values: _*)
        .build()

      tables += new Table.TestBuilder()
        .column(integers(509, 510, 511):_*)
        .build()

      checkMergeTable(tables(1), Seq(
        SlicedTable.from(tables(0), 509, 3)))
    }
  }

  private def checkMergeTable(expectedTable: Table, slicedTables: Seq[SlicedTable]): Unit = {
    val serializer = new KudoSerializer
    val bout = new ByteArrayOutputStream
    for (t <- slicedTables) {
      serializer.writeToStream(t.getTable, bout, t.getStartRow, t.getNumRows.toInt)
    }

    bout.flush()

    val bin = new ByteArrayInputStream(bout.toByteArray)
    val serializedBatches = (0 until slicedTables.size)
      .map(_ => serializer.readOneTableBuffer(bin))

    var numRows = 0
    for (obj <- serializedBatches) {
      assert(obj.isInstanceOf[SerializedTable])
      val serializedBatch = obj.asInstanceOf[SerializedTable]
      numRows += serializedBatch.getHeader.getNumRows.toInt
      assert(numRows < Int.MaxValue)
    }

    assert(numRows == expectedTable.getRowCount)

    withResource(serializer.mergeTable(serializedBatches.toList.asJava, schemaOf(expectedTable))) {
      ct =>
      val found = ct.getTable
      (0 until found.getNumberOfColumns)foreach(idx => found.getColumn(idx).getNullCount)
      println("Found table: " + found)
      TestUtils.compareTables(expectedTable, found)
    }
  }



  private def buildTestTable(): Table = {
    val listMapType = new HostColumnVector.ListType(true,
      new HostColumnVector.ListType(true,
        new HostColumnVector.StructType(true,
          new HostColumnVector.BasicType(false, DType.STRING),
          new HostColumnVector.BasicType(true, DType.STRING))))
    val mapStructType = new HostColumnVector.ListType(true,
      new HostColumnVector.StructType(true,
      new HostColumnVector.BasicType(false, DType.STRING),
      new HostColumnVector.BasicType(false, DType.STRING)))
    val structType = new HostColumnVector.StructType(true,
      new HostColumnVector.BasicType(true, DType.INT32),
      new HostColumnVector.BasicType(false, DType.FLOAT32))
    val listDateType = new HostColumnVector.ListType(true,
      new HostColumnVector.StructType(false,
        new HostColumnVector.BasicType(false, DType.INT32),
        new HostColumnVector.BasicType(true, DType.INT32)))
    new Table.TestBuilder()
      .column(Array[java.lang.Integer](100, 202, 3003, 40004, 5, -60, 1, null, 3, null, 5, null,
        7,
        null, 9, null, 11, null, 13, null, 15):_*)
      .column(true, true, false, false, true, null, true, true, null, false, false, null, true,
        true, null, false, false, null, true, true, null)
      .column(Array[java.lang.Byte](1.toByte, 2.toByte, null, 4.toByte, 5.toByte, 6.toByte,
        1.toByte, 2.toByte,
        3.toByte, null, 5.toByte, 6.toByte, 7.toByte, null, 9.toByte, 10.toByte, 11.toByte, null,
        13.toByte, 14.toByte, 15.toByte):_*)
      .column(Array[java.lang.Short](6.toShort, 5.toShort, 4.toShort, null, 2.toShort, 1.toShort,
        1.toShort, 2.toShort, 3.toShort, null, 5.toShort, 6.toShort, 7.toShort, null, 9.toShort,
        10.toShort, null, 12.toShort, 13.toShort, 14.toShort, null):_*)
      .column(Array[java.lang.Long](1L, null, 1001L, 50L, -2000L, null, 1L, 2L, 3L, 4L, null, 6L,
        7L, 8L, 9L, null, 11L, 12L, 13L, 14L, null):_*)
      .column(Array[java.lang.Float](10.1f, 20f, -1, 3.1415f, -60f, null, 1f, 2f, 3f, 4f,
        5f, null, 7f, 8f, 9f, 10f, 11f, null, 13f, 14f, 15f):_*)
      .column(Array[java.lang.Float](10.1f, 20f, -2, 3.1415f, -60f, -50f, 1f, 2f, 3f, 4f,
        5f, 6f, 7f, 8f, 9f, 10f, 11f, 12f, 13f, 14f, 15f):_*)
      .column(Array[java.lang.Double](10.1, 20.0, 33.1, 3.1415, -60.5, null, 1d, 2.0, 3.0, 4.0, 5.0,
        6.0, null, 8.0, 9.0, 10.0, 11.0, 12.0, null, 14.0, 15.0):_*)
      .column(Array[java.lang.Float](null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null):_*)
//      .timestampDayColumn(Array[java.lang.Integer](99, 100, 101, 102, 103, 104, 1, 2, 3, 4, 5, 6,
//        7, null, 9, 10, 11, 12, 13, null, 15):_*)
//      .timestampMillisecondsColumn(Array[java.lang.Long](9L, 1006L, 101L, 5092L, null, 88L, 1L,
//        2L, 3L, 4L, 5L, 6L, 7L, 8L, null, 10L, 11L, 12L, 13L, 14L, 15L):_*)
//      .timestampSecondsColumn(Array[java.lang.Long](1L, null, 3L, 4L, 5L, 6L, 1L, 2L, 3L, 4L, 5L,
//        6L, 7L, 8L, 9L, null, 11L, 12L, 13L, 14L, 15L):_*)
      .decimal32Column(-3, Array[java.lang.Integer](100, 202, 3003, 40004, 5, -60, 1, null, 3,
        null, 5, null, 7, null, 9, null, 11, null, 13, null, 15):_*)
      .decimal64Column(-8, Array[java.lang.Long](1L, null, 1001L, 50L, -2000L, null, 1L, 2L, 3L,
        4L, null, 6L, 7L, 8L, 9L, null, 11L, 12L, 13L, 14L, null):_*)
      .column(Array[java.lang.String]("A", "B", "C", "D", null, "TESTING", "1", "2", "3", "4",
        "5", "6", "7", null, "9", "10", "11", "12", "13", null, "15"):_*)
      .column(Array[java.lang.String]("A", "A", "C", "C", "E", "TESTING", "1", "2", "3", "4", "5",
        "6", "7", "", "9", "10", "11", "12", "13", "", "15"):_*)
      .column(Array[java.lang.String]("", "", "", "", "", "", "", "", "", "", "", "", "", "", "",
        "", "", "", "", "", ""):_*)
      .column(Array[java.lang.String]("", null, "", "", null, "", "", "", "", "", "", "", "", "",
        "", "", "", "", "", "", ""):_*)
      .column(Array[java.lang.String](null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null):_*)
      .column(mapStructType,  Array[JList[StructData]](
        structs(Seq(struct("1", "2"))), structs(Seq(struct("3", "4"))), null, null,
        structs(Seq(struct("key", "value"), struct("a", "b"))), null, null,
        structs(Seq(struct("3", "4"), struct("1", "2"))), structs(Seq()),
        structs(Seq(null, struct("foo", "bar"))),
        structs(Seq(null, null, null)), null, null, null, null, null, null, null, null, null,
        structs(Seq(struct("the", "end")))):_*)
      .column(structType, Array[StructData](structID(1, 1f), null, structID(2, 3f),
        null, structID(8, 7f), structID(0, 0f), null,
        null, structID(-1, -1f), structID(-100, -100f),
        structID(Integer.MAX_VALUE, Float.MaxValue), null,
        null, null,
        null, null,
        null, null,
        null, null,
        structID(Integer.MIN_VALUE, Float.MinValue)):_*)
      .column(integers(1, 2), null, integers(3, 4, null, 5, null), null, null, integers(6, 7, 8),
        integers(null, null, null), integers(1, 2, 3), integers(4, 5, 6), integers(7, 8, 9),
        integers(10, 11, 12), integers(null.asInstanceOf[Integer]), integers(14, null),
        integers(14, 15, null, 16, 17, 18), integers(19, 20, 21), integers(22, 23, 24),
        integers(25, 26, 27), integers(28, 29, 30), integers(31, 32, 33), null,
        integers(37, 38, 39))
      .column(integers(), integers(), integers(), integers(), integers(), integers(), integers(),
        integers(), integers(), integers(), integers(), integers(), integers(), integers(),
        integers(), integers(), integers(), integers(), integers(), integers(), integers())
      .column(integers(null, null), integers(null, null, null, null), integers(),
        integers(null, null, null), integers(), integers(null, null, null, null, null),
        integers(null.asInstanceOf[Integer]), integers(null, null, null), integers(null, null),
        integers(null, null, null, null), integers(null, null, null, null, null), integers(),
        integers(null, null, null, null), integers(null, null, null), integers(null, null),
        integers(null, null, null), integers(null, null), integers(null.asInstanceOf[Integer]),
        integers(null.asInstanceOf[Integer]), integers(null, null),
        integers(null, null, null, null, null))
      .column(Array[java.lang.Integer](null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null):_*)
      .column(strings("1", "2", "3"), strings("4"), strings("5"), strings("6, 7"),
        strings("", "9", null), strings("11"), strings(""), strings(null, null),
        strings("15", null), null, null, strings("18", "19", "20"), null, strings("22"),
        strings("23", ""), null, null, null, null, strings(), strings("the end"))
      .column(strings(), strings(), strings(), strings(), strings(), strings(), strings(),
        strings(), strings(), strings(), strings(), strings(), strings(), strings(), strings(),
        strings(), strings(), strings(), strings(), strings(), strings())
      .column(strings(null, null), strings(null, null, null, null), strings(),
        strings(null, null, null), strings(), strings(null, null, null, null, null),
        strings(null.asInstanceOf[String]), strings(null, null, null), strings(null, null),
        strings(null, null, null, null), strings(null, null, null, null, null), strings(),
        strings(null, null, null, null), strings(null, null, null), strings(null, null),
        strings(null, null, null), strings(null, null), strings(null.asInstanceOf[String]),
        strings(null.asInstanceOf[String]), strings(null, null),
        strings(null, null, null, null, null))
      .column(Array[java.lang.String](null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null, null):_*)
      .column(listMapType, asList(asList(struct("k1", "v1"), struct("k2", "v2")),
        singletonList(struct("k3", "v3"))),
        asList(asList(struct("k4", "v4"), struct("k5", "v5"),
          struct("k6", "v6")), singletonList(struct("k7", "v7"))),
        null, null, null, asList(asList(struct("k8", "v8"), struct("k9", "v9")),
          asList(struct("k10", "v10"), struct("k11", "v11"), struct("k12", "v12"),
            struct("k13", "v13"))),
        singletonList(asList(struct("k14", "v14"), struct("k15", "v15"))), null, null, null, null,
        asList(asList(struct("k16", "v16"), struct("k17", "v17")),
          singletonList(struct("k18", "v18"))),
        asList(asList(struct("k19", "v19"), struct("k20", "v20")),
          singletonList(struct("k21", "v21"))),
        asList(singletonList(struct("k22", "v22")), singletonList(struct("k23", "v23"))),
        asList(null, null, null),
        asList(singletonList(struct("k22", null)), singletonList(struct("k23", null))),
        null, null, null, null, null)
      .column(listDateType, asList(structII(-210, 293), structII(-719, 205), structII(-509, 183),
        structII(174, 122), structII(647, 683)),
        asList(structII(311, 992), structII(-169, 482), structII(166, 525)),
        asList(structII(156, 197), structII(926, 134), structII(747, 312), structII(293, 801)),
        asList(structII(647, null.asInstanceOf[Int]), structII(293, 387)),
        Collections.emptyList(),
        null, emptyList(), null,
        asList(structII(-210, 293), structII(-719, 205), structII(-509, 183), structII(174, 122),
          structII(647, 683)),
        asList(structII(311, 992), structII(-169, 482), structII(166, 525)),
        asList(structII(156, 197), structII(926, 134), structII(747, 312), structII(293, 801)),
        asList(structII(647, null.asInstanceOf[Int]), structII(293, 387)), emptyList, null,
        emptyList, null,
        singletonList(structII(778, 765)), asList(structII(7, 87), structII(8, 96)),
        asList(structII(9, 56), structII(10, 532), structII(11, 456)), null, emptyList)
      .build
  }
}
