/*
 * Copyright (c) 2022-2024, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.nvidia.spark.rapids.shuffle

import java.util.concurrent.TimeUnit

import scala.concurrent.duration._

import com.nvidia.spark.rapids.{FunSuiteWithTempDir, SparkQueryCompareTestSuite}

import org.apache.spark.internal.Logging

case class ResultFileSize(parquetSize: Long, jcudfSize: Long, kudoSize: Long)

case class BenchmarkResult(columnNum: Int, rowCount: Int, avgTime: Duration, split: Int = 1) {
  def toPrettyString: String = {
    s"ColumnNum: $columnNum, RowCount: $rowCount, avgTime: ${avgTime.toUnit(TimeUnit.SECONDS)}"
  }
}

case class HeaderSizeResult(columnNum: Int, split: Int, size: Long)

class ShuffleFormatSuite extends SparkQueryCompareTestSuite with FunSuiteWithTempDir with Logging {
//  test("Serializer Compression") {
//    val columnNums = Iterator.iterate(2)(_ * 2).takeWhile(_ <= 1024).toArray
//    val rowCounts = Iterator.iterate(100)(_ * 10).takeWhile(_ <= 100000).toArray
//
//    val resultMap = mutable.Map[(Int, Int), ResultFileSize]()
//
//    for (columnNum <- columnNums) {
//      for (rowCount <- rowCounts) {
//        val result = run(columnNum, rowCount)
//        resultMap.put((columnNum, rowCount), result)
//      }
//    }
//
//    withResource(new FileWriter(s"/home/rayliu/Downloads/shuffle/" +
//      s"compression_${System.currentTimeMillis()}.xls")) { writer =>
//      withResource(new CSVPrinter(writer, CSVFormat.EXCEL)) { printer =>
//        // Header for parquet compression
//        printer.printRecord(("Parquet+None Compression" +: rowCounts).toSeq.asJava)
//        for (columnNum <- columnNums) {
//          val row = rowCounts.map { rowCount =>
//            val result = resultMap((columnNum, rowCount))
//            result.parquetSize.toDouble / result.jcudfSize
//          }
//          printer.printRecord((columnNum +: row).toSeq.asJava)
//        }
//
//        printer.println()
//
//        // Header for kudo compression
//        printer.printRecord(("Kudo Compression" +: rowCounts).toSeq.asJava)
//        for (columnNum <- columnNums) {
//          val row = rowCounts.map { rowCount =>
//            val result = resultMap((columnNum, rowCount))
//            result.kudoSize.toDouble / result.jcudfSize
//          }
//          printer.printRecord((columnNum +: row).toSeq.asJava)
//        }
//
//        printer.println()
//
//        // Header for parquet size
//        printer.printRecord(("Parquet size" +: rowCounts).toSeq.asJava)
//        for (columnNum <- columnNums) {
//          val row = rowCounts.map { rowCount =>
//            val result = resultMap((columnNum, rowCount))
//            result.parquetSize
//          }
//          printer.printRecord((columnNum +: row).toSeq.asJava)
//        }
//
//        printer.println()
//
//        // Header for kudo size
//        printer.printRecord(("Kudo size" +: rowCounts).toSeq.asJava)
//        for (columnNum <- columnNums) {
//          val row = rowCounts.map { rowCount =>
//            val result = resultMap((columnNum, rowCount))
//            result.kudoSize
//          }
//          printer.printRecord((columnNum +: row).toSeq.asJava)
//        }
//
//        printer.println()
//
//        // Header for jcudf size
//        printer.printRecord(("jcudf size" +: rowCounts).toSeq.asJava)
//        for (columnNum <- columnNums) {
//          val row = rowCounts.map { rowCount =>
//            val result = resultMap((columnNum, rowCount))
//            result.jcudfSize
//          }
//          printer.printRecord((columnNum +: row).toSeq.asJava)
//        }
//
//        printer.println()
//      }
//    }
//  }
//
//  test("Verify header size impact") {
//
//    val columnNums = Iterator.iterate(2)(_ * 2).takeWhile(_ <= 1024).toArray
//    val rowCount = 100000
//    val splits = Iterator.iterate(1)(_ * 10).takeWhile(_ <= 1000).toArray
//
//    val jcudfSize = withCpuSparkSession(spark => columnNums.flatMap { colNum =>
//      serializeWithSplits(spark, colNum, rowCount, splits, new JCudfSerializer())
//    })
//      .map(r => ((r.columnNum, r.split), r.size))
//      .toMap
//
//    val kudoSize = withCpuSparkSession(spark => columnNums.flatMap { colNum =>
//      serializeWithSplits(spark, colNum, rowCount, Array(1), new KudoSerializer())
//    })
//      .map(r => (r.columnNum, r.size))
//      .toMap
//
//
//    withResource(new FileWriter(s"/home/rayliu/Downloads/shuffle/" +
//      s"header_size_${System.currentTimeMillis()}.xls")) { writer =>
//      withResource(new CSVPrinter(writer, CSVFormat.EXCEL)) { printer =>
//        printer.printRecord(("JCudf size" +: splits).toSeq.asJava)
//        for (colNum <- columnNums) {
//          val row = splits.map { split =>
//            jcudfSize((colNum, split))
//          }
//          printer.printRecord((colNum +: row).toSeq.asJava)
//        }
//
//        printer.println()
//
//        printer.printRecord(("Kudo size" +: Array(1)).toSeq.asJava)
//        for (colNum <- columnNums) {
//          val row = kudoSize(colNum)
//          printer.printRecord(Seq(colNum, row).asJava)
//        }
//
//        printer.println()
//
//        printer.printRecord(("JCudf Ratio" +: splits).toSeq.asJava)
//        for (colNum <- columnNums) {
//          val row = splits.map { split =>
//            jcudfSize((colNum, split))
//          }
//          val baseLine = kudoSize(colNum).toDouble
//          val rowRatio = row.map(_ / baseLine)
//          printer.printRecord((colNum +: rowRatio).toSeq.asJava)
//        }
//      }
//    }
//  }
//
//  test("Benchmark serialization time") {
//    val columnNums = Iterator.iterate(2)(_ * 2).takeWhile(_ <= 1024).toArray
//    val rowCounts = Iterator.iterate(100)(_ * 10).takeWhile(_ <= 100000).toArray
//
//    val kudoResult = withCpuSparkSession(spark => {
//      val serializer = new KudoSerializer()
//      println(s"Kudo serializer version: ${serializer.version()}")
//
//      columnNums.flatMap { colNum =>
//        rowCounts.map { rowCounts =>
//          benchSerialization(spark, colNum, rowCounts, 1, serializer)
//        }
//      }.map(r => ((r.columnNum, r.rowCount), r.avgTime)).toMap
//    })
//
//    val jcudfResult = withCpuSparkSession(spark => {
//      val serializer = new JCudfSerializer()
//      println(s"Jcudf serializer version: ${serializer.version()}")
//
//      columnNums.flatMap { colNum =>
//        rowCounts.map { rowCounts =>
//          benchSerialization(spark, colNum, rowCounts, 1, serializer)
//        }
//      }.map(r => ((r.columnNum, r.rowCount), r.avgTime)).toMap
//    })
//
//    withResource(new FileWriter(s"/home/rayliu/Downloads/shuffle/" +
//      s"serialization_time_${System.currentTimeMillis()}.xls")) { writer =>
//      withResource(new CSVPrinter(writer, CSVFormat.EXCEL)) { printer =>
//        printer.printRecord(("Kudo Result" +: rowCounts).toSeq.asJava)
//        for (colNum <- columnNums) {
//          val row = rowCounts.map { rowCount =>
//            kudoResult((colNum, rowCount)).toUnit(TimeUnit.SECONDS)
//          }
//          printer.printRecord((colNum +: row).toSeq.asJava)
//        }
//        printer.println()
//
//        printer.printRecord(("Jcudf Result" +: rowCounts).toSeq.asJava)
//        for (colNum <- columnNums) {
//          val row = rowCounts.map { rowCount =>
//            jcudfResult((colNum, rowCount)).toUnit(TimeUnit.SECONDS)
//          }
//          printer.printRecord((colNum +: row).toSeq.asJava)
//        }
//        printer.println()
//
//        printer.printRecord(("Kudo vs Jcudf" +: rowCounts).toSeq.asJava)
//        for (colNum <- columnNums) {
//          val row = rowCounts.map { rowCount =>
//            kudoResult((colNum, rowCount)).toUnit(TimeUnit.SECONDS) /
//              jcudfResult((colNum, rowCount)).toUnit(TimeUnit.SECONDS)
//          }
//          printer.printRecord((colNum +: row).toSeq.asJava)
//        }
//        printer.println()
//      }
//    }
//  }
//
//  test("Benchmark_split_serialization_time") {
//    val columnNums = Iterator.iterate(2)(_ * 2).takeWhile(_ <= 1024).toArray
//    val rowCount = 100000
//    val splits = Iterator.iterate(1)(_ * 10).takeWhile(_ <= 1000).toArray
//
//    val kudoResult = withCpuSparkSession(spark => {
//      val serializer = new KudoSerializer()
//      println(s"Kudo serializer version: ${serializer.version()}")
//
//      columnNums.map { colNum =>
//        benchSerialization(spark, colNum, rowCount, 1, serializer)
//      }.map(r => (r.columnNum, r.avgTime)).toMap
//    })
//
//    val jcudfResult = withCpuSparkSession(spark => {
//      val serializer = new JCudfSerializer()
//      println(s"Jcudf serializer version: ${serializer.version()}")
//
//      columnNums.flatMap { colNum =>
//        splits.map { split =>
//          val ret = benchSerialization(spark, colNum, rowCount, split, serializer)
//          println(ret)
//          ret
//        }
//      }.map(r => ((r.columnNum, r.split), r.avgTime)).toMap
//    })
//
//    withResource(new FileWriter(s"/home/rayliu/Downloads/shuffle/" +
//      s"split_serialization_time_${System.currentTimeMillis()}.xls")) { writer =>
//      withResource(new CSVPrinter(writer, CSVFormat.EXCEL)) { printer =>
//        printer.printRecord(("Kudo Result" +: Array(1)).toSeq.asJava)
//        for (colNum <- columnNums) {
//          val row = kudoResult(colNum).toUnit(TimeUnit.SECONDS)
//          printer.printRecord(Seq(colNum, row).asJava)
//        }
//        printer.println()
//
//        printer.printRecord(("Jcudf Result" +: splits).toSeq.asJava)
//        for (colNum <- columnNums) {
//          val row = splits.map { split =>
//            jcudfResult((colNum, split)).toUnit(TimeUnit.SECONDS)
//          }
//          printer.printRecord((colNum +: row).toSeq.asJava)
//        }
//        printer.println()
//
//        printer.printRecord(("Kudo vs Jcudf" +: splits).toSeq.asJava)
//        for (colNum <- columnNums) {
//          val row = splits.map { split =>
//            jcudfResult((colNum, split)).toUnit(TimeUnit.SECONDS) /
//              kudoResult(colNum).toUnit(TimeUnit.SECONDS)
//          }
//          printer.printRecord((colNum +: row).toSeq.asJava)
//        }
//        printer.println()
//      }
//    }
//  }
//
//  test("Profile_split_serialization_time") {
//    val rowCount = 100000
//
//    withCpuSparkSession(spark => {
//      val serializer = new JCudfSerializer()
//      println(s"Jcudf serializer version: ${serializer.version()}")
//      benchSerialization(spark, 512, rowCount, 1000, serializer, 50000)
//    })
//  }
//
//  private def run(columnNum: Int, rowCount: Int): ResultFileSize = {
//    val dbGen = DBGen()
//    val tableSchema = getTableSchema(columnNum)
//    val tableName = s"t_${columnNum}_$rowCount"
//    val table = dbGen.addTable(tableName, tableSchema, rowCount)
//    table.setNullProbabilityRecursively(0.5)
//
//    withCpuSparkSession(spark => {
//      spark.conf.set(PARQUET_COMPRESSION.key, "none")
//
//      dbGen.writeParquet(spark, TEST_FILES_ROOT.getAbsolutePath, 1)
//      val parquetFiles = Files.list(Paths.get(TEST_FILES_ROOT.getAbsolutePath, tableName))
//        .iterator()
//        .asScala
//        .map(_.toFile)
//        .filter(_.getName.endsWith(".parquet"))
//        .toArray
//
//      val parquetFileSize = parquetFiles.map(_.length()).sum
//
//      val jcudfSize = serializeParquetTable(tableName, parquetFiles, "jcudf") { (fos, hostCols) =>
//        JCudfSerialization.writeToStream(hostCols, fos, 0, hostCols(0).getRowCount)
//      }
//
//      val kudoSize = serializeParquetTable(tableName, parquetFiles, "kudo") { (fos, hostCols) =>
//        val kudoSer = new KudoSerializer()
//        kudoSer.writeToStream(hostCols, fos, 0, hostCols(0).getRowCount)
//      }
//
//      ResultFileSize(parquetFileSize, jcudfSize, kudoSize)
//    })
//  }
//
//  private def serializeParquetTable(tableName: String, parquetFiles: Seq[File], suffix: String)
//    (f: (OutputStream, Array[HostColumnVector]) => Unit): Long = withParquetTable(parquetFiles) {
//    (idx, cudfTable) =>
//      withResource((0 until cudfTable.getNumberOfColumns)
//        .map(cudfTable.getColumn(_).copyToHost()).toArray) { hostCols =>
//        val fullFilename = Paths.get(TEST_FILES_ROOT.getAbsolutePath, tableName,
//          s"${tableName}_$idx.$suffix").toAbsolutePath
//
//        withResource(new FileOutputStream(fullFilename.toString)) { fos =>
//          withResource(new ZstdCompressorOutputStream(fos, 1)) { zstdOut =>
//            f(zstdOut, hostCols)
//          }
//        }
//
//        Files.size(fullFilename)
//      }
//  }.sum
//
//
//  private def withParquetTable[T](parquetFiles: Seq[File])(f: (Int, CudfTable) => T): Seq[T] = {
//    parquetFiles.zipWithIndex.map {
//      case (parquetFile, idx) => withResource(CudfTable.readParquet(parquetFile)) { cudfTable =>
//        f(idx, cudfTable)
//      }
//    }
//  }
//
//
//  private def getTableSchema(columns: Int): String = {
//    val types = Seq("string", "int", "double", "boolean", "long", "double", "short")
//    (0 until columns).map { i =>
//      s"col${i + 1} ${types(i % types.length)}"
//    }.mkString(",")
//  }
//
//  private def withGeneratedParquetTable[T](spark: SparkSession, colNum: Int, rowCount: Int)
//    (f: CudfTable => T)
//  = {
//    val dbGen = DBGen()
//    val tableSchema = getTableSchema(colNum)
//    val tableName = s"t_${colNum}_$rowCount"
//    val table = dbGen.addTable(tableName, tableSchema, rowCount)
//    table.setNullProbabilityRecursively(0.5)
//
//
//    dbGen.writeParquet(spark, TEST_FILES_ROOT.getAbsolutePath, 1, overwrite = true)
//    val parquetFiles = Files.list(Paths.get(TEST_FILES_ROOT.getAbsolutePath, tableName))
//      .iterator()
//      .asScala
//      .map(_.toFile)
//      .filter(_.getName.endsWith(".parquet"))
//      .toArray
//
//    require(parquetFiles.length == 1, s"Only one parquet file is expected, but got " +
//      s"${parquetFiles.length}")
//
//    withResource(CudfTable.readParquet(parquetFiles(0))) { table =>
//      f(table)
//    }
//  }
//
//  private def benchSerialization(spark: SparkSession,
//      numCols: Int,
//      rowCount: Int,
//      split: Int,
//      serde: TableSerializer,
//      times: Int = 5)
//  : BenchmarkResult = {
//    withGeneratedParquetTable(spark, numCols, rowCount) { table =>
//      val cols = (0 until table.getNumberOfColumns).map(table.getColumn(_).copyToHost()).toArray
//      withResource(cols) { _ =>
//        val bench = new Benchmark {
//          override def setup(): Unit = {}
//
//          override def tearDown(): Unit = {}
//
//          override def benchmark(): Unit = {
//            val splitRowCount = rowCount / split
//            for (offset <- 0 until rowCount by splitRowCount) {
//              doSerialization(cols, serde, offset, splitRowCount)
//            }
//            //          println("Finish one iteration")
//          }
//        }
//
//        val avgTimeNs = Benchmark.run(bench, times)
//        BenchmarkResult(numCols, rowCount, avgTimeNs, split = split)
//      }
//    }
//  }
//
//
//  private def doSerialization(cols: Array[HostColumnVector], serde: TableSerializer,
//      offset: Int,
//      rowCount: Int): Long = {
//    withResource(new CountOutputStream()) { out =>
//      withResource(new ZstdCompressorOutputStream(out, 1)) { zout =>
//        serde.writeToStream(cols, zout, offset, rowCount)
//      }
//      out.getCount
//    }
//  }
//
//  private def serializeWithSplits(spark: SparkSession,
//      numCols: Int,
//      rowCount: Int,
//      splits: Seq[Int],
//      serde: TableSerializer)
//  : Seq[HeaderSizeResult] = {
//    withGeneratedParquetTable(spark, numCols, rowCount) { table =>
//      val cols = (0 until table.getNumberOfColumns).map(table.getColumn(_).copyToHost()).toArray
//      withResource(cols) { _ =>
//        val ret = Await.result(Future.sequence(splits.flatMap { s =>
//          val splitRowCount = rowCount / s
//          (0 until rowCount by splitRowCount).map { offset =>
//            Future {
//              val size = doSerialization(cols, serde, offset, splitRowCount)
//              HeaderSizeResult(numCols, s, size)
//            }
//          }
//        }), Duration.Inf)
//        ret.groupBy(_.split)
//          .mapValues(rs => HeaderSizeResult(numCols, rs.head.split, rs.map(_.size).sum))
//          .values
//          .toSeq
//      }
//    }
//  }
}

