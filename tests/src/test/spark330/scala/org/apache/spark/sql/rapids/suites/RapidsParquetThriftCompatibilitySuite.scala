/*
 * Copyright (c) 2024, NVIDIA CORPORATION.
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

/*** spark-rapids-shim-json-lines
{"spark": "330"}
spark-rapids-shim-json-lines ***/
package org.apache.spark.sql.rapids.suites

import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.parquet.ParquetThriftCompatibilitySuite
import org.apache.spark.sql.rapids.utils.RapidsSQLTestsBaseTrait

class RapidsParquetThriftCompatibilitySuite
  extends ParquetThriftCompatibilitySuite
  with RapidsSQLTestsBaseTrait {

  test("Read Parquet file generated by parquet-thrift Rapids") {

    val parquetFilePath =
      "test-data/parquet-thrift-compat.snappy.parquet"

    checkAnswer(spark.read.parquet(testFile(parquetFilePath)), (0 until 10).map { i =>
      val suits = Array("SPADES", "HEARTS", "DIAMONDS", "CLUBS")

      val nonNullablePrimitiveValues = Seq(
        i % 2 == 0,
        i.toByte,
        (i + 1).toShort,
        i + 2,
        i.toLong * 10,
        i.toDouble + 0.2d,
        // Thrift `BINARY` values are actually unencoded `STRING` values, and thus are always
        // treated as `BINARY (UTF8)` in parquet-thrift, since parquet-thrift always assume
        // Thrift `STRING`s are encoded using UTF-8.
        s"val_$i",
        s"val_$i",
        // Thrift ENUM values are converted to Parquet binaries containing UTF-8 strings
        suits(i % 4))

      val nullablePrimitiveValues = if (i % 3 == 0) {
        Seq.fill(nonNullablePrimitiveValues.length)(null)
      } else {
        nonNullablePrimitiveValues
      }

      val complexValues = Seq(
        Seq.tabulate(3)(n => s"arr_${i + n}"),
        // Thrift `SET`s are converted to Parquet `LIST`s
        Seq(i),
        Seq.tabulate(3)(n => (i + n: Integer) -> s"val_${i + n}").toMap,
        Seq.tabulate(3) { n =>
          (i + n) -> Seq.tabulate(3) { m =>
            Row(Seq.tabulate(3)(j => i + j + m), s"val_${i + m}")
          }
        }.toMap)

      Row(nonNullablePrimitiveValues ++ nullablePrimitiveValues ++ complexValues: _*)
    })
  }

}