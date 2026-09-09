/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.delta

import ai.rapids.cudf.{ColumnVector => CudfColumnVector, Scalar, Table}
import com.nvidia.spark.rapids.{GpuColumnVector, GpuMetric}
import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}
import com.nvidia.spark.rapids.RapidsPluginImplicits._
import org.roaringbitmap.longlong.{PeekableLongIterator, Roaring64Bitmap}

import org.apache.spark.sql.types.{BooleanType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}


object GpuDeltaParquetFileFormatUtils {
  /**
   * Row number of the row in the file. When used with [[FILE_PATH_COL]] together, it can be used
   * as unique id of a row in file. To correctly calculate this, the caller needs to disable file
   * splitting and predicate pushdown.
   */
  val METADATA_ROW_IDX_COL: String = "__metadata_row_index"
  val METADATA_ROW_IDX_FIELD: StructField = StructField(METADATA_ROW_IDX_COL, LongType,
    nullable = false)

  val METADATA_ROW_DEL_COL: String = "__metadata_row_del"
  val METADATA_ROW_DEL_FIELD: StructField = StructField(METADATA_ROW_DEL_COL, BooleanType,
    nullable = false)


  /**
   * File path of the file that the row came from.
   */
  val FILE_PATH_COL: String = "_metadata_file_path"
  val FILE_PATH_FIELD: StructField = StructField(FILE_PATH_COL, StringType, nullable = false)

  /** Metadata describing a contiguous range of rows from a single file. */
  case class FileRowRange(
      rowIndexStart: Long,
      rowCount: Long,
      delVector: Option[Roaring64Bitmap])

  /** Add metadata columns to an iterator containing rows from one file. */
  def addMetadataColumnToIterator(
      schema: StructType,
      delVector: Option[Roaring64Bitmap],
      input: Iterator[ColumnarBatch],
      maxBatchSize: Int,
      delVectorScatterTimeMetric: GpuMetric
  ): Iterator[ColumnarBatch] = {
    addMetadataColumnToIterator(schema,
      Seq(FileRowRange(0, Long.MaxValue, delVector)), input, maxBatchSize,
      delVectorScatterTimeMetric)
  }

  /**
   * Add metadata columns when the input can contain row ranges from multiple files.
   * Row indexes continue between chunks from the same file and restart for each file range.
   */
  def addMetadataColumnToIterator(
      schema: StructType,
      fileRowRanges: Seq[FileRowRange],
      input: Iterator[ColumnarBatch],
      maxBatchSize: Int,
      delVectorScatterTimeMetric: GpuMetric
  ): Iterator[ColumnarBatch] = {
    val metadataRowIndexCol = schema.fieldNames.indexOf(METADATA_ROW_IDX_COL)
    val delRowIdx = schema.fieldNames.indexOf(METADATA_ROW_DEL_COL)
    if (metadataRowIndexCol == -1 && delRowIdx == -1) {
      return input
    }

    val ranges = fileRowRanges.filter(_.rowCount > 0).iterator.buffered
    var rowsConsumedFromRange = 0L

    def rangesForBatch(numRows: Int): Seq[FileRowRange] = {
      val result = Seq.newBuilder[FileRowRange]
      var rowsRemaining = numRows.toLong
      while (rowsRemaining > 0) {
        require(ranges.hasNext, "Insufficient file row metadata for decoded Parquet batch")
        val current = ranges.head
        val available = current.rowCount - rowsConsumedFromRange
        require(available > 0, "Invalid file row metadata with no rows remaining")
        val rowsToRead = Math.min(rowsRemaining, available)
        result += current.copy(
          rowIndexStart = current.rowIndexStart + rowsConsumedFromRange,
          rowCount = rowsToRead)
        rowsConsumedFromRange += rowsToRead
        rowsRemaining -= rowsToRead
        if (rowsConsumedFromRange == current.rowCount) {
          ranges.next()
          rowsConsumedFromRange = 0
        }
      }
      result.result()
    }

    input.map { batch =>
      if (batch.numRows() == 0) {
        batch
      } else withResource(batch) { _ =>
        val rowIdxCol = if (metadataRowIndexCol == -1) {
          None
        } else {
          Some(metadataRowIndexCol)
        }

        val delRowIdx2 = if (delRowIdx == -1) {
          None
        } else {
          Some(delRowIdx)
        }
        addMetadataColumns(rowIdxCol, delRowIdx2, rangesForBatch(batch.numRows()), maxBatchSize,
          batch, delVectorScatterTimeMetric)
      }
    }
  }

  private def createFalseTable(numRows: Int): Table = {
    withResource(Scalar.fromBool(false)) { s =>
      withResource(CudfColumnVector.fromScalar(s, numRows)) { c =>
        new Table(c)
      }
    }
  }


  private def addMetadataColumns(
      rowIdxPos: Option[Int],
      delRowIdx: Option[Int],
      fileRowRanges: Seq[FileRowRange],
      maxBatchSize: Int,
      batch: ColumnarBatch,
      delVectorScatterTimeMetric: GpuMetric,
  ): ColumnarBatch = {
    val rowIdxCol = rowIdxPos.map { _ =>
      val rangeColumns = fileRowRanges.safeMap { range =>
        withResource(Scalar.fromLong(range.rowIndexStart)) { start =>
          GpuColumnVector.from(CudfColumnVector.sequence(start, Math.toIntExact(range.rowCount)),
            METADATA_ROW_IDX_FIELD.dataType)
        }
      }.toArray
      withResource(rangeColumns) { columns =>
        val combined = if (columns.length == 1) {
          columns.head.getBase.incRefCount()
        } else {
          CudfColumnVector.concatenate(columns.map(_.getBase): _*)
        }
        closeOnExcept(combined) { _ =>
          GpuColumnVector.from(combined, METADATA_ROW_IDX_FIELD.dataType)
        }
      }
    }

    closeOnExcept(rowIdxCol) { rowIdxCol =>

      val delVecCol = delRowIdx.map { _ =>
        delVectorScatterTimeMetric.ns {
          val rangeColumns = fileRowRanges.safeMap { range =>
            val delVec = range.delVector.getOrElse {
              throw new IllegalStateException("Missing deletion vector for low shuffle merge")
            }
            val rowCount = Math.toIntExact(range.rowCount)
            val table = new RoaringBitmapIterator(
              delVec.getLongIteratorFrom(range.rowIndexStart),
              range.rowIndexStart,
              range.rowIndexStart + rowCount)
              .grouped(Math.min(maxBatchSize, rowCount))
              .foldLeft(createFalseTable(rowCount)){ (table, posChunk) =>
                withResource(table) { _ =>
                  withResource(CudfColumnVector.fromLongs(posChunk: _*)) { poses =>
                    withResource(Scalar.fromBool(true)) { s =>
                      Table.scatter(Array(s), poses, table)
                    }
                  }
                }
              }
            withResource(table) { _ =>
              GpuColumnVector.from(table.getColumn(0).incRefCount(),
                METADATA_ROW_DEL_FIELD.dataType)
            }
          }.toArray
          withResource(rangeColumns) { columns =>
            val combined = if (columns.length == 1) {
              columns.head.getBase.incRefCount()
            } else {
              CudfColumnVector.concatenate(columns.map(_.getBase): _*)
            }
            closeOnExcept(combined) { _ =>
              GpuColumnVector.from(combined, METADATA_ROW_DEL_FIELD.dataType)
            }
          }
        }
      }

      closeOnExcept(delVecCol) { delVecCol =>
        // Replace row_idx column
        val columns = new Array[ColumnVector](batch.numCols())
        for (i <- 0 until batch.numCols()) {
          if (rowIdxPos.contains(i)) {
            columns(i) = rowIdxCol.get
          } else if (delRowIdx.contains(i)) {
            columns(i) = delVecCol.get
          } else {
            columns(i) = batch.column(i) match {
              case gpuCol: GpuColumnVector => gpuCol.incRefCount()
              case col => col
            }
          }
        }

        new ColumnarBatch(columns, batch.numRows())
      }
    }
  }
}

class RoaringBitmapIterator(val inner: PeekableLongIterator, val start: Long, val end: Long)
  extends Iterator[Long] {

  override def hasNext: Boolean = {
    inner.hasNext && inner.peekNext() < end
  }

  override def next(): Long = {
    inner.next() - start
  }
}
