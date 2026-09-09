/*
 * Copyright (c) 2023-2026, NVIDIA CORPORATION.
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

package com.nvidia.spark.rapids.delta.delta24x

import java.net.URI

import com.nvidia.spark.rapids._
import com.nvidia.spark.rapids.GpuMetric._
import com.nvidia.spark.rapids.delta.{GpuDeltaParquetFileFormat, RoaringBitmapWrapper}
import com.nvidia.spark.rapids.delta.GpuDeltaParquetFileFormatUtils.{addMetadataColumnToIterator,
  FileRowRange, METADATA_ROW_DEL_COL, METADATA_ROW_IDX_COL}
import com.nvidia.spark.rapids.jni.fileio.RapidsFileIO
import com.nvidia.spark.rapids.parquet._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.apache.parquet.schema.MessageType

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.delta.{DeltaColumnMappingMode, IdMapping}
import org.apache.spark.sql.delta.DeltaParquetFileFormat.DeletionVectorDescriptorWithFilterType
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.GpuFileSourceScanExec
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

case class GpuDelta24xParquetFileFormat(
    metadata: Metadata,
    isSplittable: Boolean,
    disablePushDown: Boolean,
    broadcastDvMap: Option[Broadcast[Map[URI, DeletionVectorDescriptorWithFilterType]]])
  extends GpuDeltaParquetFileFormat {

  override val columnMappingMode: DeltaColumnMappingMode = metadata.columnMappingMode
  override val referenceSchema: StructType = metadata.schema

  if (columnMappingMode == IdMapping) {
    val requiredReadConf = SQLConf.PARQUET_FIELD_ID_READ_ENABLED
    require(SparkSession.getActiveSession.exists(_.sessionState.conf.getConf(requiredReadConf)),
      s"${requiredReadConf.key} must be enabled to support Delta id column mapping mode")
    val requiredWriteConf = SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED
    require(SparkSession.getActiveSession.exists(_.sessionState.conf.getConf(requiredWriteConf)),
      s"${requiredWriteConf.key} must be enabled to support Delta id column mapping mode")
  }

  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = isSplittable

  override def buildReaderWithPartitionValuesAndMetrics(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration,
      metrics: Map[String, GpuMetric])
  : PartitionedFile => Iterator[InternalRow] = {
    val dataReader = super.buildReaderWithPartitionValuesAndMetrics(
      sparkSession,
      dataSchema,
      partitionSchema,
      requiredSchema,
      if (disablePushDown) Seq.empty else filters,
      options,
      hadoopConf,
      metrics)

    val delVecs = broadcastDvMap
    val maxDelVecScatterBatchSize = RapidsConf
      .DELTA_LOW_SHUFFLE_MERGE_SCATTER_DEL_VECTOR_BATCH_SIZE
      .get(sparkSession.sessionState.conf)
    val delVecScatterTimeMetric = metrics(GpuMetric.DELETION_VECTOR_SCATTER_TIME)
    val delVecSizeMetric = metrics(GpuMetric.DELETION_VECTOR_SIZE)


    (file: PartitionedFile) => {
      val input = dataReader(file)
      val dv = delVecs.flatMap(_.value.get(new URI(file.filePath.toString())))
        .map { dv =>
          delVecSizeMetric += dv.descriptor.inlineData.length
          RoaringBitmapWrapper.deserializeFromBytes(dv.descriptor.inlineData).inner
        }
      addMetadataColumnToIterator(prepareSchema(requiredSchema),
        dv,
        input.asInstanceOf[Iterator[ColumnarBatch]],
        maxDelVecScatterBatchSize,
        delVecScatterTimeMetric)
        .asInstanceOf[Iterator[InternalRow]]
    }
  }

  override def createMultiFileReaderFactory(
      broadcastedConf: Broadcast[SerializableConfiguration],
      pushedFilters: Array[Filter],
      fileScan: GpuFileSourceScanExec): PartitionReaderFactory = {
    val metadataColumns = fileScan.requiredSchema.fieldNames
      .exists(name => name == METADATA_ROW_IDX_COL || name == METADATA_ROW_DEL_COL)
    if (metadataColumns) {
      GpuDelta24xParquetMultiFilePartitionReaderFactory(
        fileScan.conf,
        broadcastedConf,
        prepareSchema(fileScan.relation.dataSchema),
        prepareSchema(fileScan.requiredSchema),
        prepareSchema(fileScan.readPartitionSchema),
        if (disablePushDown) Array.empty else pushedFilters,
        fileScan.rapidsConf,
        ThreadPoolConfBuilder(fileScan.rapidsConf),
        fileScan.allMetrics,
        fileScan.queryUsesInputFile,
        broadcastDvMap)
    } else {
      super.createMultiFileReaderFactory(broadcastedConf, pushedFilters, fileScan)
    }
  }

  private case class GpuDelta24xParquetMultiFilePartitionReaderFactory(
      @transient sqlConf: SQLConf,
      broadcastedConf: Broadcast[SerializableConfiguration],
      dataSchema: StructType,
      readDataSchema: StructType,
      partitionSchema: StructType,
      filters: Array[Filter],
      @transient rapidsConf: RapidsConf,
      poolConfBuilder: ThreadPoolConfBuilder,
      metrics: Map[String, GpuMetric],
      queryUsesInputFile: Boolean,
      delVecs: Option[Broadcast[Map[URI, DeletionVectorDescriptorWithFilterType]]])
    extends AbstractGpuParquetMultiFilePartitionReaderFactory(sqlConf, broadcastedConf,
      dataSchema, readDataSchema, partitionSchema, filters, rapidsConf, poolConfBuilder,
      metrics, queryUsesInputFile) {

    private val delVectorScatterBatchSize =
      RapidsConf.DELTA_LOW_SHUFFLE_MERGE_SCATTER_DEL_VECTOR_BATCH_SIZE.get(sqlConf)

    // Low shuffle metadata generation tracks file boundaries in the multithreaded reader.
    // Force that reader when the session configuration would otherwise select coalescing.
    override val canUseCoalesceFilesReader: Boolean = false
    override val canUseMultiThreadReader: Boolean = true

    override protected def createBaseMultiFileCloudReader(
        fileIO: RapidsFileIO,
        conf: Configuration,
        files: Array[PartitionedFile],
        filterFunc: PartitionedFile => ParquetFileInfoWithBlockMeta,
        isSchemaCaseSensitive: Boolean,
        debugDumpPrefix: Option[String],
        debugDumpAlways: Boolean,
        maxReadBatchSizeRows: Integer,
        maxReadBatchSizeBytes: Long,
        targetBatchSizeBytes: Long,
        maxGpuColumnSizeBytes: Long,
        useChunkedReader: Boolean,
        maxChunkedReaderMemoryUsageSizeBytes: Long,
        compressCfg: CpuCompressionConfig,
        execMetrics: Map[String, GpuMetric],
        partitionSchema: StructType,
        poolConf: ThreadPoolConf,
        maxNumFileProcessed: Int,
        ignoreMissingFiles: Boolean,
        ignoreCorruptFiles: Boolean,
        useFieldId: Boolean,
        queryUsesInputFile: Boolean,
        keepReadsInOrder: Boolean,
        combineConf: CombineConf): AbstractMultiFileCloudParquetPartitionReader = {
      new MultiFileCloudDelta24xParquetPartitionReader(
        fileIO, conf, files, filterFunc, isSchemaCaseSensitive, debugDumpPrefix,
        debugDumpAlways, maxReadBatchSizeRows, maxReadBatchSizeBytes, targetBatchSizeBytes,
        maxGpuColumnSizeBytes, useChunkedReader, maxChunkedReaderMemoryUsageSizeBytes,
        compressCfg, execMetrics, partitionSchema, poolConf, maxNumFileProcessed,
        ignoreMissingFiles, ignoreCorruptFiles, useFieldId, queryUsesInputFile,
        keepReadsInOrder, combineConf, delVecs, delVectorScatterBatchSize)
    }
  }

  private class MultiFileCloudDelta24xParquetPartitionReader(
      fileIO: RapidsFileIO,
      conf: Configuration,
      files: Array[PartitionedFile],
      filterFunc: PartitionedFile => ParquetFileInfoWithBlockMeta,
      isSchemaCaseSensitive: Boolean,
      debugDumpPrefix: Option[String],
      debugDumpAlways: Boolean,
      maxReadBatchSizeRows: Integer,
      maxReadBatchSizeBytes: Long,
      targetBatchSizeBytes: Long,
      maxGpuColumnSizeBytes: Long,
      useChunkedReader: Boolean,
      maxChunkedReaderMemoryUsageSizeBytes: Long,
      compressCfg: CpuCompressionConfig,
      execMetrics: Map[String, GpuMetric],
      partitionSchema: StructType,
      poolConf: ThreadPoolConf,
      maxNumFileProcessed: Int,
      ignoreMissingFiles: Boolean,
      ignoreCorruptFiles: Boolean,
      useFieldId: Boolean,
      queryUsesInputFile: Boolean,
      keepReadsInOrder: Boolean,
      combineConf: CombineConf,
      delVecs: Option[Broadcast[Map[URI, DeletionVectorDescriptorWithFilterType]]],
      delVectorScatterBatchSize: Int)
    extends MultiFileCloudParquetPartitionReader(fileIO, conf, files, filterFunc,
      isSchemaCaseSensitive, debugDumpPrefix, debugDumpAlways, maxReadBatchSizeRows,
      maxReadBatchSizeBytes, targetBatchSizeBytes, maxGpuColumnSizeBytes, useChunkedReader,
      maxChunkedReaderMemoryUsageSizeBytes, compressCfg, execMetrics, partitionSchema,
      poolConf, maxNumFileProcessed, ignoreMissingFiles, ignoreCorruptFiles, useFieldId,
      queryUsesInputFile, keepReadsInOrder, combineConf) {

    private case class Delta24xParquetHostMemoryEmptyMetaData(
        override val partitionedFile: PartitionedFile,
        bufferSize: Long,
        override val bytesRead: Long,
        dateRebaseMode: DateTimeRebaseMode,
        timestampRebaseMode: DateTimeRebaseMode,
        hasInt96Timestamps: Boolean,
        clippedSchema: MessageType,
        readSchema: StructType,
        numRows: Long,
        rowRanges: Seq[FileRowRange],
        override val allPartValues: Option[Array[(Long, InternalRow)]] = None)
      extends HostMemoryEmptyMetaData

    private case class Delta24xParquetHostMemoryBuffersWithMetaData(
        override val partitionedFile: PartitionedFile,
        override val memBuffersAndSizes: Array[SingleHMBAndMeta],
        override val bytesRead: Long,
        dateRebaseMode: DateTimeRebaseMode,
        timestampRebaseMode: DateTimeRebaseMode,
        hasInt96Timestamps: Boolean,
        clippedSchema: MessageType,
        readSchema: StructType,
        override val allPartValues: Option[Array[(Long, InternalRow)]],
        rowRanges: Array[Seq[FileRowRange]])
      extends HostMemoryBuffersWithMetaData {

      override def consumeHeadBuffer(): HostMemoryBuffersWithMetaData = {
        require(memBuffersAndSizes.length == rowRanges.length,
          "Parquet buffers and low shuffle row metadata must be aligned")
        this.copy(
          memBuffersAndSizes = memBuffersAndSizes.drop(1),
          rowRanges = rowRanges.drop(1))
      }
    }

    private def deletionVector(
        file: PartitionedFile): Option[org.roaringbitmap.longlong.Roaring64Bitmap] = {
      delVecs.flatMap(_.value.get(new URI(file.filePath.toString()))).map { dv =>
        execMetrics(DELETION_VECTOR_SIZE) += dv.descriptor.inlineData.length
        RoaringBitmapWrapper.deserializeFromBytes(dv.descriptor.inlineData).inner
      }
    }

    override protected def addExtraColumnsToBatches(
        input: Iterator[ColumnarBatch],
        metadata: HostMemoryBuffersWithMetaDataBase): Iterator[ColumnarBatch] = {
      val (schema, rowRanges) = metadata match {
        case meta: Delta24xParquetHostMemoryBuffersWithMetaData =>
          (meta.readSchema, meta.rowRanges.head)
        case meta: Delta24xParquetHostMemoryEmptyMetaData =>
          (meta.readSchema, meta.rowRanges)
        case other =>
          throw new IllegalArgumentException(s"Unexpected Parquet metadata type ${other.getClass}")
      }
      addMetadataColumnToIterator(schema, rowRanges, input,
        delVectorScatterBatchSize,
        execMetrics(DELETION_VECTOR_SCATTER_TIME))
    }

    override protected def newHMBWithMetaDataForChunks(
        partitionedFile: PartitionedFile,
        memBuffersAndSizes: Array[SingleHMBAndMeta],
        bytesRead: Long,
        fileBlockMeta: ParquetFileInfoWithBlockMeta): HostMemoryBuffersWithMetaData = {
      val delVector = deletionVector(partitionedFile)
      var rowIndex = 0L
      val rowRanges = memBuffersAndSizes.map { buffer =>
        val range = FileRowRange(rowIndex, buffer.numRows, delVector)
        rowIndex += buffer.numRows
        Seq(range)
      }
      Delta24xParquetHostMemoryBuffersWithMetaData(
        partitionedFile, memBuffersAndSizes, bytesRead, fileBlockMeta.dateRebaseMode,
        fileBlockMeta.timestampRebaseMode, fileBlockMeta.hasInt96Timestamps,
        fileBlockMeta.schema, fileBlockMeta.readSchema, None, rowRanges)
    }

    override protected def newCombinedHMBWithMetaData(
        combinedMeta: CombinedMeta,
        newHmbBufferInfo: SingleHMBAndMeta,
        offset: Long): HostMemoryBuffersWithMetaData = {
      val metaToUse = combinedMeta.firstNonEmpty
      val combinedRanges = combinedMeta.toCombine.iterator.flatMap {
        case meta: Delta24xParquetHostMemoryBuffersWithMetaData =>
          meta.rowRanges.iterator.flatten
        case meta: Delta24xParquetHostMemoryEmptyMetaData => meta.rowRanges.iterator
        case other =>
          throw new IllegalArgumentException(s"Unexpected Parquet metadata type ${other.getClass}")
      }.toSeq
      Delta24xParquetHostMemoryBuffersWithMetaData(
        metaToUse.partitionedFile, Array(newHmbBufferInfo), offset,
        metaToUse.dateRebaseMode, metaToUse.timestampRebaseMode, metaToUse.hasInt96Timestamps,
        metaToUse.clippedSchema, metaToUse.readSchema, Some(combinedMeta.allPartValues),
        Array(combinedRanges))
    }

    override protected def newHMEmptyMetadataForChunks(
        partitionedFile: PartitionedFile,
        bufferSize: Long,
        bytesRead: Long,
        dateRebaseMode: DateTimeRebaseMode,
        timestampRebaseMode: DateTimeRebaseMode,
        hasInt96Timestamps: Boolean,
        clippedSchema: MessageType,
        readSchema: StructType,
        numRows: Long,
        blocks: collection.Seq[BlockMetaData]): HostMemoryEmptyMetaData = {
      val rowRanges = if (numRows > 0) {
        Seq(FileRowRange(0, numRows, deletionVector(partitionedFile)))
      } else {
        Seq.empty
      }
      Delta24xParquetHostMemoryEmptyMetaData(
        partitionedFile, bufferSize, bytesRead, dateRebaseMode, timestampRebaseMode,
        hasInt96Timestamps, clippedSchema, readSchema, numRows, rowRanges)
    }

    override protected def newCombinedHMEmptyMetadata(
        emptyMeta: CombinedEmptyMeta,
        nonEmptyMeta: CombinedMeta): HostMemoryEmptyMetaData = {
      val metaToUse = emptyMeta.metaForEmpty
        .asInstanceOf[Delta24xParquetHostMemoryEmptyMetaData]
      val combinedRanges = emptyMeta.emptyMetas.flatMap {
        _.asInstanceOf[Delta24xParquetHostMemoryEmptyMetaData].rowRanges
      }.toSeq
      Delta24xParquetHostMemoryEmptyMetaData(
        metaToUse.partitionedFile, emptyMeta.emptyBufferSize, emptyMeta.emptyTotalBytesRead,
        metaToUse.dateRebaseMode, metaToUse.timestampRebaseMode, metaToUse.hasInt96Timestamps,
        metaToUse.clippedSchema, metaToUse.readSchema, emptyMeta.emptyNumRows, combinedRanges,
        Some(nonEmptyMeta.allPartValues))
    }
  }

  /**
   * We sometimes need to replace FileFormat within LogicalPlans, so we have to override
   * `equals` to ensure file format changes are captured
   */
  override def equals(other: Any): Boolean = {
    other match {
      case ff: GpuDelta24xParquetFileFormat =>
        ff.columnMappingMode == columnMappingMode &&
          ff.referenceSchema == referenceSchema &&
          ff.isSplittable == isSplittable
      case _ => false
    }
  }

  override def hashCode(): Int = getClass.getCanonicalName.hashCode()
}
