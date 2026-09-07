/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
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

import java.net.URI

import com.databricks.sql.io.RowIndexFilterType
import com.databricks.sql.transaction.tahoe.{
  DeltaColumnMapping,
  DeltaColumnMappingMode,
  DeltaParquetFileFormat,
  IdMapping,
  NameMapping,
  NoMapping
}
import com.databricks.sql.transaction.tahoe.actions.{Metadata, Protocol}
import com.databricks.sql.transaction.tahoe.files.TahoeFileIndex
import com.databricks.sql.transaction.tahoe.schema.SchemaMergingUtils
import com.nvidia.spark.rapids.{GpuMetric, RapidsConf, SparkPlanMeta}
import com.nvidia.spark.rapids.delta.GpuDeltaParquetFileFormatUtils.{addMetadataColumnsToBatch,
  addMetadataColumnToIterator}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.{FilePartition, HadoopFsRelation, PartitionedFile}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.rapids.{GpuFileSourceScanExec, InputFileUtils}
import org.apache.spark.sql.rapids.shims.TrampolineConnectShims
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{MetadataBuilder, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.SerializableConfiguration

/**
 * GPU Delta Parquet file format for Databricks 17.3.
 *
 * DB-17.3 uses a fundamentally different DV mechanism than DB-14.3:
 * - No broadcastDvMap / broadcastHadoopConf
 * - Per-file DV via PartitionedFile.otherConstantMetadataColumnValues or TahoeFileIndex metadata
 * - The CPU constructor starts with (protocol, metadata, ...); this GPU wrapper also carries the
 *   relation so it can inspect DBR file-index metadata.
 */
case class GpuDeltaParquetFileFormat(
    @transient relation: HadoopFsRelation,
    protocol: Protocol,
    metadata: Metadata,
    generateRowIndexFilterId: Boolean = false,
    generateRowIndexFilterColumn: Boolean = false,
    generateDeltaFileInScanId: Boolean = false,
    nullableRowTrackingConstantFields: Boolean = false,
    nullableRowTrackingGeneratedFields: Boolean = false,
    optimizationsEnabled: Boolean = true,
    tablePath: Option[String] = None,
    isCDCRead: Boolean = false,
    lowShuffleMergeScan: Option[GpuLowShuffleMergeScanInfo] = None
  ) extends GpuDeltaParquetFileFormatBase {

  override val columnMappingMode: DeltaColumnMappingMode = metadata.columnMappingMode
  override val referenceSchema: StructType = metadata.schema

  if (columnMappingMode == IdMapping) {
    val requiredReadConf = SQLConf.PARQUET_FIELD_ID_READ_ENABLED
    require(TrampolineConnectShims.getActiveSession.sessionState.conf.getConf(requiredReadConf),
      s"${requiredReadConf.key} must be enabled to support Delta id column mapping mode")
    val requiredWriteConf = SQLConf.PARQUET_FIELD_ID_WRITE_ENABLED
    require(TrampolineConnectShims.getActiveSession.sessionState.conf.getConf(requiredWriteConf),
      s"${requiredWriteConf.key} must be enabled to support Delta id column mapping mode")
  }

  /**
   * Delta 3.3+ has an extra nested field-id metadata key that must not be passed into the
   * Parquet reader after name mapping rewrites.
   */
  override def prepareSchema(inputSchema: StructType): StructType = {
    val schema = DeltaColumnMapping.createPhysicalSchema(
      inputSchema, referenceSchema, columnMappingMode)
    if (columnMappingMode == NameMapping) {
      SchemaMergingUtils.transformColumns(schema) { (_, field, _) =>
        field.copy(metadata = new MetadataBuilder()
          .withMetadata(field.metadata)
          .remove(DeltaColumnMapping.PARQUET_FIELD_ID_METADATA_KEY)
          .remove(DeltaColumnMapping.PARQUET_FIELD_NESTED_IDS_METADATA_KEY)
          .build())
      }
    } else {
      schema
    }
  }

  /**
   * Helper method copied from Apache Spark
   * sql/catalyst/src/main/scala/org/apache/spark/sql/connector/catalog/CatalogV2Implicits.scala
   */
  private def quoteIfNeeded(part: String): String = {
    if (part.matches("[a-zA-Z0-9_]+") && !part.matches("\\d+")) {
      part
    } else {
      s"`${part.replace("`", "``")}`"
    }
  }

  /**
   * Translates pushed filters to physical column names when Delta column mapping is enabled.
   */
  private def prepareFiltersForRead(filters: Seq[Filter]): Seq[Filter] = {
    if (lowShuffleMergeScan.isDefined || !effectiveOptimizationsEnabled) {
      Seq.empty
    } else if (columnMappingMode != NoMapping) {
      val physicalNameMap = DeltaColumnMapping.getLogicalNameToPhysicalNameMap(referenceSchema)
        .map {
          case (logicalName, physicalName) =>
            (logicalName.map(quoteIfNeeded).mkString("."),
              physicalName.map(quoteIfNeeded).mkString("."))
        }
      filters.flatMap(RapidsDeletionVectors.translateFilterForColumnMapping(_, physicalNameMap))
    } else {
      filters
    }
  }

  override def isSplitable(
      sparkSession: SparkSession,
      options: Map[String, String],
      path: Path): Boolean = lowShuffleMergeScan.isEmpty && effectiveOptimizationsEnabled

  private def hasDeletionVectorRead: Boolean =
    GpuDeltaParquetFileFormat.isDeletionVectorRead(
      generateRowIndexFilterId, generateRowIndexFilterColumn, tablePath)

  @transient private lazy val tahoeFileIndexOpt: Option[TahoeFileIndex] = relation.location match {
    case t: TahoeFileIndex => Some(t)
    case _ => None
  }

  private lazy val hasDeletionVectorsInTahoeFileIndex: Boolean = {
    tahoeFileIndexOpt.exists { tahoeFileIndex =>
      tahoeFileIndex.rowIndexFilters.exists(_.nonEmpty) ||
        tahoeFileIndex
          .matchingFiles(partitionFilters = Seq(TrueLiteral), dataFilters = Seq(TrueLiteral))
          .exists(_.deletionVector != null)
    }
  }

  private def effectiveOptimizationsEnabled: Boolean =
    optimizationsEnabled && !hasDeletionVectorRead && !hasDeletionVectorsInTahoeFileIndex

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
      prepareFiltersForRead(filters),
      options,
      hadoopConf,
      metrics)

    lowShuffleMergeScan.map { scanInfo =>
      val maxBatchSize = RapidsConf.DELTA_LOW_SHUFFLE_MERGE_SCATTER_DEL_VECTOR_BATCH_SIZE
        .get(sparkSession.sessionState.conf)
      val scatterTime = metrics(GpuMetric.DELETION_VECTOR_SCATTER_TIME)
      val deletionVectorSize = metrics(GpuMetric.DELETION_VECTOR_SIZE)
      (file: PartitionedFile) => {
        val bitmap = lookupBitmap(scanInfo, file.filePath.toString, deletionVectorSize)
        addMetadataColumnToIterator(
          prepareSchema(requiredSchema),
          bitmap,
          dataReader(file).asInstanceOf[Iterator[ColumnarBatch]],
          maxBatchSize,
          scatterTime).asInstanceOf[Iterator[InternalRow]]
      }
    }.getOrElse(dataReader)
  }

  override def createMultiFileReaderFactory(
      broadcastedConf: Broadcast[SerializableConfiguration],
      pushedFilters: Array[Filter],
      fileScan: GpuFileSourceScanExec): PartitionReaderFactory = {
    lowShuffleMergeScan.map { scanInfo =>
      // Low-shuffle metadata is file-relative. Prevent the coalescing reader from combining
      // rows from multiple files into one batch; the multithreaded reader remains enabled.
      val delegate = super.createMultiFileReaderFactory(
        broadcastedConf,
        pushedFilters,
        fileScan.copy(queryUsesInputFile = true)(fileScan.rapidsConf))
      new LowShuffleMergePartitionReaderFactory(
        delegate,
        prepareSchema(fileScan.requiredSchema),
        scanInfo,
        fileScan.rapidsConf.get(
          RapidsConf.DELTA_LOW_SHUFFLE_MERGE_SCATTER_DEL_VECTOR_BATCH_SIZE),
        fileScan.allMetrics)
    }.getOrElse(super.createMultiFileReaderFactory(broadcastedConf, pushedFilters, fileScan))
  }

  private def lookupBitmap(
      scanInfo: GpuLowShuffleMergeScanInfo,
      path: String,
      deletionVectorSize: GpuMetric): Option[org.roaringbitmap.longlong.Roaring64Bitmap] = {
    scanInfo.rowIndexMaps.flatMap { broadcast =>
      broadcast.value.get(new URI(path)).map { bytes =>
        deletionVectorSize += bytes.length
        RoaringBitmapWrapper.deserializeFromBytes(bytes).inner
      }
    }
  }

  private class LowShuffleMergePartitionReaderFactory(
      delegate: PartitionReaderFactory,
      readSchema: StructType,
      scanInfo: GpuLowShuffleMergeScanInfo,
      maxScatterBatchSize: Int,
      metrics: Map[String, GpuMetric]) extends PartitionReaderFactory {

    override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
      delegate.createReader(partition)

    override def supportColumnarReads(partition: InputPartition): Boolean = true

    override def createColumnarReader(
      partition: InputPartition): PartitionReader[ColumnarBatch] = {
      val files = partition.asInstanceOf[FilePartition].filesWithAbsolutePaths
      val byPath = files.flatMap { file =>
        Seq(file.filePath.toString -> file, file.urlEncodedPath -> file)
      }.toMap
      new LowShuffleMergePartitionReader(
        delegate.createColumnarReader(partition),
        byPath,
        readSchema,
        scanInfo,
        maxScatterBatchSize,
        metrics)
    }
  }

  private class LowShuffleMergePartitionReader(
      delegate: PartitionReader[ColumnarBatch],
      files: Map[String, PartitionedFile],
      readSchema: StructType,
      scanInfo: GpuLowShuffleMergeScanInfo,
      maxScatterBatchSize: Int,
      metrics: Map[String, GpuMetric]) extends PartitionReader[ColumnarBatch] {

    private var currentFile: PartitionedFile = _
    private var currentBitmap: Option[org.roaringbitmap.longlong.Roaring64Bitmap] = None
    private var rowIndex = 0L

    override def next(): Boolean = delegate.next()

    override def get(): ColumnarBatch = {
      val batch = delegate.get()
      val inputPath = InputFileUtils.getCurInputFilePath()
      val inputStart = InputFileUtils.getCurInputFileStartOffset
      val inputLength = InputFileUtils.getCurInputFileLength
      if (currentFile == null ||
          (currentFile.filePath.toString != inputPath && currentFile.urlEncodedPath != inputPath) ||
          currentFile.start != inputStart ||
          currentFile.length != inputLength) {
        currentFile = files.getOrElse(inputPath,
          throw new IllegalStateException(s"Unknown low-shuffle input file $inputPath"))
        rowIndex = 0L
        currentBitmap = lookupBitmap(
          scanInfo, currentFile.filePath.toString, metrics(GpuMetric.DELETION_VECTOR_SIZE))
      }
      val numRows = batch.numRows()
      val result = addMetadataColumnsToBatch(
        readSchema,
        currentBitmap,
        batch,
        maxScatterBatchSize,
        rowIndex,
        metrics(GpuMetric.DELETION_VECTOR_SCATTER_TIME))
      rowIndex += numRows
      result
    }

    override def close(): Unit = delegate.close()
  }
}

object GpuDeltaParquetFileFormat {
  private[delta] val EDGE_COMPUTED_COLUMN_SKIP_ROW =
    "_databricks_internal_edge_computed_column_skip_row"

  def isDeletionVectorRead(format: DeltaParquetFileFormat): Boolean =
    isDeletionVectorRead(
      format.generateRowIndexFilterId,
      format.generateRowIndexFilterColumn,
      format.tablePath)

  def isDeletionVectorRead(
      generateRowIndexFilterId: Boolean,
      generateRowIndexFilterColumn: Boolean,
      tablePath: Option[String]): Boolean =
    generateRowIndexFilterId || generateRowIndexFilterColumn || tablePath.isDefined

  def tagSupportForGpuFileSourceScan(meta: SparkPlanMeta[FileSourceScanExec]): Unit = {
    val requiredSchema = meta.wrapped.requiredSchema
    val hasDeletionVectorSkipRowColumn = requiredSchema.exists { field =>
      field.name == EDGE_COMPUTED_COLUMN_SKIP_ROW ||
        field.name == DeltaParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME
    }
    val isGpuDvFilterScan = DeltaSpark400DB173Provider.isDVScan(meta)
    if (requiredSchema.exists { field =>
      field.name.startsWith("_databricks_internal") &&
        field.name != EDGE_COMPUTED_COLUMN_SKIP_ROW
    }) {
      meta.willNotWorkOnGpu(
        s"reading metadata columns starting with prefix _databricks_internal is not supported")
    }
    val format = meta.wrapped.relation.fileFormat.asInstanceOf[DeltaParquetFileFormat]
    val hasRowIndexFilters = hasRowIndexFiltersInTahoeFileIndex(meta.wrapped.relation)
    if (hasDeletionVectorSkipRowColumn &&
        DeltaSpark400DB173Provider.isDmlMetricDVScan(meta)) {
      meta.willNotWorkOnGpu(
        "Delta deletion vector skip-row columns are not supported on GPU for DB-17.3")
    }
    if (hasUnsupportedRowIndexFiltersInTahoeFileIndex(meta.wrapped.relation)) {
      meta.willNotWorkOnGpu(
        "DB-17.3 native deletion vector reads on GPU support only IF_CONTAINED " +
          "row-index filters")
    }
    if (isDeletionVectorRead(format) ||
        hasDeletionVectorSkipRowColumn ||
        isGpuDvFilterScan ||
        hasRowIndexFilters) {
      if (format.isCDCRead) {
        meta.willNotWorkOnGpu(
          "CDC reads with deletion vectors are not yet supported on GPU for DB-17.3")
      }
      if (!DeltaSpark400DB173Provider.isPushDVPredicateDownEnabled(meta.conf)) {
        meta.willNotWorkOnGpu(
          "DB-17.3 deletion vector reads on GPU require native cuDF deletion-vector " +
            "support with spark.databricks.delta.deletionVectors.useMetadataRowIndex " +
            "and spark.rapids.sql.delta.deletionVectors.predicatePushdown.enabled set to true")
      }
      if (!format.optimizationsEnabled) {
        meta.willNotWorkOnGpu(
          "DB-17.3 native deletion vector reads require Delta scan optimizations enabled")
      }
    }
    if (format.generateDeltaFileInScanId) {
      meta.willNotWorkOnGpu(
        "Delta file-in-scan metadata columns are not supported on GPU for DB-17.3")
    }
    if (format.nullableRowTrackingConstantFields) {
      meta.willNotWorkOnGpu(
        "nullable Delta row-tracking constant fields are not supported on GPU for DB-17.3")
    }
    if (format.nullableRowTrackingGeneratedFields) {
      meta.willNotWorkOnGpu(
        "nullable Delta row-tracking generated fields are not supported on GPU for DB-17.3")
    }
  }

  /**
   * Convert a CPU Delta Parquet file format to the GPU version.
   * Called from DatabricksDeltaProviderBase.getReadFileFormat.
   */
  def convertToGpu(relation: HadoopFsRelation): GpuDeltaParquetFileFormat = {
    val fmt = relation.fileFormat.asInstanceOf[DeltaParquetFileFormat]
    GpuDeltaParquetFileFormat(
      relation = relation,
      protocol = fmt.protocol,
      metadata = fmt.metadata,
      generateRowIndexFilterId = fmt.generateRowIndexFilterId,
      generateRowIndexFilterColumn = fmt.generateRowIndexFilterColumn,
      generateDeltaFileInScanId = fmt.generateDeltaFileInScanId,
      nullableRowTrackingConstantFields = fmt.nullableRowTrackingConstantFields,
      nullableRowTrackingGeneratedFields = fmt.nullableRowTrackingGeneratedFields,
      optimizationsEnabled = fmt.optimizationsEnabled,
      tablePath = fmt.tablePath,
      isCDCRead = fmt.isCDCRead,
      lowShuffleMergeScan = GpuLowShuffleMergeScanRegistry.lookup(relation.options))
  }

  private def hasRowIndexFiltersInTahoeFileIndex(relation: HadoopFsRelation): Boolean = {
    relation.location match {
      case tahoeFileIndex: TahoeFileIndex =>
        tahoeFileIndex.rowIndexFilters.exists(_.nonEmpty)
      case _ => false
    }
  }

  private def hasUnsupportedRowIndexFiltersInTahoeFileIndex(
      relation: HadoopFsRelation): Boolean = {
    relation.location match {
      case tahoeFileIndex: TahoeFileIndex =>
        val hasUnsupportedCachedFilter = tahoeFileIndex.rowIndexFilters.exists { filters =>
          filters.values.exists(_.getRowIndexFilterType != RowIndexFilterType.IF_CONTAINED)
        }
        hasUnsupportedCachedFilter || tahoeFileIndex.matchingFiles(
          Seq(TrueLiteral), Seq(TrueLiteral)).exists { addFile =>
          val provider = try {
            tahoeFileIndex.getRowIndexFilterForFile(addFile.path)
          } catch {
            case e: AssertionError
                if RapidsDeletionVectors.isMissingRowIndexFilterAssertion(e) =>
              None
          }
          provider.exists {
            _.getRowIndexFilterType != RowIndexFilterType.IF_CONTAINED
          }
        }
      case _ => false
    }
  }
}
