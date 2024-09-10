package com.nvidia.spark.rapids

import ai.rapids.cudf.serde.TableSerializer
import ai.rapids.cudf.serde.kudo.KudoSerializer
import ai.rapids.cudf.serde.kudo2.CompressionMode
import com.nvidia.spark.rapids.RapidsConf.conf

import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf

object Kudo extends Logging {
  val SHUFFLE_ENABLE_KUDO = conf("spark.rapids.sql.shuffle.kudo.enable")
    .doc("Eanble kudo shuffle")
    .booleanConf
    .createWithDefault(false)

  val SHUFFLE_KUDO_COMPRESSION_MODE = conf("spark.rapids.sql.shuffle.kudo.compression.mode")
    .doc("Compresson mode for kudo shuffle")
    .stringConf
    .createWithDefault("buffer")

  val SHUFFLE_KUDO_BATCH_MIN_COLUMN = conf("spark.rapids.sql.shuffle.kudo.batch.min.column")
    .doc("Minimum number of columns for kudo shuffle to use batch compression mode")
    .integerConf
    .createWithDefault(10)

  val SHUFFLE_KUDO_BATCH_MAX_SIZE = conf("spark.rapids.sql.shuffle.kudo.batch.max.size")
    .doc("Maximum number of bytes for kudo shuffle to use batch compression mode")
    .longConf
    .createWithDefault(5 * 1024 * 1024)

  val SHUFFLE_KUDO_COMPRESSION_LEVEL = conf("spark.rapids.sql.shuffle.kudo.compression.level")
    .doc("Compression level used in kudo shuffle")
    .integerConf
    .createWithDefault(1)

  def getKudoConf(conf: SQLConf): Option[KudoConf] = {
    getKudoConf(new RapidsConf(conf))
  }

  def getKudoConf(rapidsConf: RapidsConf): Option[KudoConf] = {
    if (rapidsConf.get(SHUFFLE_ENABLE_KUDO)) {
      logWarning("Kudo shuffle is experimental and may not be stable, use at your own risk")
      val mode = CompressionMode.valueOf(rapidsConf.get(SHUFFLE_KUDO_COMPRESSION_MODE).toUpperCase)
      val batchMinColumns = rapidsConf.get(SHUFFLE_KUDO_BATCH_MIN_COLUMN)
      val batchMaxBytes = rapidsConf.get(SHUFFLE_KUDO_BATCH_MAX_SIZE)
      val compressionLevel = rapidsConf.get(SHUFFLE_KUDO_COMPRESSION_LEVEL)
      Some(KudoConf(mode, batchMinColumns, batchMaxBytes, compressionLevel))
    } else {
      None
    }
  }
}

case class KudoConf(compressMode: CompressionMode, columnBatchMinCol: Int,
    columnBatchMaxSize: Long, compressionLevel: Int) {
  def serializer(): TableSerializer = new KudoSerializer()
}