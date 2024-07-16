package com.nvidia.spark.rapids

import ai.rapids.cudf.serde.TableSerializer
import ai.rapids.cudf.serde.kudo.KudoSerializer
import com.nvidia.spark.rapids.RapidsConf.conf

import org.apache.spark.sql.internal.SQLConf

object Kudo {
  val SHUFFLE_ENABLE_KUDO = conf("spark.rapids.sql.shuffle.kudo.enable")
    .doc("Eanble kudo shuffle")
    .booleanConf
    .createWithDefault(false)

  def getKudoConf(conf: SQLConf): Option[KudoConf] = {
    val rapidsConf = new RapidsConf(conf)

    if (rapidsConf.get(SHUFFLE_ENABLE_KUDO)) {
      Some(KudoConf())
    } else {
      None
    }
  }
}

case class KudoConf() {
  def serializer(): TableSerializer = new KudoSerializer()
}
