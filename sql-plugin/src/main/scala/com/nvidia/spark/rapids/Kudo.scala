package com.nvidia.spark.rapids

import com.nvidia.spark.rapids.RapidsConf.conf
import com.nvidia.spark.rapids.shuffle.kudo.KudoSerializer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf

object Kudo extends Logging {
  val SHUFFLE_ENABLE_KUDO = conf("spark.rapids.sql.shuffle.kudo.enable")
    .doc("Eanble kudo shuffle")
    .booleanConf
    .createWithDefault(false)

  def getKudoConf(conf: SQLConf): Option[KudoConf] = {
    val rapidsConf = new RapidsConf(conf)

    if (rapidsConf.get(SHUFFLE_ENABLE_KUDO)) {
      val conf = KudoConf()
      logWarning(s"Kudo shuffle is enabled, which is experimental, " +
        s"version: ${conf.serializer().version()}")
      Some(KudoConf())
    } else {
      None
    }
  }
}

case class KudoConf() {
  def serializer(): KudoSerializer = new KudoSerializer()
}
