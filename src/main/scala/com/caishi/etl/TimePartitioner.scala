package com.caishi.etl

/**
 * Created by root on 15-11-3.
 */
class TimePartitioner(numParts : Int) extends org.apache.spark.Partitioner{
  override def numPartitions: Int = numParts
  override def getPartition(key: Any): Int = {
    val domain = new java.net.URL(key.toString).getHost()
    val code = (domain.hashCode % numPartitions)
    if (code < 0) {
      code + numPartitions
    } else {
      code
    }
  }

  override def equals(other: Any): Boolean = other match {
    case iteblog: TimePartitioner =>
      iteblog.numPartitions == numPartitions
    case _ =>
      false
  }
  override def hashCode: Int = numPartitions
}
