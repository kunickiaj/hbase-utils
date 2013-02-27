package com.adamkunicki.hbase

import org.apache.hadoop.hbase.client.HTable

class TermIndex(zookeepers: String, tableName: String) extends BitmapIndex {

  override def createTableHandle: HTable = {
    conf.set("hbase.zookeeper.quorum", zookeepers)
    new HTable(conf, tableName)
  }

  def addToIndex(item: Any, terms: Seq[String]) {

  }

  def getItemsMatchingAllTerms(terms: Seq[String]): Seq[Byte] = List()

  def getItemsMatchingAnyTerms(terms: Seq[String]): Seq[Byte] = List()

  def getItemsMatchingNoTerms(terms: Seq[String]): Seq[Byte] = List()
}
