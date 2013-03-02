package com.adamkunicki.hbase

import org.apache.hadoop.hbase.client.HTable

class TermIndex(zookeepers: String, tableName: String) extends BitmapIndex {

  override def createTableHandle: HTable = {
    conf.set(Constants.HBASE_ZOOKEEPER_QUORUM, zookeepers)
    conf.set(Constants.ZOOKEEPER_ID_PATH, "/" + tableName + "/ids")
    conf.set(Constants.HBASE_TABLE_NAME, tableName)
    new HTable(conf, tableName)
  }
}
