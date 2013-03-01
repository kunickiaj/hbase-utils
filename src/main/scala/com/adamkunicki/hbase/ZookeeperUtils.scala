package com.adamkunicki.hbase

import java.io.{IOException, Closeable}
import com.netflix.curator.RetryPolicy
import com.netflix.curator.retry.RetryUntilElapsed
import com.netflix.curator.framework.CuratorFrameworkFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Get, HTable}
import org.apache.hadoop.hbase.util.Bytes
import com.netflix.curator.framework.recipes.atomic.{DistributedAtomicInteger, CachedAtomicInteger, AtomicValue}

/**
 * Utility class to fetch a unique integer id (bitmap position) for each row indexed.
 * @param conf HBase configuration file
 */
class ZookeeperUtils(conf: Configuration) extends Closeable with Logging with Resources {
  private val cacheSize = 100
  private val totalRetries = 10
  private val secondsBetweenRetries = 1
  private val totalRetryTime = totalRetries * secondsBetweenRetries
  private val retryPolicy: RetryPolicy = new RetryUntilElapsed(totalRetryTime, secondsBetweenRetries)
  private val curatorClient = {
    val client = CuratorFrameworkFactory.newClient(conf.get(Constants.HBASE_ZOOKEEPER_QUORUM), retryPolicy)
    client.start()
    client
  }
  private val cachedInt: CachedAtomicInteger = {
    val path = conf.get(Constants.ZOOKEEPER_ID_PATH)
    val distributedInt = new DistributedAtomicInteger(curatorClient, path, retryPolicy)

    try {
      if (curatorClient.checkExists().forPath(path) == null) {
        // This is the first id
        curatorClient.create().creatingParentsIfNeeded().forPath(path)
        // Reset to zero
        distributedInt.forceSet(0)
      }
      new CachedAtomicInteger(distributedInt, cacheSize)
    } catch {
      case e: IOException => {
        log.error("Failed to create id in Zookeeper", e)
        throw new IOException(e)
      }
    }
  }

  def getIntIdForRow(row: String): Int = {
    // See if we al ready have this row in the index so that we don't duplicate it.
    log.info("Table name: " + conf.get(Constants.HBASE_TABLE_NAME))
    using(new HTable(conf, conf.get(Constants.HBASE_TABLE_NAME)))(table => {
      val get = new Get(Bytes.toBytes(row))
      get.addFamily(Constants.ITEM_INT.getBytes)
      val result = table.get(get)
      if (!result.isEmpty) {
        return Bytes.toInt(result.value())
      }
    })

    // Otherwise grab a new id from Zookeeper
    val intId = cachedInt.next()
    if (!intId.succeeded()) {
      throw new IOException("Failed to acquire new id")
    }
    intId.postValue()
  }

  def close() = {
    curatorClient.close()
  }
}
