package com.adamkunicki.hbase

import java.io.Closeable
import com.netflix.curator.RetryPolicy
import com.netflix.curator.retry.RetryUntilElapsed
import com.netflix.curator.framework.CuratorFrameworkFactory

class ZookeeperUtils(zookeepers: String) extends Closeable {
  private val totalRetries = 10
  private val secondsBetweenRetries = 1
  private val totalRetryTime = totalRetries * secondsBetweenRetries
  private val retryPolicy: RetryPolicy = new RetryUntilElapsed(totalRetryTime, secondsBetweenRetries)

  protected var curatorClient = CuratorFrameworkFactory.newClient(zookeepers, retryPolicy)

  def close() {
    curatorClient.close()
  }
}

object ZookeeperIdGenerator extends ZookeeperUtils("192.168.141.155") {

}
