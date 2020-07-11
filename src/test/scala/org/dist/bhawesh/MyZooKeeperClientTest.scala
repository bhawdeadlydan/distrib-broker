package org.dist.bhawesh

import org.dist.common.{TestUtils, ZookeeperTestHarness}
import org.dist.simplekafka.util.ZkUtils.Broker

class MyZooKeeperClientTest extends ZookeeperTestHarness {

  test("should register brokers with zookeeper") {
    val myZookeeperClient = new MyZookeeperClient(zkClient);
    myZookeeperClient.registerBroker(Broker(0, "10.10.10.10", TestUtils.choosePort()))
    myZookeeperClient.registerBroker(Broker(1, "10.10.10.11", TestUtils.choosePort()))

    assert(2 == myZookeeperClient.getAllBrokers().size)
  }
}
