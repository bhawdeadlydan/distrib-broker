package org.dist.bhawesh

import org.I0Itec.zkclient.ZkClient
import org.dist.common.{TestUtils, ZookeeperTestHarness}
import org.dist.simplekafka.util.ZKStringSerializer
import org.dist.simplekafka.util.ZkUtils.Broker

class MyZooKeeperClientTest extends ZookeeperTestHarness {

  test("should register brokers with zookeeper") {
    val myZookeeperClient = new MyZookeeperClient(zkClient);
    val myZookeeperClient2 = new MyZookeeperClient(zkClient);
    myZookeeperClient.registerBroker(Broker(0, "10.10.10.10", TestUtils.choosePort()))
    myZookeeperClient.registerBroker(Broker(1, "10.10.10.11", TestUtils.choosePort()))
    myZookeeperClient.registerBroker(Broker(2, "10.10.10.12", TestUtils.choosePort()))

    assert(3 == myZookeeperClient2.getAllBrokers.size)
  }
  test("should get notified when broker is registered") {
    val zkClient1 = new ZkClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)
    val myZooKeeperClient1 = new MyZookeeperClient(zkClient1)

    val brokerChangeListener = new MyBrokerChangeListener(myZooKeeperClient1)
    myZooKeeperClient1.subscribeBrokerChangeListener(brokerChangeListener)
    myZooKeeperClient1.registerBroker(Broker(0, "10.10.10.10", TestUtils.choosePort()))

    val zkClient2 = new ZkClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)
    val myZooKeeperClient2 = new MyZookeeperClient(zkClient2)
    myZooKeeperClient2.registerBroker(Broker(1, "10.10.10.11", TestUtils.choosePort()))


    val zkClient3 = new ZkClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)
    val myZooKeeperClient3 = new MyZookeeperClient(zkClient3)
    myZooKeeperClient3.registerBroker(Broker(2, "10.10.10.12", TestUtils.choosePort()))

    TestUtils.waitUntilTrue(() => {
      brokerChangeListener.liveBrokers.size == 3
    }, "Waiting for all brokers to get added", 1000)

    zkClient2.close()

    TestUtils.waitUntilTrue(() => {
      brokerChangeListener.liveBrokers.size == 2
    }, "Waiting for all brokers to get added", 1000)

  }
}
