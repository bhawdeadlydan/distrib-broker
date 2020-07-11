package org.dist.bhawesh

import org.I0Itec.zkclient.ZkClient
import org.dist.common.{TestUtils, ZookeeperTestHarness}
import org.dist.simplekafka.util.ZKStringSerializer
import org.dist.simplekafka.util.ZkUtils.Broker

class ControllerZookeeperTest extends ZookeeperTestHarness {
  test("should elect first one as leader Controller") {
    val zkClient1 = new ZkClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)
    val myZookeeperClient1 = new MyZookeeperClient(zkClient1)
    val broker1 = Broker(1, "10.10.10.10", TestUtils.choosePort())
    myZookeeperClient1.registerBroker(broker1)

    val myController1 = new ControllerZookeeper(myZookeeperClient1, broker1.id)
    myController1.startup()

    val zkClient2 = new ZkClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, ZKStringSerializer)
    val myZookeeperClient2 = new MyZookeeperClient(zkClient2)
    val broker2 = Broker(2, "10.10.10.11", TestUtils.choosePort())
    myZookeeperClient2.registerBroker(broker2)

    val myController2 = new ControllerZookeeper(myZookeeperClient2, broker2.id)
    myController2.startup()

    TestUtils.waitUntilTrue(() => {
      myController1.currentLeader == broker1.id
    } ,"Waiting for first controller to get elected", 500)
    TestUtils.waitUntilTrue(() => {
      myController2.currentLeader == broker1.id
    } ,"Waiting for first controller to get elected", 500)

    zkClient1.close()
    TestUtils.waitUntilTrue(() => {
      myController2.currentLeader == broker2.id
    }, "Waiting for second controller to get elected", 500)
  }
}
