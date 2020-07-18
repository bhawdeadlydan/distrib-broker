package org.dist.bhawesh

import org.dist.common.{TestUtils, ZookeeperTestHarness}
import org.dist.simplekafka.PartitionReplicas
import org.dist.simplekafka.util.ZkUtils.Broker

class TopicChangeHandlerTest extends ZookeeperTestHarness {

  test("should register for topic change and get replica assignments") {
    val myZookeeperClient = new MyZookeeperClient(zkClient)
    myZookeeperClient.registerBroker(Broker(0, "10.10.10.10", TestUtils.choosePort()))
    myZookeeperClient.registerBroker(Broker(1, "10.10.10.11", TestUtils.choosePort()))
    myZookeeperClient.registerBroker(Broker(2, "10.10.10.12", TestUtils.choosePort()))

    val createTopicCommand = new CreateTopicCommand(myZookeeperClient)
    val zookeeperController = new ControllerZookeeper(myZookeeperClient, 1)
    zookeeperController.startup()
    createTopicCommand.createTopic(topicName = "topic1", noOfPartitions = 2, replicationFactor = 2)

    TestUtils.waitUntilTrue(() => {
      zookeeperController.replicas.size > 0
    }, "Waiting for topic metadata", 1000)
  }
}
