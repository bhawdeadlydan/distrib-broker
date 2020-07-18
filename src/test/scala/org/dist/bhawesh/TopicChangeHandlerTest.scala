package org.dist.bhawesh

import org.dist.common.{TestUtils, ZookeeperTestHarness}
import org.dist.simplekafka.PartitionReplicas
import org.dist.simplekafka.util.ZkUtils.Broker

class TopicChangeHandlerTest extends ZookeeperTestHarness {
  class TestContext {
    var replicas:Seq[PartitionReplicas] = List()
    def leaderAndIsr(topicName:String, replicas:Seq[PartitionReplicas]) = {
      this.replicas = replicas
    }
  }
  test("should register for topic change and get replica assignments") {
    val myZookeeperClient = new MyZookeeperClient(zkClient)
    myZookeeperClient.registerBroker(Broker(0, "10.10.10.10", TestUtils.choosePort()))
    myZookeeperClient.registerBroker(Broker(1, "10.10.10.11", TestUtils.choosePort()))
    myZookeeperClient.registerBroker(Broker(2, "10.10.10.12", TestUtils.choosePort()))

    val createTopicCommand = new CreateTopicCommand(myZookeeperClient)
    val testContext = new TestContext
    val topicChangeListener  = new TopicChangeHandler(myZookeeperClient, testContext.leaderAndIsr)
    myZookeeperClient.subscribeTopicChangeListener(topicChangeListener)
    createTopicCommand.createTopic(topicName = "topic1", noOfPartitions=2, replicationFactor = 2)

    TestUtils.waitUntilTrue(() => {
      testContext.replicas.size > 0
    }, "Waiting for topic metadata", 1000)
  }

}
