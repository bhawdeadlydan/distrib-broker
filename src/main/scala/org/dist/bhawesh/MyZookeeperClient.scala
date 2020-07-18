package org.dist.bhawesh

import com.fasterxml.jackson.core.`type`.TypeReference
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.{ZkNoNodeException, ZkNodeExistsException}
import org.dist.simplekafka.{ControllerExistsException, PartitionReplicas}
import org.dist.simplekafka.common.JsonSerDes
import org.dist.simplekafka.util.ZkUtils
import org.dist.simplekafka.util.ZkUtils.Broker

import scala.jdk.CollectionConverters._

class MyZookeeperClient(zkClient: ZkClient) {

  def createPersistentPath(zkClient: ZkClient, topicsPath: String, topicsData: String): Unit = {
    try {
      zkClient.createPersistent(topicsPath, topicsData)
    } catch {
      case e: ZkNoNodeException => {
        createParentPath(zkClient, topicsPath)
        zkClient.createPersistent(topicsPath, topicsData)
      }
    }
  }

  def setPartitionReplicasForTopic(topicName: String, partitionReplicas: Set[PartitionReplicas]): Unit = {
    val topicsPath = getTopicPath(topicName)
    val topicsData = JsonSerDes.serialize(partitionReplicas)
    createPersistentPath(zkClient, topicsPath, topicsData)
  }

  def getAllBrokerIds(): Set[Int] = {
    zkClient.getChildren(BrokerIdsPath).asScala.map(_.toInt).toSet
  }

  def subscribeTopicChangeListener(topicChangeListener: TopicChangeHandler): Any = {
    zkClient.subscribeChildChanges(TopicPath, topicChangeListener)
  }

  val TopicPath = "/topics"

  def getTopicPath(topicName: String): String = {
    TopicPath + "/" + topicName
  }

  def readPartitionAssignmentsFor(topicName: String): Seq[PartitionReplicas] = {
    val partitionAssignmentInfo: String = zkClient.readData(getTopicPath(topicName))
    JsonSerDes.deserialize[List[PartitionReplicas]](partitionAssignmentInfo.getBytes, new TypeReference[List[PartitionReplicas]]() {})
  }

  def tryCreatingControllerPath(leaderId: String) = {
    try {
      createEphemeralPath(zkClient, ControllerPath, leaderId)
    } catch {
      case exception: ZkNodeExistsException =>
        val electedLeader: String = zkClient.readData(ControllerPath)
        throw ControllerExistsException(electedLeader)
    }
  }

  val BrokerIdsPath = "/brokers/ids"
  val ControllerPath = "/controller"

  def subscribeControllerChangeListener(controllerZookeeper: ControllerZookeeper) = {
    zkClient.subscribeDataChanges(ControllerPath, new ControllerChangeListener(controllerZookeeper, zkClient))
  }

  def getBrokerInfo(brokerId: Int): Broker = {
    val serialisedBrokerInfo: String = zkClient.readData(getBrokerPath(brokerId.toInt))
    JsonSerDes.deserialize(serialisedBrokerInfo.getBytes, classOf[Broker])
  }

  def subscribeBrokerChangeListener(brokerChangeListener: MyBrokerChangeListener) = {
    zkClient.subscribeChildChanges(BrokerIdsPath, brokerChangeListener)
  }

  def getAllBrokers(): Set[Broker] = {
    zkClient.getChildren(BrokerIdsPath).asScala.map {
      brokerId: String => {
        getBrokerInfo(brokerId.toInt)
      }
    }.toSet
  }

  def createParentPath(zkClient: ZkClient, brokerPath: String) = {
    val parentDirectory = brokerPath.substring(0, brokerPath.lastIndexOf('/'))
    if (parentDirectory.length != 0) {
      zkClient.createPersistent(parentDirectory, true)
    }
  }

  def createEphemeralPath(zkClient: ZkClient, brokerPath: String, brokerData: String) = {
    try {
      zkClient.createEphemeral(brokerPath, brokerData)
    }
    catch {
      case e: ZkNoNodeException =>
        createParentPath(zkClient, brokerPath)
        zkClient.createPersistent(brokerPath, brokerData)
    }
  }

  def registerBroker(broker: ZkUtils.Broker) = {
    val brokerData = JsonSerDes.serialize(broker)
    val brokerPath = getBrokerPath(broker.id)
    createEphemeralPath(zkClient, brokerPath, brokerData)
  }


  private def getBrokerPath(id: Int) = {
    BrokerIdsPath + "/" + id
  }

}
