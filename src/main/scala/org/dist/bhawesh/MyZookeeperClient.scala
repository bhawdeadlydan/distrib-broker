package org.dist.bhawesh

import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.{ZkNoNodeException, ZkNodeExistsException}
import org.dist.simplekafka.ControllerExistsException
import org.dist.simplekafka.common.JsonSerDes
import org.dist.simplekafka.util.ZkUtils
import org.dist.simplekafka.util.ZkUtils.Broker

import scala.jdk.CollectionConverters._

class MyZookeeperClient(zkClient: ZkClient) {
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
