package org.dist.bhawesh

import org.dist.simplekafka.{ControllerExistsException, PartitionReplicas}

class ControllerZookeeper(zookeeperClient: MyZookeeperClient, val brokerId: Int) {
  var replicas:Seq[PartitionReplicas] = List()
  def onTopicChange(topicName: String, replicas: Seq[PartitionReplicas]): Unit = {

    this.replicas = replicas
  }

  var currentLeader = -1

  def elect() = {
    val leaderId = s"${brokerId}"
    try {
      zookeeperClient.tryCreatingControllerPath(leaderId)
      this.currentLeader = brokerId
    } catch {
      case e: ControllerExistsException =>
        this.currentLeader = e.controllerId.toInt
    }
  }

  def startup() = {
    zookeeperClient.subscribeControllerChangeListener(this)
    zookeeperClient.subscribeTopicChangeListener(this)
    elect()
  }

  def setCurrentLeader(electedLeader: Int) = {
    currentLeader = electedLeader
  }
}
