package org.dist.bhawesh

import org.dist.simplekafka.ControllerExistsException

class ControllerZookeeper(zookeeperClient: MyZookeeperClient, val brokerId: Int) {
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
    elect()
  }

  def setCurrentLeader(electedLeader: Int) = {
    currentLeader = electedLeader
  }
}
