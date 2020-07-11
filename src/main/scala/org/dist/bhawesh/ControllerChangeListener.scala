package org.dist.bhawesh

import org.I0Itec.zkclient.{IZkDataListener, ZkClient}
import org.dist.simplekafka.common.Logging

class ControllerChangeListener(controllerZookeeper: ControllerZookeeper, zkClient: ZkClient) extends IZkDataListener with Logging {
  override def handleDataChange(dataPath: String, data: Any): Unit = {
    val existingControllerId:String = zkClient.readData(dataPath)
    controllerZookeeper.setCurrentLeader(existingControllerId.toInt)
  }

  override def handleDataDeleted(dataPath: String): Unit = {
    controllerZookeeper.elect()
  }
}
