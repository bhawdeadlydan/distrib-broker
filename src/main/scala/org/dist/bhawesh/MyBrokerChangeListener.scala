package org.dist.bhawesh

import java.util

import org.I0Itec.zkclient.IZkChildListener
import org.dist.simplekafka.common.Logging
import org.dist.simplekafka.util.ZkUtils.Broker


class MyBrokerChangeListener(zookeeperClient: MyZookeeperClient) extends IZkChildListener with Logging {
  var liveBrokers: Set[Broker] = Set()

  import scala.jdk.CollectionConverters._

  override def handleChildChange(parentPath: String, currentChilds: util.List[String]): Unit = {
    info("Broker change listener fired for path %s with children %s".format(parentPath, currentChilds.asScala.mkString(",")))
    try {
      val currentBrokerIds: Set[Int] = currentChilds.asScala.map(_.toInt).toSet
      val newBrokerIds: Set[Int] = currentBrokerIds -- getLiveBrokerIds
      val deadBrokerIds: Set[Int] = getLiveBrokerIds -- currentBrokerIds

      newBrokerIds.foreach((brokerId: Int) => {
        liveBrokers = liveBrokers + zookeeperClient.getBrokerInfo(brokerId)
      })

      deadBrokerIds.foreach((brokerId: Int) => {
        val deadBrokers = liveBrokers.filter(b => deadBrokerIds.contains(b.id))
        liveBrokers = liveBrokers -- deadBrokers
      })

      if (newBrokerIds.nonEmpty) {
        info("New Brokers %s added to path %s ".format(newBrokerIds.mkString(","), parentPath))
      }

      if (deadBrokerIds.nonEmpty) {
        info("Brokers %s are dead at %s ".format(newBrokerIds.mkString(","), parentPath))
      }

    } catch {
      case e: Throwable => error("Error while handling broker changes", e)
    }
  }

  private def getLiveBrokerIds: Set[Int] = {
    liveBrokers.map(_.id.toInt)
  }
}
