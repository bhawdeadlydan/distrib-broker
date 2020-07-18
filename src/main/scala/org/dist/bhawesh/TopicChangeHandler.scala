package org.dist.bhawesh

import java.util

import org.I0Itec.zkclient.IZkChildListener
import org.dist.simplekafka.PartitionReplicas
import org.dist.simplekafka.common.Logging

import scala.jdk.CollectionConverters._

class TopicChangeHandler(myZookeeperClient: MyZookeeperClient, onTopicChange: (String, Seq[PartitionReplicas]) => Unit) extends IZkChildListener with Logging {
  override def handleChildChange(parentPath: String, currentChilds: util.List[String]): Unit = {
    currentChilds.asScala.foreach { topicName =>
      val replicas: Seq[PartitionReplicas] = myZookeeperClient.readPartitionAssignmentsFor(topicName)
      onTopicChange(topicName, replicas)
    }
  }
}
