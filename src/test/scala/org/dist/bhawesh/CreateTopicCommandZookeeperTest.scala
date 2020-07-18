package org.dist.bhawesh

import org.dist.common.{TestUtils, ZookeeperTestHarness}
import org.dist.simplekafka.server.Config
import org.dist.util.Networks

class CreateTopicCommandZookeeperTest extends ZookeeperTestHarness {
  test("should create persistent path with paritions being assigned to zookeeper") {
    val config = new Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
  }

}
