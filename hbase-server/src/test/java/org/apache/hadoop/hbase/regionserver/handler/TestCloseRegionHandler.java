/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver.handler;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.RegionTransition;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coordination.OpenRegionCoordination;
import org.apache.hadoop.hbase.coordination.ZkCoordinatedStateManager;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.coordination.ZkCloseRegionCoordination;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MockServer;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

/**
 * Test of the {@link CloseRegionHandler}.
 */
@Category(MediumTests.class)
public class TestCloseRegionHandler {
  static final Log LOG = LogFactory.getLog(TestCloseRegionHandler.class);
  private final static HBaseTestingUtility HTU = HBaseTestingUtility.createLocalHTU();
  private static final HTableDescriptor TEST_HTD =
    new HTableDescriptor(TableName.valueOf("TestCloseRegionHandler"));
  private HRegionInfo TEST_HRI;
  private int testIndex = 0;

  @BeforeClass public static void before() throws Exception {
    HTU.getConfiguration().setBoolean("hbase.assignment.usezk", true);
    HTU.startMiniZKCluster();
  }

  @AfterClass public static void after() throws IOException {
    HTU.shutdownMiniZKCluster();
  }

  /**
   * Before each test, use a different HRI, so the different tests
   * don't interfere with each other. This allows us to use just
   * a single ZK cluster for the whole suite.
   */
  @Before
  public void setupHRI() {
    TEST_HRI = new HRegionInfo(TEST_HTD.getTableName(),
      Bytes.toBytes(testIndex),
      Bytes.toBytes(testIndex + 1));
    testIndex++;
  }

  /**
   * Test that if we fail a flush, abort gets set on close.
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-4270">HBASE-4270</a>
   * @throws IOException
   * @throws NodeExistsException
   * @throws KeeperException
   */
  @Test public void testFailedFlushAborts()
  throws IOException, NodeExistsException, KeeperException {
    final Server server = new MockServer(HTU, false);
    final RegionServerServices rss = HTU.createMockRegionServerService();
    HTableDescriptor htd = TEST_HTD;
    final HRegionInfo hri =
      new HRegionInfo(htd.getTableName(), HConstants.EMPTY_END_ROW,
        HConstants.EMPTY_END_ROW);
    HRegion region = HTU.createLocalHRegion(hri,  htd);
    try {
      assertNotNull(region);
      // Spy on the region so can throw exception when close is called.
      HRegion spy = Mockito.spy(region);
      final boolean abort = false;
      Mockito.when(spy.close(abort)).
      thenThrow(new IOException("Mocked failed close!"));
      // The CloseRegionHandler will try to get an HRegion that corresponds
      // to the passed hri -- so insert the region into the online region Set.
      rss.addToOnlineRegions(spy);
      // Assert the Server is NOT stopped before we call close region.
      assertFalse(server.isStopped());

      ZkCoordinatedStateManager consensusProvider = new ZkCoordinatedStateManager();
      consensusProvider.initialize(server);
      consensusProvider.start();

      ZkCloseRegionCoordination.ZkCloseRegionDetails zkCrd =
        new ZkCloseRegionCoordination.ZkCloseRegionDetails();
      zkCrd.setPublishStatusInZk(false);
      zkCrd.setExpectedVersion(-1);

      CloseRegionHandler handler = new CloseRegionHandler(server, rss, hri, false,
            consensusProvider.getCloseRegionCoordination(), zkCrd);
      boolean throwable = false;
      try {
        handler.process();
      } catch (Throwable t) {
        throwable = true;
      } finally {
        assertTrue(throwable);
        // Abort calls stop so stopped flag should be set.
        assertTrue(server.isStopped());
      }
    } finally {
      HRegion.closeHRegion(region);
    }
  }

     /**
      * Test if close region can handle ZK closing node version mismatch
      * @throws IOException
      * @throws NodeExistsException
      * @throws KeeperException
     * @throws DeserializationException
      */
     @Test public void testZKClosingNodeVersionMismatch()
     throws IOException, NodeExistsException, KeeperException, DeserializationException {
       final Server server = new MockServer(HTU);
       final RegionServerServices rss = HTU.createMockRegionServerService();

       HTableDescriptor htd = TEST_HTD;
       final HRegionInfo hri = TEST_HRI;

       ZkCoordinatedStateManager coordinationProvider = new ZkCoordinatedStateManager();
       coordinationProvider.initialize(server);
       coordinationProvider.start();

       // open a region first so that it can be closed later
       OpenRegion(server, rss, htd, hri, coordinationProvider.getOpenRegionCoordination());

       // close the region
       // Create it CLOSING, which is what Master set before sending CLOSE RPC
       int versionOfClosingNode = ZKAssign.createNodeClosing(server.getZooKeeper(),
         hri, server.getServerName());

       // The CloseRegionHandler will validate the expected version
       // Given it is set to invalid versionOfClosingNode+1,
       // CloseRegionHandler should be M_ZK_REGION_CLOSING

       ZkCloseRegionCoordination.ZkCloseRegionDetails zkCrd =
         new ZkCloseRegionCoordination.ZkCloseRegionDetails();
       zkCrd.setPublishStatusInZk(true);
       zkCrd.setExpectedVersion(versionOfClosingNode+1);

       CloseRegionHandler handler = new CloseRegionHandler(server, rss, hri, false,
         coordinationProvider.getCloseRegionCoordination(), zkCrd);
       handler.process();

       // Handler should remain in M_ZK_REGION_CLOSING
       RegionTransition rt =
         RegionTransition.parseFrom(ZKAssign.getData(server.getZooKeeper(), hri.getEncodedName()));
       assertTrue(rt.getEventType().equals(EventType.M_ZK_REGION_CLOSING ));
     }

     /**
      * Test if the region can be closed properly
      * @throws IOException
      * @throws NodeExistsException
      * @throws KeeperException
     * @throws org.apache.hadoop.hbase.exceptions.DeserializationException
      */
     @Test public void testCloseRegion()
     throws IOException, NodeExistsException, KeeperException, DeserializationException {
       final Server server = new MockServer(HTU);
       final RegionServerServices rss = HTU.createMockRegionServerService();

       HTableDescriptor htd = TEST_HTD;
       HRegionInfo hri = TEST_HRI;

       ZkCoordinatedStateManager coordinationProvider = new ZkCoordinatedStateManager();
       coordinationProvider.initialize(server);
       coordinationProvider.start();

       // open a region first so that it can be closed later
       OpenRegion(server, rss, htd, hri, coordinationProvider.getOpenRegionCoordination());

       // close the region
       // Create it CLOSING, which is what Master set before sending CLOSE RPC
       int versionOfClosingNode = ZKAssign.createNodeClosing(server.getZooKeeper(),
         hri, server.getServerName());

       // The CloseRegionHandler will validate the expected version
       // Given it is set to correct versionOfClosingNode,
       // CloseRegionHandlerit should be RS_ZK_REGION_CLOSED

       ZkCloseRegionCoordination.ZkCloseRegionDetails zkCrd =
         new ZkCloseRegionCoordination.ZkCloseRegionDetails();
       zkCrd.setPublishStatusInZk(true);
       zkCrd.setExpectedVersion(versionOfClosingNode);

       CloseRegionHandler handler = new CloseRegionHandler(server, rss, hri, false,
         coordinationProvider.getCloseRegionCoordination(), zkCrd);
       handler.process();
       // Handler should have transitioned it to RS_ZK_REGION_CLOSED
       RegionTransition rt = RegionTransition.parseFrom(
         ZKAssign.getData(server.getZooKeeper(), hri.getEncodedName()));
       assertTrue(rt.getEventType().equals(EventType.RS_ZK_REGION_CLOSED));
     }

     private void OpenRegion(Server server, RegionServerServices rss,
         HTableDescriptor htd, HRegionInfo hri, OpenRegionCoordination coordination)
     throws IOException, NodeExistsException, KeeperException, DeserializationException {
       // Create it OFFLINE node, which is what Master set before sending OPEN RPC
       ZKAssign.createNodeOffline(server.getZooKeeper(), hri, server.getServerName());

       OpenRegionCoordination.OpenRegionDetails ord =
         coordination.getDetailsForNonCoordinatedOpening();
       OpenRegionHandler openHandler =
         new OpenRegionHandler(server, rss, hri, htd, -1, coordination, ord);
       rss.getRegionsInTransitionInRS().put(hri.getEncodedNameAsBytes(), Boolean.TRUE);
       openHandler.process();
       // This parse is not used?
       RegionTransition.parseFrom(ZKAssign.getData(server.getZooKeeper(), hri.getEncodedName()));
       // delete the node, which is what Master do after the region is opened
       ZKAssign.deleteNode(server.getZooKeeper(), hri.getEncodedName(),
         EventType.RS_ZK_REGION_OPENED, server.getServerName());
     }

}

