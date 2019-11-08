/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.coordination.zk;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.cloud.ZkTestServer;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.coordination.ElectionGroup;
import org.apache.solr.coordination.heap.OnHeapElectionGroup;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressSSL
public class ZkElectionGroupTest extends SolrTestCaseJ4 {
  
  protected volatile static ZkTestServer zkServer;

  protected volatile static Path zkDir;
  
  private static SolrZkClient zkClient;
  
  @BeforeClass
  public static void setUpClass() throws Exception {
    zkDir = createTempDir("zkData");
    zkServer = new ZkTestServer(zkDir);
    zkServer.run();
    zkClient = new SolrZkClient("localhost:" + zkServer.getPort(), 30000);
  }
  
  @AfterClass
  public static void tearDownClass() throws IOException, InterruptedException {
    zkClient.close();
    zkServer.shutdown();
    zkServer = null;
    zkDir = null;
  }
  
  public void testNoParticipantsNoLeader() {
    ElectionGroup eg = new ZkElectionGroup("1", zkClient);
    assertNull(eg.getLeader());
  }
  
  public void testSingleParticipantsIsLeader() {
    ElectionGroup eg = new ZkElectionGroup("1", zkClient);
    eg.addParticipant("1");
    String leader = eg.getLeader();
    assertNotNull(leader);
    assertEquals("1", leader);
  }
  
  public void testSecondParticipantDoesntChangeLeader() {
    ElectionGroup eg = new OnHeapElectionGroup("1");
    eg.addParticipant("1");
    assertEquals("1", eg.getLeader());
    eg.addParticipant("2");
    assertEquals("1", eg.getLeader());
  }
  
  public void testRemoveLeaderParticipant() {
    ElectionGroup eg = new OnHeapElectionGroup("1");
    eg.addParticipant("1");
    eg.addParticipant("2");
    assertEquals("1", eg.getLeader());
    eg.removeParticipant("1");
    assertEquals("2", eg.getLeader());
  }
  
  public void testRemoveNonLeaderParticipant() {
    ElectionGroup eg = new OnHeapElectionGroup("1");
    eg.addParticipant("1");
    eg.addParticipant("2");
    assertEquals("1", eg.getLeader());
    eg.removeParticipant("2");
    assertEquals("1", eg.getLeader());
  }
  
  public void testListenerCalledOnLeaderChanged() {
    ElectionGroup eg = new OnHeapElectionGroup("1");
    AtomicInteger listenerCalls = new AtomicInteger();
    eg.registerListener(() -> listenerCalls.incrementAndGet());
    assertEquals(0, listenerCalls.get());
    eg.addParticipant("1");
    assertEquals(1, listenerCalls.get());
    eg.addParticipant("2");
    assertEquals(1, listenerCalls.get());
    eg.removeParticipant("1");
    assertEquals(2, listenerCalls.get());
  }
  
  public void testRemoveListener() {
    ElectionGroup eg = new OnHeapElectionGroup("1");
    AtomicInteger listenerCalls = new AtomicInteger();
    Runnable r = new Runnable() {
      @Override
      public void run() {listenerCalls.incrementAndGet();}
    };
    eg.registerListener(r);
    assertEquals(0, listenerCalls.get());
    eg.addParticipant("1");
    assertEquals(1, listenerCalls.get());
    eg.unRegisterListener(r);
    eg.removeParticipant("1");
    assertEquals(1, listenerCalls.get());
  }
  
  public void testEquals() {
    ElectionGroup eg = new OnHeapElectionGroup("1");
    assertEquals(eg, eg);
    assertEquals(eg, new OnHeapElectionGroup("1"));
    assertNotEquals(eg, new OnHeapElectionGroup("2"));
    assertNotEquals(eg, null);
  }
  
  public void testHashCode() {
    assertEquals(new OnHeapElectionGroup("1").hashCode(), new OnHeapElectionGroup("1").hashCode());
  }
  
  public void testNullId() {
    expectThrows(NullPointerException.class, () -> new OnHeapElectionGroup(null));
  }

}
