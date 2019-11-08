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
package org.apache.solr.coordination.heap;

import org.apache.solr.SolrTestCase;
import org.apache.solr.coordination.CoordinationManager;
import org.apache.solr.coordination.ElectionGroup;
import org.apache.solr.coordination.heap.OnHeapCoordinationManager;

public class OnHeapCoordinationTest extends SolrTestCase {
  
  private CoordinationManager coordinationMgr;
  
  @Override
  public void setUp() throws Exception {
    coordinationMgr = new OnHeapCoordinationManager();
    super.setUp();
  }
  
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    coordinationMgr.close();
  }

  public void testCreateElectionGroup() {
    ElectionGroup eg = coordinationMgr.getElectionGroup("1");
    assertNotNull(eg);
  }
  
  public void testSameIdSameElectionGroup() {
    ElectionGroup eg = coordinationMgr.getElectionGroup("1");
    assertEquals(eg, coordinationMgr.getElectionGroup("1"));
  }
  
  public void testDifferentIdDifferentElectionGroup() {
    ElectionGroup eg = coordinationMgr.getElectionGroup("1");
    assertNotEquals(eg, coordinationMgr.getElectionGroup("2"));
  }
  
}
