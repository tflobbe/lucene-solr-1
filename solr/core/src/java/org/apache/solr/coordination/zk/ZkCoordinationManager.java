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

import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.coordination.CoordinationManager;
import org.apache.solr.coordination.ElectionGroup;

import java.io.IOException;

public class ZkCoordinationManager implements CoordinationManager {
  
  private final SolrZkClient client;
  
  public ZkCoordinationManager(String zkHost) {
    client = new SolrZkClient(zkHost, 60000);
  }

  @Override
  public void close() throws IOException {
    client.close();
  }

  @Override
  public ElectionGroup getElectionGroup(String id) {
    return new ZkElectionGroup(id, client);
  }

}
