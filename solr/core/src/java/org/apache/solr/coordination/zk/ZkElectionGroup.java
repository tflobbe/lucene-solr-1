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

import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkCmdExecutor;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.coordination.ElectionGroup;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ZkElectionGroup implements ElectionGroup {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  private final static Pattern SESSION_ID = Pattern.compile(".*?/?(.*?-.*?)-n_\\d+");
  
  private final String id;
  private final SolrZkClient zkClient;
  private final String baseElectionPath;
  private final String currentLeaderPath;
  private final ZkCmdExecutor zkCmdExecutor;
  private final LeaderUpdateWatcher leaderUpdateWatch;
  private String leader;
  
  public ZkElectionGroup(String id, SolrZkClient client) {
    Objects.requireNonNull(id, "ZkElectionGroup id can't be null");
    Objects.requireNonNull(client, "ZkElectionGroup client can't be null");
    this.id = id;
    this.zkClient = client;
    zkCmdExecutor = new ZkCmdExecutor(zkClient.getZkClientTimeout());
    baseElectionPath = "/elections/" + id + "/participants/";
    currentLeaderPath = "/elections/" + id + "/leader";
    leaderUpdateWatch = new LeaderUpdateWatcher();
    setup();
    fetchLeader();
  }
  
  private synchronized void fetchLeader() {
    String leaderInZk = null;
    try {
      byte[] data = zkClient.getData(currentLeaderPath, leaderUpdateWatch, null, true);
      leaderInZk = new String(data, StandardCharsets.UTF_8);
    } catch (KeeperException.NoNodeException noNode) {
      // expected, there is no leader for this ElectionGroup yet
      try {
        Stat stat = zkClient.exists(currentLeaderPath, leaderUpdateWatch, true);
        if (stat == null) {
          // no leader yet
          leaderInZk = null;
        } else {
          // It didn't exist before, but now it does. Fetch again.
          fetchLeader();
        }
      } catch (KeeperException | InterruptedException e) {
        SolrZkClient.checkInterrupted(e);
        throw new RuntimeException(e);
      }
    } catch (KeeperException | InterruptedException e) {
      SolrZkClient.checkInterrupted(e);
      throw new RuntimeException(e);
    }
    leader = leaderInZk;
  }

  private synchronized void updateLeader() {
    String newLeader = null;
    try {
      byte[] data = zkClient.getData(currentLeaderPath, leaderUpdateWatch, null, true);
      newLeader = new String(data, StandardCharsets.UTF_8);
    } catch (KeeperException.NoNodeException noNode) {
      // expected, there is no leader for this ElectionGroup yet
      newLeader = null;
      
    } catch (KeeperException | InterruptedException e) {
      SolrZkClient.checkInterrupted(e);
      throw new RuntimeException(e);
    }
    updateLeaderAndNotify(newLeader);
  }

  private void setup() {
    try {
      zkCmdExecutor.ensureExists(baseElectionPath.substring(0, baseElectionPath.length() - 1), zkClient);
    } catch (KeeperException | InterruptedException e) {
      SolrZkClient.checkInterrupted(e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getLeader() {
    return leader;
  }

  @Override
  public void addParticipant(String participantId) {
    String compositeId = generateFullId(participantId);
    boolean retry = true;
    int tries = 0;
    while (retry) {
      try {
        String leaderSeqPath;
        leaderSeqPath = zkClient.create(baseElectionPath + compositeId + "-n_", participantId.getBytes(StandardCharsets.UTF_8),
            CreateMode.EPHEMERAL_SEQUENTIAL, false);
        log.debug("Joined leadership election with path: {}", leaderSeqPath);
        retry = false;
      } catch (ConnectionLossException e) {
        // we don't know if we made our node or not...
        List<String> entries;
        try {
          entries = zkClient.getChildren(baseElectionPath, null, true);
        } catch (KeeperException | InterruptedException e1) {
          SolrZkClient.checkInterrupted(e);
          throw new RuntimeException(e1);
        }
        
        boolean foundId = false;
        for (String entry : entries) {
          String nodeId = getNodeId(entry);
          if (compositeId.equals(nodeId)) {
            // we did create our node...
            foundId  = true;
            break;
          }
        }
        if (!foundId) {
          retry = true;
          if (tries++ > 20) {
            throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
                "", e);
          }
          try {
            Thread.sleep(50);
          } catch (InterruptedException e2) {
            Thread.currentThread().interrupt();
          }
        }

      } catch (KeeperException.NoNodeException e) {
        // we must have failed in creating the election node - someone else must
        // be working on it, lets try again
        if (tries++ > 20) {
          throw new RuntimeException(e);
        }
        retry = true;
        try {
          Thread.sleep(50);
        } catch (InterruptedException e2) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("Thread interrupted", e2);
        }
      } catch (KeeperException|InterruptedException e) {
        SolrZkClient.checkInterrupted(e);
        throw new RuntimeException(e);
      }
    }
  }

  private String generateFullId(String participantId) {
    return zkClient.getSolrZooKeeper().getSessionId() + "-" + participantId;
  }
  
  private String getNodeId(String nStringSequence) {
    String id;
    Matcher m = SESSION_ID.matcher(nStringSequence);
    if (m.matches()) {
      id = m.group(1);
    } else {
      throw new IllegalStateException("Could not find regex match in:"
          + nStringSequence);
    }
    return id;
  }

  @Override
  public void removeParticipant(String participantId) {}

  @Override
  public void registerListener(Runnable listener) {}

  @Override
  public void unRegisterListener(Runnable r) {}
  
  private synchronized void updateLeaderAndNotify(String newLeader) {
    String oldLeader = leader;
    leader = newLeader;
    if (oldLeader == null || !oldLeader.equals(leader)) {
      notifyListeners();
    }
  }
  
  
  private void notifyListeners() {}


  private class LeaderUpdateWatcher implements Watcher {
    
    @Override
    public void process(WatchedEvent event) {
      // session events are not change events, and do not remove the watcher
      if (EventType.None.equals(event.getType())) {
        return;
      }
      updateLeader();
    }
    
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + id.hashCode();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    ZkElectionGroup other = (ZkElectionGroup) obj;
    if (!id.equals(other.id)) return false;
    return true;
  }
  
  

}
