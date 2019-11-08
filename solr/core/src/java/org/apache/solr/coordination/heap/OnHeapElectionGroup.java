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

import org.apache.solr.coordination.ElectionGroup;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

public class OnHeapElectionGroup implements ElectionGroup {
  
  private final String id;
  
  private final List<String> participants;
  
  private final AtomicReference<String> leader;
  
  private final List<Runnable> listeners;
  
  public OnHeapElectionGroup(String id) {
    Objects.requireNonNull(id, "OnHeapElectionGroup id can't be null");
    this.id = id;
    participants = new ArrayList<>();
    leader = new AtomicReference<>(null);
    listeners = new CopyOnWriteArrayList<>();
  }
  
  @Override
  public String getLeader() {
    return leader.get();
  }
  

  @Override
  public void addParticipant(String participantId) {
    synchronized (leader) {
      participants.add(participantId);
      updateLeaderAndNotify();
    }
  }
  

  private void notifyListeners() {
    for (Runnable r:listeners) {
      r.run();
    }
  }

  @Override
  public void removeParticipant(String participantId) {
    synchronized (leader) {
      participants.remove(participantId);
      updateLeaderAndNotify();
    }
  }

  private void updateLeaderAndNotify() {
    synchronized (leader) {
      String oldLeader = leader.get();
      if (participants.isEmpty()) {
        leader.set(null);
      } else {
        leader.set(participants.get(0));
      }
      if (oldLeader == null || !oldLeader.equals(leader.get())) {
        notifyListeners();
      }
    }
  }
  

  @Override
  public void registerListener(Runnable listener) {
    listeners.add(listener);
  }
  

  @Override
  public void unRegisterListener(Runnable r) {
    listeners.remove(r);
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
    OnHeapElectionGroup other = (OnHeapElectionGroup) obj;
    if (!id.equals(other.id)) return false;
    return true;
  }

}
