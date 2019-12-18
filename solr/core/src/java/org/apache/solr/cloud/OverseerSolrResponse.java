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
package org.apache.solr.cloud;

import org.apache.solr.client.solrj.SolrResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.Utils;

import java.io.ByteArrayInputStream;
import java.util.Objects;

public class OverseerSolrResponse extends SolrResponse {
  
  @JsonProperty("response")
  NamedList<Object> responseList = null;

  @JsonProperty("elapsedTime")
  private long elapsedTime;
  
  public OverseerSolrResponse(NamedList list) {
    responseList = list;
  }
  
  @Override
  public long getElapsedTime() {
    return elapsedTime;
  }
  
  @Override
  public void setResponse(NamedList<Object> rsp) {
    this.responseList = rsp;
  }

  @Override
  public void setElapsedTime(long elapsedTime) {
    this.elapsedTime = elapsedTime;
  }

  @Override
  public NamedList<Object> getResponse() {
    return responseList;
  }

  /**
   * This method serializes the content of an {@code OverseerSolrResponse} into JSON. Note that:
   * <ul>
   * <li>The elapsed time is not serialized</li>
   * <li>Duplicated keys are not supported</li>
   * <li>{@code #deserialize(byte[])} turns every JSON Object into a NamedList</li>
   * </ul>
   */
  @SuppressWarnings("deprecation")
  public static byte[] serialize(OverseerSolrResponse responseObject) {
    Objects.requireNonNull(responseObject);
    if (Boolean.getBoolean("solr.unsafeOverseerResponseSerialization")) {
      return SolrResponse.serializable(responseObject);
    }
    return Utils.toJSON(responseObject.getResponse());
  }
  
  @SuppressWarnings("deprecation")
  public static OverseerSolrResponse deserialize(byte[] responseBytes) {
    Objects.requireNonNull(responseBytes);
    try {
      @SuppressWarnings("unchecked")
      NamedList<Object> response = (NamedList<Object>) Utils.fromJSON(new ByteArrayInputStream(responseBytes), Utils.NAMEDLISTOBJBUILDER);
      return new OverseerSolrResponse(response);
    } catch (RuntimeException e) {
      if (Boolean.getBoolean("solr.unsafeOverseerResponseDeserialization")) {
        return (OverseerSolrResponse) SolrResponse.deserialize(responseBytes);
      }
      throw new SolrException(ErrorCode.SERVER_ERROR, "Exception deserializing response from Javabin", e);
    }
  }
  
}
