package org.apache.lucene.index;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.util.LucenePluginUtils;

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

public class MergePolicyFactoryArgs {

  private final Map<String, Object> args = new HashMap<>();

  public MergePolicyFactoryArgs() {
  }

  public MergePolicyFactoryArgs(Iterator<Map.Entry<String, Object>> iterator) {
    while (iterator.hasNext()) {
      final Map.Entry<String, Object> entry = iterator.next();
      args.put(entry.getKey(), entry.getValue());
    }
  }

  public void put(String key, Object val) {
    args.put(key, val);
  }

  public Object remove(String key) {
    return args.remove(key);
  }

  public Object get(String key) {
    return args.get(key);
  }

  public Set<String> keySet() {
    return args.keySet();
  }

  public void invokeSetters(MergePolicy policy) {
    for (String key : args.keySet()) {
      final Object val = args.get(key);
      LucenePluginUtils.invokeSetter(policy, key, val);
    }
  }

}
