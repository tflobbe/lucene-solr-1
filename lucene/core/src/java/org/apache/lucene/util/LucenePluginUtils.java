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

package org.apache.lucene.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.Map;

public class LucenePluginUtils {

  public static void invokeSetters(Object bean, Iterator<Map.Entry<String, Object>> iterator) {
    while (iterator.hasNext()) {
      Map.Entry<String, Object> entry = iterator.next();
      String key = entry.getKey();
      Object val = entry.getValue();
      invokeSetter(bean, key, val);
    }
  }

  public static void invokeSetter(Object bean, String key, Object val) {
    String setterName = "set" + String.valueOf(Character.toUpperCase(key.charAt(0))) + key.substring(1);
    Method method = null;
    try {
      for (Method m : bean.getClass().getMethods()) {
        if (m.getName().equals(setterName) && m.getParameterTypes().length == 1) {
          method = m;
          break;
        }
      }
      if (method == null) {
        throw new RuntimeException("no setter corrresponding to '" + key + "' in " + bean.getClass().getName());
      }
      Class<?> pClazz = method.getParameterTypes()[0];
      method.invoke(bean, val);
    } catch (InvocationTargetException | IllegalAccessException e1) {
      throw new RuntimeException("Error invoking setter " + setterName + " on class : " + bean.getClass().getName(), e1);
    }
  }

}
