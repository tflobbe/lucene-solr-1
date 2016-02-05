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
package org.apache.solr.index;

import org.apache.lucene.index.MergePolicy;
import org.apache.solr.core.SolrResourceLoader;

/**
 * A {@link MergePolicyFactory} for simple {@link MergePolicy} objects. Implementations can override only
 * {@link #getMergePolicyInstance()} and this class will then configure it with all set properties.
 */
public class SimpleMergePolicyFactory extends MergePolicyFactory {

  static protected final String CLASS = "class";
  final protected String className;

  protected SimpleMergePolicyFactory(SolrResourceLoader resourceLoader, MergePolicyFactoryArgs args) {
    super(resourceLoader, args);
    className = (String) args.remove(CLASS);
  }

  /** Returns an instance of the {@link MergePolicy} without setting all its parameters. */
  protected MergePolicy getMergePolicyInstance() {
    if (className == null) {
      throw new IllegalArgumentException(getClass().getSimpleName()+" requires a '"+CLASS+"' argument.");
    } else {
      return resourceLoader.newInstance(className, MergePolicy.class);
    }
  }

  @Override
  public final MergePolicy getMergePolicy() {
    final MergePolicy mp = getMergePolicyInstance();
    args.invokeSetters(mp);
    return mp;
  }

}
