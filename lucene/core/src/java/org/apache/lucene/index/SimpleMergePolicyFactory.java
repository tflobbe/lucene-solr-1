package org.apache.lucene.index;

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

/**
 * A {@link MergePolicyFactory} for simple {@link MergePolicy} objects.
 *
 * @lucene.experimental
 */
public abstract class SimpleMergePolicyFactory extends MergePolicyFactory {

  private final String mergePolicyClassName;

  protected SimpleMergePolicyFactory(MergePolicyFactoryHelper helper, MergePolicyFactoryArgs args, String mergePolicyClassName) {
    super(helper, args);
    this.mergePolicyClassName = mergePolicyClassName;
  }

  @Override
  public MergePolicy getMergePolicy() {
    final MergePolicy mp = helper.newInstance(
        mergePolicyClassName,
        MergePolicy.class,
        MergePolicyFactoryHelper.NO_SUB_PACKAGES,
        MergePolicyFactoryHelper.NO_CLASSES,
        MergePolicyFactoryHelper.NO_OBJECTS);
    args.invokeSetters(mp);
    return mp;
  }

}
