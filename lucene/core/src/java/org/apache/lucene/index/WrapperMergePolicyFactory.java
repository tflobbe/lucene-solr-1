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
 * A {@link MergePolicyFactory} for wrapping {@link MergePolicy} objects.
 *
 * @lucene.experimental
 */
public abstract class WrapperMergePolicyFactory extends MergePolicyFactory {

  private class WrappedMergePolicyFactoryArgs extends MergePolicyFactoryArgs {

    final String mergePolicyFactoryClassName;

    WrappedMergePolicyFactoryArgs(MergePolicyFactoryArgs wrapperArgs, String baseArgName) {
      super();
      String wrappedMergePolicyFactoryClassName = null;
      final String baseArgsPrefix = baseArgName+'.';
      for (String key : wrapperArgs.keySet()) {
        final Object val = wrapperArgs.get(key);
        if (baseArgName.equals(key)) {
          wrappedMergePolicyFactoryClassName = (String)val;
          wrapperArgs.remove(key);
        } if (key.startsWith(baseArgsPrefix)) {
          this.put(key.substring(baseArgsPrefix.length()), val);
          wrapperArgs.remove(key);
        }
      }
      mergePolicyFactoryClassName = wrappedMergePolicyFactoryClassName;
    }
  }

  private final String wrapperMergePolicyClassName;
  private final WrappedMergePolicyFactoryArgs wrappedMergePolicyArgs;

  protected WrapperMergePolicyFactory(MergePolicyFactoryHelper helper,
      MergePolicyFactoryArgs wrapperArgs, String wrapperMergePolicyClassName,
      String baseArgName) {
    super(helper, wrapperArgs);
    this.wrapperMergePolicyClassName = wrapperMergePolicyClassName;
    wrappedMergePolicyArgs = new WrappedMergePolicyFactoryArgs(this.args, baseArgName);
  }

  @Override
  public MergePolicy getMergePolicy() {
    final MergePolicy wrappedMP = getWrappedMergePolicy();
    final MergePolicy wrapperMP = (helper == null ? null : helper.newInstance(
        wrapperMergePolicyClassName,
        MergePolicy.class,
        new Class[] { MergePolicy.class },
        new Object[] { wrappedMP }));
    args.invokeSetters(wrapperMP);
    return wrapperMP;
  }

  protected MergePolicy getWrappedMergePolicy() {
    final MergePolicyFactory mpf = helper.newInstance(
        wrappedMergePolicyArgs.mergePolicyFactoryClassName,
        MergePolicyFactory.class,
        new Class[] { MergePolicyFactoryArgs.class },
        new Object[] { wrappedMergePolicyArgs });
    return mpf.getMergePolicy();
  }

}
