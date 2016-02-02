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

public class TestLogMergePolicy extends BaseMergePolicyTestCase {

  public MergePolicy mergePolicy() {
    return newLogMergePolicy(random());
  }

  final private boolean changeMergeFactor = random().nextBoolean();
  final private boolean changeNoCFSRatio = random().nextBoolean();

  @Override
  MergePolicyFactoryArgs mergePolicyFactoryArgs() {
    final MergePolicyFactoryArgs args = new MergePolicyFactoryArgs();
    if (changeMergeFactor) args.put("mergeFactor", new Integer(42));
    if (changeNoCFSRatio) args.put("noCFSRatio", new Double(0.42d));
    return args;
  }

  protected void checkFactoryCreatedMergePolicy(MergePolicy mergePolicy) {
    assertTrue(mergePolicy instanceof LogMergePolicy);
    final LogMergePolicy mp = (LogMergePolicy)mergePolicy;
    if (changeMergeFactor) assertEquals(42, mp.getMergeFactor());
    if (changeNoCFSRatio) assertEquals(0.42d, mp.getNoCFSRatio(), 0.0);
  }

  public void testDefaultForcedMergeMB() {
    LogByteSizeMergePolicy mp = new LogByteSizeMergePolicy();
    assertTrue(mp.getMaxMergeMBForForcedMerge() > 0.0);
  }
}
