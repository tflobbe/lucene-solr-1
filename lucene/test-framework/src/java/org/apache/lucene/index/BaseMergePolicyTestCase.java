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

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base test case for {@link MergePolicy}.
 */
public abstract class BaseMergePolicyTestCase extends LuceneTestCase {
  
  /** Create a new {@link MergePolicy} instance. */
  protected abstract MergePolicy mergePolicy();

  public void testForceMergeNotNeeded() throws IOException {
    Directory dir = newDirectory();
    final AtomicBoolean mayMerge = new AtomicBoolean(true);
    final MergeScheduler mergeScheduler = new SerialMergeScheduler() {
      @Override
      synchronized public void merge(IndexWriter writer, MergeTrigger trigger, boolean newMergesFound) throws IOException {
        if (!mayMerge.get() && writer.getNextMerge() != null) {
          throw new AssertionError();
        }
        super.merge(writer, trigger, newMergesFound);
      }
    };
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())).setMergeScheduler(mergeScheduler).setMergePolicy(mergePolicy()));
    writer.getConfig().getMergePolicy().setNoCFSRatio(random().nextBoolean() ? 0 : 1);
    final int numSegments = TestUtil.nextInt(random(), 2, 20);
    for (int i = 0; i < numSegments; ++i) {
      final int numDocs = TestUtil.nextInt(random(), 1, 5);
      for (int j = 0; j < numDocs; ++j) {
        writer.addDocument(new Document());
      }
      writer.getReader().close();
    }
    for (int i = 5; i >= 0; --i) {
      final int segmentCount = writer.getSegmentCount();
      final int maxNumSegments = i == 0 ? 1 : TestUtil.nextInt(random(), 1, 10);
      mayMerge.set(segmentCount > maxNumSegments);
      writer.forceMerge(maxNumSegments);
    }
    writer.close();
    dir.close();
  }
  
  Class<?>[] mergePolicyFactoryConstructorParameterTypes() {
    return new Class[] {
        MergePolicyFactoryHelper.class,
        MergePolicyFactoryArgs.class
    };
  }

  Object[] mergePolicyFactoryConstructorParameters() {
    return new Object[] {
        mergePolicyFactoryHelper(),
        mergePolicyFactoryArgs()
    };
  }

  protected abstract void checkFactoryCreatedMergePolicy(MergePolicy mergePolicy);

  MergePolicyFactoryHelper mergePolicyFactoryHelper() {
    return new MergePolicyFactoryHelper() {
      public <T> T newInstance(String cName, Class<T> expectedType, Class[] params, Object[] args) {
        Class<? extends T> clazz;
        try {
          clazz = Class.forName(cName).asSubclass(expectedType);
        } catch (ClassNotFoundException e) {
          return null;
        }
        Constructor<? extends T> constructor = null;
        try {
          constructor = clazz.getConstructor(params);
        } catch (NoSuchMethodException|SecurityException e) {
          return null;
        }
        T obj = null;
        try {
          obj = constructor.newInstance(args);
        } catch (IllegalAccessException|IllegalArgumentException|InstantiationException|InvocationTargetException e) {
          return null;
        }
        return obj;
      }
    };
  }

  MergePolicyFactoryArgs mergePolicyFactoryArgs() {
    return new MergePolicyFactoryArgs();
  }

  public void testMergePolicyFactory() throws IOException {
    final MergePolicy mp = mergePolicy();
    Class<?> mpClass = mp.getClass();
    do {
      implTestMergePolicyFactory(mpClass);
      mpClass = mpClass.getSuperclass();
    } while (MergePolicy.class.isAssignableFrom(mpClass));
    implTestMergePolicyFactory(MergePolicy.class);
  }

  private void implTestMergePolicyFactory(Class<?> mpClass) throws IOException {
    if (Modifier.isAbstract(mpClass.getModifiers())) return;
    final String mpClassName = mpClass.getName();
    final String mpfClassName = mpClassName+"Factory";
    // find factory class
    Class<?> mpfClass = null;
    try {
      mpfClass = Class.forName(mpfClassName);
    } catch (ClassNotFoundException e) {
      fail(mpfClassName+" factory for "+mpClassName+" not found: "+mpClass);
    }
    final int mpfClassModifiers = mpfClass.getModifiers();
    assertTrue(mpfClassName+" factory for "+mpClassName+" is not public: "+mpfClass,
        Modifier.isPublic(mpfClassModifiers));
    // find factory class's constructor
    Constructor<?> constructor = null;
    final Class<?>[] mpfConstructorParameterTypes = mergePolicyFactoryConstructorParameterTypes();
    try {
      constructor = mpfClass.getConstructor(mpfConstructorParameterTypes);
    } catch (NoSuchMethodException|SecurityException e) {
      fail(mpfClassName+" constructor("+mpfConstructorParameterTypes+") not found - "+e);
    }
    // construct a factory instance
    MergePolicyFactory mpf = null;
    final Object[] mpfConstructorParameters = mergePolicyFactoryConstructorParameters();
    try {
      final Object obj = constructor.newInstance(mpfConstructorParameters);
      assertTrue(obj+" is not a MergePolicyFactory", obj instanceof MergePolicyFactory);
      mpf = (MergePolicyFactory)obj;
    } catch (IllegalAccessException|IllegalArgumentException|InstantiationException|InvocationTargetException e) {
      fail(mpfClassName+" constructor.newInstance("+mpfConstructorParameterTypes+") failed - "+e);
    }
    // create a merge policy using the factory instance
    final MergePolicy mp = mpf.getMergePolicy();
    assertTrue(mp+" cannot be assigned to "+mpClass, mpClass.isAssignableFrom(mp.getClass()));
    checkFactoryCreatedMergePolicy(mp);
  }

}
