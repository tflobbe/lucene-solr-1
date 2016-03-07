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
package org.apache.solr;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for PointField functionality
 *
 *
 */
public class TestPointFields extends SolrTestCaseJ4 {
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    initCore("solrconfig.xml","schema-point.xml");
  }
  
  @Override
  @After
  public void tearDown() throws Exception {
    clearIndex();
    assertU(commit());
    super.tearDown();
  }
  
  @Test
  public void testIntPointFieldExactQuery() throws Exception {
    for (int i=0; i<10; i++) {
      assertU(adoc("id", String.valueOf(i), "number_p_i", String.valueOf(i+1)));
    }
    assertU(commit());
    for (int i=0; i<10; i++) {
      assertQ(req("q", "number_p_i:"+(i+1), "fl", "id, number_p_i"), 
          "//*[@numFound='1']");
    }
    
    for (int i=0; i<10; i++) {
      assertQ(req("q", "number_p_i:" + (i+1) + " OR number_p_i:" + ((i+1)%10 + 1)), "//*[@numFound='2']");
    }
  }
  
  @Test
  public void testIntPointFieldReturn() throws Exception {
    for (int i=0; i<10; i++) {
      assertU(adoc("id", String.valueOf(i), "number_p_i", String.valueOf(i)));
    }
    assertU(commit());
    String[] expected = new String[11];
    expected[0] = "//*[@numFound='10']"; 
    for (int i = 1; i <= 10; i++) {
      expected[i] = "//result/doc[" + i + "]/int[@name='number_p_i'][.='" + (i-1) + "']";
    }
    assertQ(req("q", "*:*", "fl", "id, number_p_i"), expected);
  }
  
  @Test
  public void testIntPointFieldRangeQuery() throws Exception {
    for (int i=0; i<10; i++) {
      assertU(adoc("id", String.valueOf(i), "number_p_i", String.valueOf(i)));
    }
    assertU(commit());
    assertQ(req("q", "number_p_i:[0 TO 3]", "fl", "id, number_p_i"), 
        "//*[@numFound='4']",
        "//result/doc[1]/int[@name='number_p_i'][.='0']",
        "//result/doc[2]/int[@name='number_p_i'][.='1']",
        "//result/doc[3]/int[@name='number_p_i'][.='2']",
        "//result/doc[4]/int[@name='number_p_i'][.='3']");
    
    assertQ(req("q", "number_p_i:{0 TO 3]", "fl", "id, number_p_i"), 
        "//*[@numFound='3']",
        "//result/doc[1]/int[@name='number_p_i'][.='1']",
        "//result/doc[2]/int[@name='number_p_i'][.='2']",
        "//result/doc[3]/int[@name='number_p_i'][.='3']");
    
    assertQ(req("q", "number_p_i:[0 TO 3}", "fl", "id, number_p_i"), 
        "//*[@numFound='3']",
        "//result/doc[1]/int[@name='number_p_i'][.='0']",
        "//result/doc[2]/int[@name='number_p_i'][.='1']",
        "//result/doc[3]/int[@name='number_p_i'][.='2']");
    
    assertQ(req("q", "number_p_i:{0 TO 3}", "fl", "id, number_p_i"), 
        "//*[@numFound='2']",
        "//result/doc[1]/int[@name='number_p_i'][.='1']",
        "//result/doc[2]/int[@name='number_p_i'][.='2']");
    
    assertQ(req("q", "number_p_i:{0 TO *}", "fl", "id, number_p_i"), 
        "//*[@numFound='9']",
        "//result/doc[1]/int[@name='number_p_i'][.='1']");
    
    assertQ(req("q", "number_p_i:{* TO 3}", "fl", "id, number_p_i"), 
        "//*[@numFound='3']",
        "//result/doc[1]/int[@name='number_p_i'][.='0']");
    
    assertQ(req("q", "number_p_i:[* TO 3}", "fl", "id, number_p_i"), 
        "//*[@numFound='3']",
        "//result/doc[1]/int[@name='number_p_i'][.='0']");
    
    assertQ(req("q", "number_p_i:[* TO *}", "fl", "id, number_p_i"), 
        "//*[@numFound='10']",
        "//result/doc[1]/int[@name='number_p_i'][.='0']",
        "//result/doc[10]/int[@name='number_p_i'][.='9']");
  }

}
