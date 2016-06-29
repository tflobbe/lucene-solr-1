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

import org.apache.solr.schema.IntPointField;
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
    for (int i=0; i < 10; i++) {
      assertU(adoc("id", String.valueOf(i), "number_p_i", String.valueOf(i+1)));
    }
    assertU(commit());
    for (int i = 0; i < 10; i++) {
      assertQ(req("q", "number_p_i:"+(i+1), "fl", "id, number_p_i"), 
          "//*[@numFound='1']");
    }
    
    for (int i = 0; i < 10; i++) {
      assertQ(req("q", "number_p_i:" + (i+1) + " OR number_p_i:" + ((i+1)%10 + 1)), "//*[@numFound='2']");
    }
  }
  
  @Test
  public void testIntPointFieldReturn() throws Exception {
    for (int i=0; i < 10; i++) {
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
    for (int i = 0; i < 10; i++) {
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
  
  @Test
  public void testIntPointFieldSort() throws Exception {
    for (int i = 0; i < 10; i++) {
      assertU(adoc("id", String.valueOf(i), "number_p_i_dv", String.valueOf(i), "number_p_i", String.valueOf(i)));
    }
    assertU(commit());
    assertTrue(h.getCore().getLatestSchema().getField("number_p_i_dv").hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField("number_p_i_dv").getType() instanceof IntPointField);
    assertQ(req("q", "*:*", "fl", "id, number_p_i_dv", "sort", "number_p_i_dv desc"), 
        "//*[@numFound='10']",
        "//result/doc[1]/int[@name='number_p_i_dv'][.='9']",
        "//result/doc[2]/int[@name='number_p_i_dv'][.='8']",
        "//result/doc[3]/int[@name='number_p_i_dv'][.='7']",
        "//result/doc[10]/int[@name='number_p_i_dv'][.='0']");
    
    assertFalse(h.getCore().getLatestSchema().getField("number_p_i").hasDocValues());
//    nocommit: Figure out what the best exception is
//    assertQEx("unexpected docvalues type NONE for field 'number_p_i' (expected=NUMERIC). Re-index with correct docvalues type.", 
//        req("q", "*:*", "fl", "id, number_p_i", "sort", "number_p_i desc"), 
//        SolrException.ErrorCode.BAD_REQUEST);
  }
  
  @Test
  public void testIntPointFieldFacetField() throws Exception {
    for (int i = 0; i < 10; i++) {
      assertU(adoc("id", String.valueOf(i), "number_p_i_dv", String.valueOf(i), "number_p_i", String.valueOf(i)));
    }
    assertU(commit());
    assertTrue(h.getCore().getLatestSchema().getField("number_p_i_dv").hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField("number_p_i_dv").getType() instanceof IntPointField);
    assertQ(req("q", "*:*", "fl", "id, number_p_i_dv", "facet", "true", "facet.field", "number_p_i_dv"), 
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='number_p_i_dv']/int[@name='1'][.='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='number_p_i_dv']/int[@name='2'][.='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='number_p_i_dv']/int[@name='3'][.='1']");
    
    assertU(adoc("id", "10", "number_p_i_dv", "1", "number_p_i", "1"));
    
    assertU(commit());
    assertQ(req("q", "*:*", "fl", "id, number_p_i_dv", "facet", "true", "facet.field", "number_p_i_dv"), 
        "//*[@numFound='11']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='number_p_i_dv']/int[@name='1'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='number_p_i_dv']/int[@name='2'][.='1']",
        "//lst[@name='facet_counts']/lst[@name='facet_fields']/lst[@name='number_p_i_dv']/int[@name='3'][.='1']");
    
    assertFalse(h.getCore().getLatestSchema().getField("number_p_i").hasDocValues());
//    nocommit: Figure out what the best exception is
//    assertQEx("unexpected docvalues type NONE for field 'number_p_i' (expected=NUMERIC). Re-index with correct docvalues type.", 
//        req("q", "*:*", "fl", "id, number_p_i", "sort", "number_p_i desc"), 
//        SolrException.ErrorCode.BAD_REQUEST);
  }
  
  @Test
  public void testIntPointFieldRanceFacet() throws Exception {
    for (int i = 0; i < 10; i++) {
      assertU(adoc("id", String.valueOf(i), "number_p_i_dv", String.valueOf(i), "number_p_i", String.valueOf(i)));
    }
    assertU(commit());
    assertTrue(h.getCore().getLatestSchema().getField("number_p_i_dv").hasDocValues());
    assertQ(req("q", "*:*", "fl", "id, number_p_i_dv", "facet", "true", "facet.range", "number_p_i_dv", "facet.range.start", "-10", "facet.range.end", "10", "facet.range.gap", "2"), 
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='number_p_i_dv']/lst[@name='counts']/int[@name='0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='number_p_i_dv']/lst[@name='counts']/int[@name='2'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='number_p_i_dv']/lst[@name='counts']/int[@name='4'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='number_p_i_dv']/lst[@name='counts']/int[@name='6'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='number_p_i_dv']/lst[@name='counts']/int[@name='8'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='number_p_i_dv']/lst[@name='counts']/int[@name='-10'][.='0']");
    
    assertQ(req("q", "*:*", "fl", "id, number_p_i_dv", "facet", "true", "facet.range", "number_p_i_dv", "facet.range.start", "-10", "facet.range.end", "10", "facet.range.gap", "2", "facet.range.method", "dv"), 
        "//*[@numFound='10']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='number_p_i_dv']/lst[@name='counts']/int[@name='0'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='number_p_i_dv']/lst[@name='counts']/int[@name='2'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='number_p_i_dv']/lst[@name='counts']/int[@name='4'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='number_p_i_dv']/lst[@name='counts']/int[@name='6'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='number_p_i_dv']/lst[@name='counts']/int[@name='8'][.='2']",
        "//lst[@name='facet_counts']/lst[@name='facet_ranges']/lst[@name='number_p_i_dv']/lst[@name='counts']/int[@name='-10'][.='0']");
    
    assertFalse(h.getCore().getLatestSchema().getField("number_p_i").hasDocValues());
//    nocommit: Figure out what the best exception is
//    assertQEx("unexpected docvalues type NONE for field 'number_p_i' (expected=NUMERIC). Re-index with correct docvalues type.", 
//        req("q", "*:*", "fl", "id, number_p_i", "sort", "number_p_i desc"), 
//        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void testIntPointFunctionQuery() throws Exception {
    for (int i = 0; i < 10; i++) {
      assertU(adoc("id", String.valueOf(i), "number_p_i_dv", String.valueOf(i), "number_p_i", String.valueOf(i)));
    }
    assertU(commit());
    assertTrue(h.getCore().getLatestSchema().getField("number_p_i_dv").hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField("number_p_i_dv").getType() instanceof IntPointField);
    assertQ(req("q", "*:*", "fl", "id, number_p_i_dv", "sort", "product(-1,number_p_i_dv) asc"), 
        "//*[@numFound='10']",
        "//result/doc[1]/int[@name='number_p_i_dv'][.='9']",
        "//result/doc[2]/int[@name='number_p_i_dv'][.='8']",
        "//result/doc[3]/int[@name='number_p_i_dv'][.='7']",
        "//result/doc[10]/int[@name='number_p_i_dv'][.='0']");
    
    assertQ(req("q", "*:*", "fl", "id, number_p_i_dv, product(-1,number_p_i_dv)"), 
        "//*[@numFound='10']",
        "//result/doc[1]/float[@name='product(-1,number_p_i_dv)'][.='-0.0']",
        "//result/doc[2]/float[@name='product(-1,number_p_i_dv)'][.='-1.0']",
        "//result/doc[3]/float[@name='product(-1,number_p_i_dv)'][.='-2.0']",
        "//result/doc[10]/float[@name='product(-1,number_p_i_dv)'][.='-9.0']");
    
    assertFalse(h.getCore().getLatestSchema().getField("number_p_i").hasDocValues());
//    nocommit: Figure out what the best exception is
//    assertQEx("unexpected docvalues type NONE for field 'number_p_i' (expected=NUMERIC). Re-index with correct docvalues type.", 
//        req("q", "*:*", "fl", "id, number_p_i", "sort", "number_p_i desc"), 
//        SolrException.ErrorCode.BAD_REQUEST);
  }

  @Test
  public void testIntPointStats() throws Exception {
    for (int i = 0; i < 10; i++) {
      assertU(adoc("id", String.valueOf(i), "number_p_i_dv", String.valueOf(i), "number_p_i", String.valueOf(i)));
    }
    assertU(adoc("id", "11"));
    assertU(commit());
    assertTrue(h.getCore().getLatestSchema().getField("number_p_i_dv").hasDocValues());
    assertTrue(h.getCore().getLatestSchema().getField("number_p_i_dv").getType() instanceof IntPointField);
    assertQ(req("q", "*:*", "fl", "id, number_p_i_dv", "stats", "true", "stats.field", "number_p_i_dv"), 
        "//*[@numFound='11']",
        "//lst[@name='stats']/lst[@name='stats_fields']/lst[@name='number_p_i_dv']/double[@name='min'][.='0.0']",
        "//lst[@name='stats']/lst[@name='stats_fields']/lst[@name='number_p_i_dv']/double[@name='max'][.='9.0']",
        "//lst[@name='stats']/lst[@name='stats_fields']/lst[@name='number_p_i_dv']/long[@name='count'][.='10']",
        "//lst[@name='stats']/lst[@name='stats_fields']/lst[@name='number_p_i_dv']/long[@name='missing'][.='1']");
    
    assertFalse(h.getCore().getLatestSchema().getField("number_p_i").hasDocValues());
//    nocommit: Figure out what the best exception is
//    assertQEx("unexpected docvalues type NONE for field 'number_p_i' (expected=NUMERIC). Re-index with correct docvalues type.", 
//        req("q", "*:*", "fl", "id, number_p_i", "sort", "number_p_i desc"), 
//        SolrException.ErrorCode.BAD_REQUEST);
  }
  
}
