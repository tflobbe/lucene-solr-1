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

package org.apache.solr.schema;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Map;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.IntDocValues;
import org.apache.lucene.queries.function.valuesource.IntFieldSource;
import org.apache.lucene.queries.function.valuesource.SortedSetFieldSource;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.mutable.MutableValue;
import org.apache.lucene.util.mutable.MutableValueInt;
import org.apache.solr.search.QParser;
import org.apache.solr.uninverting.UninvertingReader.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A numeric field that can contain 32-bit signed two's complement integer values.
 *
 * <ul>
 *  <li>Min Value Allowed: -2147483648</li>
 *  <li>Max Value Allowed: 2147483647</li>
 * </ul>
 * 
 * @see Integer
 */
public class IntPointField extends PointField implements IntValueFieldType {
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  public IntPointField() {
    super(Integer.BYTES);
  }

  {
    type=PointTypes.INTEGER;
  }

  @Override
  public Object toNativeType(Object val) {
    if(val==null) return null;
    if (val instanceof Number) return ((Number) val).intValue();
    try {
      if (val instanceof String) return Integer.parseInt((String) val);
    } catch (NumberFormatException e) {
      Float v = Float.parseFloat((String) val);
      return v.intValue();
    }
    return super.toNativeType(val);
  }
  
  public Query getRangeQuery(QParser parser, SchemaField field, String min, String max, boolean minInclusive, boolean maxInclusive) {
    int actualMin,actualMax;
    if (min == null) {
      actualMin = Integer.MIN_VALUE;
    } else {
      actualMin = Integer.parseInt(min);
      if (!minInclusive) {
        actualMin++;
      }
    }
    if (max == null) {
      actualMax = Integer.MAX_VALUE;
    } else {
      actualMax = Integer.parseInt(max);
      if (!maxInclusive) {
        actualMax--;
      }
    }
    return IntPoint.newRangeQuery(field.getName(), actualMin, actualMax);
  }
  
  @Override
  protected ValueSource getSingleValueSource(SortedSetSelector.Type choice, SchemaField f) {
    return new SortedSetFieldSource(f.getName(), choice) {
      @Override
      public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
        SortedSetFieldSource thisAsSortedSetFieldSource = this; // needed for nested anon class ref
        
        SortedSetDocValues sortedSet = DocValues.getSortedSet(readerContext.reader(), field);
        SortedDocValues view = SortedSetSelector.wrap(sortedSet, selector);
        
        return new IntDocValues(thisAsSortedSetFieldSource) {
          @Override
          public int intVal(int doc) {
            BytesRef bytes = view.get(doc);
            if (0 == bytes.length) {
              // the only way this should be possible is for non existent value
              assert !exists(doc) : "zero bytes for doc, but exists is true";
              return 0;
            }
            return IntPoint.decodeDimension(bytes.bytes, bytes.offset);
          }

          @Override
          public boolean exists(int doc) {
            return -1 != view.getOrd(doc);
          }

          @Override
          public ValueFiller getValueFiller() {
            return new ValueFiller() {
              private final MutableValueInt mval = new MutableValueInt();
              
              @Override
              public MutableValue getValue() {
                return mval;
              }
              
              @Override
              public void fillValue(int doc) {
                // micro optimized (eliminate at least one redudnent ord check) 
                //mval.exists = exists(doc);
                //mval.value = mval.exists ? intVal(doc) : 0;
                //
                BytesRef bytes = view.get(doc);
                mval.exists = (0 == bytes.length);
                mval.value = mval.exists ? IntPoint.decodeDimension(bytes.bytes, bytes.offset) : 0;
              }
            };
          }
        };
      }
    };
  }
  
  @Override
  public Object toObject(SchemaField sf, BytesRef term) {
    return IntPoint.decodeDimension(term.bytes, term.offset);
  }

  @Override
  protected Query getExactQuery(QParser parser, SchemaField field, String externalVal) {
    //TODO: better handling of string->int conversion
    return IntPoint.newExactQuery(field.getName(), Integer.parseInt(externalVal));
  }
  
  @Override
  public CharsRef indexedToReadable(BytesRef indexedForm, CharsRefBuilder charsRef) {
    final String value = Integer.toString(IntPoint.decodeDimension(indexedForm.bytes, indexedForm.offset));
    charsRef.grow(value.length());
    charsRef.setLength(value.length());
    value.getChars(0, charsRef.length(), charsRef.chars(), 0);
    return charsRef.get();
  }
  
  public String indexedToReadable(String _indexedForm) {
    final BytesRef indexedForm = new BytesRef(_indexedForm);
    return Integer.toString(IntPoint.decodeDimension(indexedForm.bytes, 0));
  }
  
  public void readableToIndexed(CharSequence val, BytesRefBuilder result) {
    result.grow(Integer.BYTES);
    result.setLength(Integer.BYTES);
    IntPoint.encodeDimension(Integer.parseInt(val.toString()), result.bytes(), 0);// TODO:0?
  }
  

  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    field.checkSortability();

    Object missingValue = null;
    boolean sortMissingLast  = field.sortMissingLast();
    boolean sortMissingFirst = field.sortMissingFirst();

    if (sortMissingLast) {
      missingValue = top ? Integer.MIN_VALUE : Integer.MAX_VALUE;
    }
    else if( sortMissingFirst ) {
      missingValue = top ? Integer.MAX_VALUE : Integer.MIN_VALUE;
    }
    SortField sf = new SortField( field.getName(), SortField.Type.INT, top);
    sf.setMissingValue(missingValue);
    return sf;
  }
  
  @Override
  public Type getUninversionType(SchemaField sf) {
    if (sf.multiValued()) {
      return Type.SORTED_SET_INTEGER;
    } else {
      return Type.INTEGER_POINT;
    }
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser qparser) {
    field.checkFieldCacheSource();
    return new IntFieldSource(field.getName());
  }
  
  @Override
  public FieldType.LegacyNumericType getNumericType() {
    return FieldType.LegacyNumericType.INT;
  }
  
  @Override
  public IndexableField createField(SchemaField field, Object value, float boost) {
    if (!isFieldUsed(field)) return null;
    
    if (boost != 1.0 && log.isTraceEnabled()) {
      log.trace("Can't use document/field boost for PointField. Field: " + field.getName() + ", boost: " + boost);
    }
    int intValue = (value instanceof Number) ? ((Number)value).intValue(): Integer.parseInt(value.toString());
    return new IntPoint(field.getName(), intValue);
  }
}
