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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FieldType.LegacyNumericType;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.valuesource.DoubleFieldSource;
import org.apache.lucene.queries.function.valuesource.FloatFieldSource;
import org.apache.lucene.queries.function.valuesource.IntFieldSource;
import org.apache.lucene.queries.function.valuesource.LongFieldSource;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.uninverting.UninvertingReader.Type;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;
import org.apache.lucene.util.LegacyNumericUtils;
import org.apache.lucene.util.NumericUtils;
import org.apache.solr.common.SolrException;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.util.DateFormatUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides field types to support for Lucene's {@link
 * org.apache.lucene.document.IntPoint}, {@link org.apache.lucene.document.LongPoint}, {@link org.apache.lucene.document.FloatPoint} and
 * {@link org.apache.lucene.document.DoublePoint}.
 * See {@link org.apache.lucene.search.PointRangeQuery} for more details.
 * It supports integer, float, long, double and date types.
 */
public abstract class PointField extends PrimitiveFieldType {

  protected PointTypes type;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    super.init(schema, args);
    String t = args.remove("type");

    if (t != null) {
      try {
        type = PointTypes.valueOf(t.toUpperCase(Locale.ROOT));
      } catch (IllegalArgumentException e) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
                "Invalid type specified in schema.xml for field: " + args.get("name"), e);
      }
    }
  }

//  @Override
//  public Object toObject(IndexableField f) {
//    final Number val = f.numericValue();
//    if (val != null) {
//      return (type == PointTypes.DATE) ? new Date(val.longValue()) : val;
//    } else {
//      throw new AssertionError("Field has no numeric value");
//    }
//  }

  @Override
  public SortField getSortField(SchemaField field, boolean top) {
    field.checkSortability();

    Object missingValue = null;
    boolean sortMissingLast  = field.sortMissingLast();
    boolean sortMissingFirst = field.sortMissingFirst();

    SortField sf;

    switch (type) {
      case INTEGER:
        if (sortMissingLast) {
          missingValue = top ? Integer.MIN_VALUE : Integer.MAX_VALUE;
        }
        else if( sortMissingFirst ) {
          missingValue = top ? Integer.MAX_VALUE : Integer.MIN_VALUE;
        }
        sf = new SortField( field.getName(), SortField.Type.INT, top);
        sf.setMissingValue(missingValue);
        return sf;
      
      case FLOAT:
        if (sortMissingLast) {
          missingValue = top ? Float.NEGATIVE_INFINITY : Float.POSITIVE_INFINITY;
        }
        else if( sortMissingFirst ) {
          missingValue = top ? Float.POSITIVE_INFINITY : Float.NEGATIVE_INFINITY;
        }
        sf = new SortField( field.getName(), SortField.Type.FLOAT, top);
        sf.setMissingValue(missingValue);
        return sf;
      
      case DATE: // fallthrough
      case LONG:
        if (sortMissingLast) {
          missingValue = top ? Long.MIN_VALUE : Long.MAX_VALUE;
        }
        else if( sortMissingFirst ) {
          missingValue = top ? Long.MAX_VALUE : Long.MIN_VALUE;
        }
        sf = new SortField( field.getName(), SortField.Type.LONG, top);
        sf.setMissingValue(missingValue);
        return sf;
        
      case DOUBLE:
        if (sortMissingLast) {
          missingValue = top ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
        }
        else if( sortMissingFirst ) {
          missingValue = top ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY;
        }
        sf = new SortField( field.getName(), SortField.Type.DOUBLE, top);
        sf.setMissingValue(missingValue);
        return sf;
        
      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field: " + field.name);
    }
  }
  
  @Override
  public Type getUninversionType(SchemaField sf) {
    if (sf.multiValued()) {
      switch (type) {
        case INTEGER:
          return Type.SORTED_SET_INTEGER;
        case LONG:
        case DATE:
          return Type.SORTED_SET_LONG;
        case FLOAT:
          return Type.SORTED_SET_FLOAT;
        case DOUBLE:
          return Type.SORTED_SET_DOUBLE;
        default:
          throw new AssertionError();
      }
    } else {
      switch (type) {
        case INTEGER:
          return Type.INTEGER;
        case LONG:
        case DATE:
          return Type.LONG;
        case FLOAT:
          return Type.FLOAT;
        case DOUBLE:
          return Type.DOUBLE;
        default:
          throw new AssertionError();
      }
    }
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser qparser) {
    field.checkFieldCacheSource();
    switch (type) {
      case INTEGER:
        return new IntFieldSource( field.getName());
      case FLOAT:
        return new FloatFieldSource( field.getName());
      case DATE:
        return new TrieDateFieldSource( field.getName());        
      case LONG:
        return new LongFieldSource( field.getName());
      case DOUBLE:
        return new DoubleFieldSource( field.getName());
      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field: " + field.name);
    }
  }

  @Override
  public final ValueSource getSingleValueSource(MultiValueSelector choice, SchemaField field, QParser parser) {
    // trivial base case
    if (!field.multiValued()) {
      // single value matches any selector
      return getValueSource(field, parser);
    }

    // See LUCENE-6709
    if (! field.hasDocValues()) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                              "docValues='true' is required to select '" + choice.toString() +
                              "' value from multivalued field ("+ field.getName() +") at query time");
    }
    
    // multivalued Trie fields all use SortedSetDocValues, so we give a clean error if that's
    // not supported by the specified choice, else we delegate to a helper
    SortedSetSelector.Type selectorType = choice.getSortedSetSelectorType();
    if (null == selectorType) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                              choice.toString() + " is not a supported option for picking a single value"
                              + " from the multivalued field: " + field.getName() +
                              " (type: " + this.getTypeName() + ")");
    }
    
    return getSingleValueSource(selectorType, field);
  }

  /**
   * Helper method that will only be called for multivalued Trie fields that have doc values.
   * Default impl throws an error indicating that selecting a single value from this multivalued 
   * field is not supported for this field type
   *
   * @param choice the selector Type to use, will never be null
   * @param field the field to use, garunteed to be multivalued.
   * @see #getSingleValueSource(MultiValueSelector,SchemaField,QParser) 
   */
  protected ValueSource getSingleValueSource(SortedSetSelector.Type choice, SchemaField field) {
    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                            "Can not select a single value for multivalued field: " + field.getName()
                            + " (single valued field selection not supported for type: " + this.getTypeName()
                            + ")");
  }

  @Override
  public boolean isTokenized() {
    return false;
  }

  @Override
  public boolean multiValuedFieldCache() {
    return false;
  }

  /**
   * @return the type of this field
   */
  public PointTypes getType() {
    return type;
  }

  @Override
  public FieldType.LegacyNumericType getNumericType() {
    switch (type) {
      case INTEGER:
        return FieldType.LegacyNumericType.INT;
      case LONG:
      case DATE:
        return FieldType.LegacyNumericType.LONG;
      case FLOAT:
        return FieldType.LegacyNumericType.FLOAT;
      case DOUBLE:
        return FieldType.LegacyNumericType.DOUBLE;
      default:
        throw new AssertionError();
    }
  }

//  @Override
//  public Query getRangeQuery(QParser parser, SchemaField field, String min, String max, boolean minInclusive, boolean maxInclusive) {
//    TODO: Handle the DV cases with indexed=false
//    if (field.multiValued() && field.hasDocValues() && !field.indexed()) {
//      // for the multi-valued dv-case, the default rangeimpl over toInternal is correct
//      return super.getRangeQuery(parser, field, min, max, minInclusive, maxInclusive);
//    }
//    Query query;
//    final boolean matchOnly = field.hasDocValues() && !field.indexed();
//    switch (type) {
//      case INTEGER:
//        if (matchOnly) {
//          query = DocValuesRangeQuery.newLongRange(field.getName(),
//                min == null ? null : (long) Integer.parseInt(min),
//                max == null ? null : (long) Integer.parseInt(max),
//                minInclusive, maxInclusive);
//        } else {
//          query = LegacyNumericRangeQuery.newIntRange(field.getName(), ps,
//              min == null ? null : Integer.parseInt(min),
//              max == null ? null : Integer.parseInt(max),
//              minInclusive, maxInclusive);
//        }
//        break;
//      case FLOAT:
//        if (matchOnly) {
//          query = DocValuesRangeQuery.newLongRange(field.getName(),
//                min == null ? null : (long) LegacyNumericUtils.floatToSortableInt(Float.parseFloat(min)),
//                max == null ? null : (long) LegacyNumericUtils.floatToSortableInt(Float.parseFloat(max)),
//                minInclusive, maxInclusive);
//        } else {
//          query = LegacyNumericRangeQuery.newFloatRange(field.getName(), ps,
//              min == null ? null : Float.parseFloat(min),
//              max == null ? null : Float.parseFloat(max),
//              minInclusive, maxInclusive);
//        }
//        break;
//      case LONG:
//        if (matchOnly) {
//          query = DocValuesRangeQuery.newLongRange(field.getName(),
//                min == null ? null : Long.parseLong(min),
//                max == null ? null : Long.parseLong(max),
//                minInclusive, maxInclusive);
//        } else {
//          query = LegacyNumericRangeQuery.newLongRange(field.getName(), ps,
//              min == null ? null : Long.parseLong(min),
//              max == null ? null : Long.parseLong(max),
//              minInclusive, maxInclusive);
//        }
//        break;
//      case DOUBLE:
//        if (matchOnly) {
//          query = DocValuesRangeQuery.newLongRange(field.getName(),
//                min == null ? null : LegacyNumericUtils.doubleToSortableLong(Double.parseDouble(min)),
//                max == null ? null : LegacyNumericUtils.doubleToSortableLong(Double.parseDouble(max)),
//                minInclusive, maxInclusive);
//        } else {
//          query = LegacyNumericRangeQuery.newDoubleRange(field.getName(), ps,
//              min == null ? null : Double.parseDouble(min),
//              max == null ? null : Double.parseDouble(max),
//              minInclusive, maxInclusive);
//        }
//        break;
//      case DATE:
//        if (matchOnly) {
//          query = DocValuesRangeQuery.newLongRange(field.getName(),
//                min == null ? null : DateFormatUtil.parseMath(null, min).getTime(),
//                max == null ? null : DateFormatUtil.parseMath(null, max).getTime(),
//                minInclusive, maxInclusive);
//        } else {
//          query = LegacyNumericRangeQuery.newLongRange(field.getName(), ps,
//              min == null ? null : DateFormatUtil.parseMath(null, min).getTime(),
//              max == null ? null : DateFormatUtil.parseMath(null, max).getTime(),
//              minInclusive, maxInclusive);
//        }
//        break;
//      default:
//        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field");
//    }
//
//    return query;
//  }
  
  @Override
  public Query getFieldQuery(QParser parser, SchemaField field, String externalVal) {
    if (!field.indexed() && field.hasDocValues()) {
      // currently implemented as singleton range
      return getRangeQuery(parser, field, externalVal, externalVal, true, true);
    } else {
      return getExactQuery(parser, field, externalVal);
    }
  }

  protected abstract Query getExactQuery(QParser parser, SchemaField field, String externalVal);

  @Deprecated
  static int toInt(byte[] arr, int offset) {
    return (arr[offset]<<24) | ((arr[offset+1]&0xff)<<16) | ((arr[offset+2]&0xff)<<8) | (arr[offset+3]&0xff);
  }
  
  @Deprecated
  static long toLong(byte[] arr, int offset) {
    int high = (arr[offset]<<24) | ((arr[offset+1]&0xff)<<16) | ((arr[offset+2]&0xff)<<8) | (arr[offset+3]&0xff);
    int low = (arr[offset+4]<<24) | ((arr[offset+5]&0xff)<<16) | ((arr[offset+6]&0xff)<<8) | (arr[offset+7]&0xff);
    return (((long)high)<<32) | (low&0x0ffffffffL);
  }

  @Override
  public String storedToReadable(IndexableField f) {
    return toExternal(f);
  }

  @Override
  public String toInternal(String val) {
    return readableToIndexed(val);
  }
  
  @Override
  public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
    writer.writeVal(name, f.numericValue());
  }

  static String badFieldString(IndexableField f) {
    String s = f.stringValue();
    return "ERROR:SCHEMA-INDEX-MISMATCH,stringValue="+s;
  }

//  @Override
//  public String toExternal(IndexableField f) {
//    return (type == PointTypes.DATE)
//      ? DateFormatUtil.formatExternal((Date) toObject(f))
//      : toObject(f).toString();
//  }

  @Override
  public Object toObject(SchemaField sf, BytesRef term) {
    switch (type) {
      case INTEGER:
        return LegacyNumericUtils.prefixCodedToInt(term);
      case FLOAT:
        return NumericUtils.sortableIntToFloat(LegacyNumericUtils.prefixCodedToInt(term));
      case LONG:
        return LegacyNumericUtils.prefixCodedToLong(term);
      case DOUBLE:
        return NumericUtils.sortableLongToDouble(LegacyNumericUtils.prefixCodedToLong(term));
      case DATE:
        return new Date(LegacyNumericUtils.prefixCodedToLong(term));
      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field: " + type);
    }
  }

  @Override
  public String storedToIndexed(IndexableField f) {
    final BytesRefBuilder bytes = new BytesRefBuilder();
    storedToIndexed(f, bytes);
    return bytes.get().utf8ToString();
  }

  private void storedToIndexed(IndexableField f, final BytesRefBuilder bytes) {
    final Number val = f.numericValue();
    if (val != null) {
      switch (type) {
        case INTEGER:
          LegacyNumericUtils.intToPrefixCoded(val.intValue(), 0, bytes);
          break;
        case FLOAT:
          LegacyNumericUtils.intToPrefixCoded(NumericUtils.floatToSortableInt(val.floatValue()), 0, bytes);
          break;
        case LONG: //fallthrough!
        case DATE:
          LegacyNumericUtils.longToPrefixCoded(val.longValue(), 0, bytes);
          break;
        case DOUBLE:
          LegacyNumericUtils.longToPrefixCoded(NumericUtils.doubleToSortableLong(val.doubleValue()), 0, bytes);
          break;
        default:
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field: " + f.name());
      }
    } else {
      // the old BinaryField encoding is no longer supported
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Invalid field contents: "+f.name());
    }
  }
  
  @Override
  public IndexableField createField(SchemaField field, Object value, float boost) {
    boolean indexed = field.indexed();
    boolean stored = field.stored();
    boolean docValues = field.hasDocValues();

    if (!indexed && !stored && !docValues) {
      if (log.isTraceEnabled())
        log.trace("Ignoring unindexed/unstored field: " + field);
      return null;
    }
    
    /*FieldType ft = new FieldType();
    ft.setStored(stored);
    ft.setTokenized(true);
    ft.setOmitNorms(field.omitNorms());
    ft.setIndexOptions(indexed ? getIndexOptions(field, value.toString()) : IndexOptions.NONE);*/

    /*switch (type) {
      case INTEGER:
        ft.setNumericType(FieldType.LegacyNumericType.INT);
        break;
      case FLOAT:
        ft.setNumericType(FieldType.LegacyNumericType.FLOAT);
        break;
      case LONG:
        ft.setNumericType(FieldType.LegacyNumericType.LONG);
        break;
      case DOUBLE:
        ft.setNumericType(FieldType.LegacyNumericType.DOUBLE);
        break;
      case DATE:
        ft.setNumericType(FieldType.LegacyNumericType.LONG);
        break;
      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for trie field: " + type);
    }*/

    final org.apache.lucene.document.Field f;

    switch (type) {
      case INTEGER:
        int i = (value instanceof Number)
          ? ((Number)value).intValue()
          : Integer.parseInt(value.toString());
        f = new IntPoint(field.getName(), i); //new LegacyIntField(field.getName(), i, ft);
        break;
      case FLOAT:
        float fl = (value instanceof Number)
          ? ((Number)value).floatValue()
          : Float.parseFloat(value.toString());
        f = new FloatPoint(field.getName(), fl); //new LegacyFloatField(field.getName(), fl, ft);
        break;
      case LONG:
        long l = (value instanceof Number)
          ? ((Number)value).longValue()
          : Long.parseLong(value.toString());
        f = new LongPoint(field.getName(), l); //new LegacyLongField(field.getName(), l, ft);
        break;
      case DOUBLE:
        double d = (value instanceof Number)
          ? ((Number)value).doubleValue()
          : Double.parseDouble(value.toString());
        f = new DoublePoint(field.getName(), d); // LegacyDoubleField(field.getName(), d, ft);
        break;
      case DATE:
        Date date = (value instanceof Date)
          ? ((Date)value)
          : DateFormatUtil.parseMath(null, value.toString());
        f = new LongPoint(field.getName(), date.getTime()); //new LegacyLongField(field.getName(), date.getTime(), ft);
        break;
      default:
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Unknown type for point field: " + type);
    }

    f.setBoost(boost);
    return f;
  }

  @Override
  public List<IndexableField> createFields(SchemaField sf, Object value, float boost) {
    if (!(sf.hasDocValues() || sf.stored())) {
      return Collections.singletonList(createField(sf, value, boost));
    }
    List<IndexableField> fields = new ArrayList<>();
    final IndexableField field = createField(sf, value, boost);
    fields.add(field);
    
    if (sf.hasDocValues()) {
      if (sf.multiValued()) {
        BytesRefBuilder bytes = new BytesRefBuilder();
        storedToIndexed(field, bytes);
        fields.add(new SortedSetDocValuesField(sf.getName(), bytes.get()));
      } else {
        final long bits;
        if (field.numericValue() instanceof Integer || field.numericValue() instanceof Long) {
          bits = field.numericValue().longValue();
        } else if (field.numericValue() instanceof Float) {
          bits = Float.floatToIntBits(field.numericValue().floatValue());
        } else {
          assert field.numericValue() instanceof Double;
          bits = Double.doubleToLongBits(field.numericValue().doubleValue());
        }
        fields.add(new NumericDocValuesField(sf.getName(), bits));
      }
    }
    if (sf.stored()) {
      //TODO: Can value be something other than a String?
      fields.add(new StoredField(sf.getName(), Integer.parseInt((String)value)));
    }
    return fields;
  }

  public enum PointTypes {
    INTEGER,
    LONG,
    FLOAT,
    DOUBLE,
    DATE
  }

  @Override
  public void checkSchemaField(final SchemaField field) {
  }
}
