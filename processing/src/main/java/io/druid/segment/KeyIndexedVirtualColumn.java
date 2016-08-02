/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.metamx.common.StringUtils;
import io.druid.query.QueryCacheHelper;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilterCacheHelper;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.data.IndexedInts;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

/**
 */
public class KeyIndexedVirtualColumn implements VirtualColumn
{
  private static final byte VC_TYPE_ID = 0x02;

  private final String outputName;
  private final String keyDimension;
  private final List<String> valueDimensions;

  private final IndexHolder indexProvider;

  @JsonCreator
  public KeyIndexedVirtualColumn(
      @JsonProperty("keyDimension") String keyDimension,
      @JsonProperty("valueDimensions") List<String> valueDimensions,
      @JsonProperty("outputName") String outputName
  )
  {
    Preconditions.checkArgument(keyDimension != null, "key dimension should not be null");
    Preconditions.checkArgument(
        valueDimensions != null && !valueDimensions.isEmpty(),
        "value dimensions should not be null or empty"
    );
    Preconditions.checkArgument(outputName != null, "output name should not be null");

    this.keyDimension = keyDimension;
    this.valueDimensions = valueDimensions;
    this.outputName = outputName;
    this.indexProvider = new IndexHolder();
  }

  @Override
  public ObjectColumnSelector asMetric(String dimension, ColumnSelectorFactory factory)
  {
    final DimensionSelector selector = factory.makeDimensionSelector(DefaultDimensionSpec.of(dimension));

    return new ObjectColumnSelector<String>()
    {
      @Override
      public Class classOfObject()
      {
        return String.class;
      }

      @Override
      public String get()
      {
        if (indexProvider.index >= 0) {
          final IndexedInts values = selector.getRow();
          if (values != null && indexProvider.index < values.size()) {
            return selector.lookupName(values.get(indexProvider.index));
          }
        }
        return null;
      }
    };
  }

  @Override
  public FloatColumnSelector asFloatMetric(String dimension, ColumnSelectorFactory factory)
  {
    final ObjectColumnSelector selector = asMetric(dimension, factory);

    return new FloatColumnSelector()
    {
      @Override
      public float get()
      {
        String v = (String) selector.get();
        return v == null ? 0 : Float.valueOf(v);
      }
    };
  }

  @Override
  public DoubleColumnSelector asDoubleMetric(String dimension, ColumnSelectorFactory factory)
  {
    final ObjectColumnSelector selector = asMetric(dimension, factory);

    return new DoubleColumnSelector()
    {
      @Override
      public double get()
      {
        String v = (String) selector.get();
        return v == null ? 0 : Double.valueOf(v);
      }
    };
  }

  @Override
  public LongColumnSelector asLongMetric(String dimension, ColumnSelectorFactory factory)
  {
    final ObjectColumnSelector selector = asMetric(dimension, factory);

    return new LongColumnSelector()
    {
      @Override
      public long get()
      {
        String v = (String) selector.get();
        return v == null ? 0 : Long.valueOf(v);
      }
    };
  }

  @Override
  public DimensionSelector asDimension(final String dimension, ColumnSelectorFactory factory)
  {
    final DimensionSelector selector = factory.makeDimensionSelector(DefaultDimensionSpec.of(keyDimension));
    return new IndexProvidingSelector()
    {
      @Override
      public ColumnSelectorFactory wrapFactory(final ColumnSelectorFactory factory)
      {
        return new ColumnSelectorFactory()
        {
          @Override
          public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
          {
            if (valueDimensions.contains(dimensionSpec.getDimension())) {
              throw new UnsupportedOperationException("makeDimensionSelector");
            }
            return factory.makeDimensionSelector(dimensionSpec);
          }

          @Override
          public FloatColumnSelector makeFloatColumnSelector(String columnName)
          {
            return valueDimensions.contains(columnName) ? asFloatMetric(columnName, factory)
                                                        : factory.makeFloatColumnSelector(columnName);
          }

          @Override
          public DoubleColumnSelector makeDoubleColumnSelector(String columnName)
          {
            return valueDimensions.contains(columnName) ? asDoubleMetric(columnName, factory)
                                                        : factory.makeDoubleColumnSelector(columnName);
          }

          @Override
          public LongColumnSelector makeLongColumnSelector(String columnName)
          {
            return valueDimensions.contains(columnName) ? asLongMetric(columnName, factory)
                                                        : factory.makeLongColumnSelector(columnName);
          }

          @Override
          public ObjectColumnSelector makeObjectColumnSelector(String columnName)
          {
            return valueDimensions.contains(columnName) ? asMetric(columnName, factory)
                                                        : factory.makeObjectColumnSelector(columnName);
          }

          @Override
          public ExprEvalColumnSelector makeMathExpressionSelector(String expression)
          {
            // todo
            throw new UnsupportedOperationException("makeMathExpressionSelector");
          }

          @Override
          public ColumnCapabilities getColumnCapabilities(String columnName)
          {
            // todo
            if (valueDimensions.contains(columnName)) {
              throw new UnsupportedOperationException("getColumnCapabilities");
            }
            return factory.getColumnCapabilities(columnName);
          }
        };
      }

      @Override
      public IndexedInts getRow()
      {
        final IndexedInts row = selector.getRow();
        return new IndexedInts()
        {
          @Override
          public int size()
          {
            return row.size();
          }

          @Override
          public int get(int index)
          {
            indexProvider.index = index;
            return row.get(index);
          }

          @Override
          public void fill(int index, int[] toFill)
          {
            throw new UnsupportedOperationException("fill");
          }

          @Override
          public void close() throws IOException
          {
            row.close();
          }

          @Override
          public Iterator<Integer> iterator()
          {
            final Iterator<Integer> iterator = row.iterator();
            return new Iterator<Integer>()
            {
              int index = 0;

              @Override
              public boolean hasNext()
              {
                return iterator.hasNext();
              }

              @Override
              public Integer next()
              {
                indexProvider.index = index++;
                return iterator.next();
              }

              @Override
              public void remove()
              {
                iterator.remove();
              }
            };
          }
        };
      }

      @Override
      public int getValueCardinality()
      {
        return selector.getValueCardinality();
      }

      @Override
      public String lookupName(int id)
      {
        return selector.lookupName(id);
      }

      @Override
      public int lookupId(String name)
      {
        return selector.lookupId(name);
      }
    };
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] key = StringUtils.toUtf8(keyDimension);
    byte[] value = QueryCacheHelper.computeCacheBytes(valueDimensions);
    byte[] output = StringUtils.toUtf8(outputName);

    return ByteBuffer.allocate(3 + key.length + value.length + output.length)
                     .put(VC_TYPE_ID)
                     .put(key).put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(value).put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(output)
                     .array();
  }

  @JsonProperty
  public String getKeyDimension()
  {
    return keyDimension;
  }

  @JsonProperty
  public List<String> getValueDimensions()
  {
    return valueDimensions;
  }

  @JsonProperty
  public String getOutputName()
  {
    return outputName;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof KeyIndexedVirtualColumn)) {
      return false;
    }

    KeyIndexedVirtualColumn that = (KeyIndexedVirtualColumn) o;

    if (!keyDimension.equals(that.keyDimension)) {
      return false;
    }
    if (!valueDimensions.equals(that.valueDimensions)) {
      return false;
    }
    if (!outputName.equals(that.outputName)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = keyDimension.hashCode();
    result = 31 * result + valueDimensions.hashCode();
    result = 31 * result + outputName.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "MapVirtualColumn{" +
           "keyDimension='" + keyDimension + '\'' +
           ", valueDimensions='" + valueDimensions + '\'' +
           ", outputName='" + outputName + '\'' +
           '}';
  }

  public static interface IndexProvidingSelector extends DimensionSelector
  {
    ColumnSelectorFactory wrapFactory(ColumnSelectorFactory factory);
  }

  private static class IndexHolder
  {
    int index = -1;
  }
}
