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

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.metamx.common.ISE;
import io.druid.data.ValueDesc;
import io.druid.data.input.Row;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.ValueMatcher;
import io.druid.segment.column.Column;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.serde.ComplexMetricExtractor;
import io.druid.segment.serde.ComplexMetricSerde;
import io.druid.segment.serde.ComplexMetrics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 */
public class ColumnSelectorFactories
{
  public static class NotSupports extends ColumnSelectorFactory.ExprUnSupport
  {
    @Override
    public Iterable<String> getColumnNames()
    {
      throw new UnsupportedOperationException("getColumnNames");
    }

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      throw new UnsupportedOperationException("makeDimensionSelector");
    }

    @Override
    public FloatColumnSelector makeFloatColumnSelector(String columnName)
    {
      throw new UnsupportedOperationException("makeFloatColumnSelector");
    }

    @Override
    public DoubleColumnSelector makeDoubleColumnSelector(String columnName)
    {
      throw new UnsupportedOperationException("makeDoubleColumnSelector");
    }

    @Override
    public LongColumnSelector makeLongColumnSelector(String columnName)
    {
      throw new UnsupportedOperationException("makeLongColumnSelector");
    }

    @Override
    public ObjectColumnSelector makeObjectColumnSelector(String columnName)
    {
      throw new UnsupportedOperationException("makeObjectColumnSelector");
    }

    @Override
    public ValueDesc getColumnType(String columnName)
    {
      throw new UnsupportedOperationException("getColumnType");
    }
  }

  public static class Delegated implements ColumnSelectorFactory
  {
    protected final ColumnSelectorFactory delegate;

    public Delegated(ColumnSelectorFactory delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      return delegate.makeDimensionSelector(dimensionSpec);
    }

    @Override
    public FloatColumnSelector makeFloatColumnSelector(String columnName)
    {
      return delegate.makeFloatColumnSelector(columnName);
    }

    @Override
    public DoubleColumnSelector makeDoubleColumnSelector(String columnName)
    {
      return delegate.makeDoubleColumnSelector(columnName);
    }

    @Override
    public LongColumnSelector makeLongColumnSelector(String columnName)
    {
      return delegate.makeLongColumnSelector(columnName);
    }

    @Override
    public ObjectColumnSelector makeObjectColumnSelector(String columnName)
    {
      return delegate.makeObjectColumnSelector(columnName);
    }

    @Override
    public ExprEvalColumnSelector makeMathExpressionSelector(String expression)
    {
      return delegate.makeMathExpressionSelector(expression);
    }

    @Override
    public ValueMatcher makeAuxiliaryMatcher(DimFilter filter)
    {
      return delegate.makeAuxiliaryMatcher(filter);
    }

    @Override
    public ValueDesc getColumnType(String columnName)
    {
      return delegate.getColumnType(columnName);
    }

    @Override
    public Iterable<String> getColumnNames()
    {
      return delegate.getColumnNames();
    }
  }

  public static abstract class ArrayIndexed extends ColumnSelectorFactory.ExprUnSupport
  {
    protected final ObjectColumnSelector selector;
    protected final ValueDesc elementType;

    protected ArrayIndexed(ObjectColumnSelector selector, ValueDesc elementType)
    {
      this.selector = selector;
      this.elementType = elementType;
    }

    public ObjectColumnSelector getSelector()
    {
      return selector;
    }

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      throw new UnsupportedOperationException("makeDimensionSelector");
    }

    @Override
    public Iterable<String> getColumnNames()
    {
      throw new UnsupportedOperationException("getColumnNames");
    }

    @Override
    public ValueDesc getColumnType(String columnName)
    {
      throw new UnsupportedOperationException("getColumnType");
    }

    protected abstract Object getObject();

    @Override
    public FloatColumnSelector makeFloatColumnSelector(String columnName)
    {
      return new FloatColumnSelector()
      {
        @Override
        public float get()
        {
          Object value = getObject();
          if (value == null) {
            return 0.0f;
          }
          if (value instanceof Number) {
            return ((Number) value).floatValue();
          }
          return Float.valueOf(String.valueOf(value));
        }
      };
    }

    @Override
    public DoubleColumnSelector makeDoubleColumnSelector(String columnName)
    {
      return new DoubleColumnSelector()
      {
        @Override
        public double get()
        {
          Object value = getObject();
          if (value == null) {
            return 0.0d;
          }
          if (value instanceof Number) {
            return ((Number) value).doubleValue();
          }
          return Double.valueOf(String.valueOf(value));
        }
      };
    }

    @Override
    public LongColumnSelector makeLongColumnSelector(String columnName)
    {
      return new LongColumnSelector()
      {
        @Override
        public long get()
        {
          Object value = getObject();
          if (value == null) {
            return 0L;
          }
          if (value instanceof Number) {
            return ((Number) value).longValue();
          }
          return Long.valueOf(String.valueOf(value));
        }
      };
    }

    @Override
    public ObjectColumnSelector makeObjectColumnSelector(String columnName)
    {
      return new ObjectColumnSelector()
      {
        @Override
        public ValueDesc type()
        {
          return elementType;
        }

        @Override
        public Object get()
        {
          return getObject();
        }
      };
    }
  }

  public static final class FixedArrayIndexed extends ArrayIndexed
  {
    private final int index;

    public FixedArrayIndexed(int index, ObjectColumnSelector selector, ValueDesc elementType)
    {
      super(selector, elementType);
      this.index = index;
    }

    protected final Object getObject()
    {
      List value = (List) selector.get();
      return value == null ? null : value.get(index);
    }
  }

  public static final class VariableArrayIndexed extends ArrayIndexed
  {
    private int index = -1;

    public VariableArrayIndexed(ObjectColumnSelector selector, ValueDesc elementType)
    {
      super(selector, elementType);
    }

    public void setIndex(int index)
    {
      this.index = index;
    }

    protected final Object getObject()
    {
      List value = (List) selector.get();
      return value == null ? null : value.get(index);
    }
  }

  public static final class FromRow extends ColumnSelectorFactory.ExprSupport
  {
    private final AggregatorFactory agg;
    private final Supplier<Row> in;
    private final boolean deserializeComplexMetrics;

    public FromRow(
        final AggregatorFactory agg,
        final Supplier<Row> in,
        final boolean deserializeComplexMetrics
    )
    {
      this.agg = agg;
      this.in = in;
      this.deserializeComplexMetrics = deserializeComplexMetrics;
    }

    @Override
    public LongColumnSelector makeLongColumnSelector(final String columnName)
    {
      if (columnName.equals(Column.TIME_COLUMN_NAME)) {
        return new LongColumnSelector()
        {
          @Override
          public long get()
          {
            return in.get().getTimestampFromEpoch();
          }
        };
      }
      return new LongColumnSelector()
      {
        @Override
        public long get()
        {
          return in.get().getLongMetric(columnName);
        }
      };
    }

    @Override
    public FloatColumnSelector makeFloatColumnSelector(final String columnName)
    {
      return new FloatColumnSelector()
      {
        @Override
        public float get()
        {
          return in.get().getFloatMetric(columnName);
        }
      };
    }

    @Override
    public DoubleColumnSelector makeDoubleColumnSelector(final String columnName)
    {
      return new DoubleColumnSelector()
      {
        @Override
        public double get()
        {
          return in.get().getDoubleMetric(columnName);
        }
      };
    }

    @Override
    public ObjectColumnSelector makeObjectColumnSelector(final String column)
    {
      if (Column.TIME_COLUMN_NAME.equals(column)) {
        return new ObjectColumnSelector()
        {
          @Override
          public ValueDesc type()
          {
            return ValueDesc.LONG;
          }

          @Override
          public Object get()
          {
            return in.get().getTimestampFromEpoch();
          }
        };
      }

      final String typeName = agg.getInputTypeName();
      final ValueDesc type = ValueDesc.of(typeName);

      final boolean dimension = ValueDesc.isDimension(type);

      if (dimension || ValueDesc.isPrimitive(type) || !deserializeComplexMetrics) {
        return new ObjectColumnSelector()
        {
          @Override
          public ValueDesc type()
          {
            return type;
          }

          @Override
          public Object get()
          {
            if (dimension) {
              return in.get().getDimension(column);
            }
            switch (type.type()) {
              case FLOAT:
                return in.get().getFloatMetric(column);
              case LONG:
                return in.get().getLongMetric(column);
              case DOUBLE:
                return in.get().getDoubleMetric(column);
            }
            return in.get().getRaw(column);
          }
        };
      } else {
        final ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(typeName);
        if (serde == null) {
          throw new ISE("Don't know how to handle type[%s]", typeName);
        }

        final ComplexMetricExtractor extractor = serde.getExtractor();
        return new ObjectColumnSelector()
        {
          @Override
          public ValueDesc type()
          {
            return type;
          }

          @Override
          public Object get()
          {
            return extractor.extractValue(in.get(), column);
          }
        };
      }
    }

    @Override
    public ValueMatcher makeAuxiliaryMatcher(DimFilter filter)
    {
      return null;
    }

    @Override
    public ValueDesc getColumnType(String columnName)
    {
      return null;
    }

    @Override
    public Iterable<String> getColumnNames()
    {
      return null;
    }

    @Override
    public DimensionSelector makeDimensionSelector(
        DimensionSpec dimensionSpec
    )
    {
      return dimensionSpec.decorate(makeDimensionSelectorUndecorated(dimensionSpec));
    }

    private DimensionSelector makeDimensionSelectorUndecorated(
        DimensionSpec dimensionSpec
    )
    {
      final String dimension = dimensionSpec.getDimension();
      final ExtractionFn extractionFn = dimensionSpec.getExtractionFn();

      return new DimensionSelector()
      {
        @Override
        public IndexedInts getRow()
        {
          final List<String> dimensionValues = in.get().getDimension(dimension);
          final ArrayList<Integer> vals = Lists.newArrayList();
          if (dimensionValues != null) {
            for (int i = 0; i < dimensionValues.size(); ++i) {
              vals.add(i);
            }
          }

          return new IndexedInts()
          {
            @Override
            public int size()
            {
              return vals.size();
            }

            @Override
            public int get(int index)
            {
              return vals.get(index);
            }

            @Override
            public Iterator<Integer> iterator()
            {
              return vals.iterator();
            }

            @Override
            public void close() throws IOException
            {
            }

            @Override
            public void fill(int index, int[] toFill)
            {
              throw new UnsupportedOperationException("fill not supported");
            }
          };
        }

        @Override
        public int getValueCardinality()
        {
          throw new UnsupportedOperationException("value cardinality is unknown in incremental index");
        }

        @Override
        public String lookupName(int id)
        {
          final String value = in.get().getDimension(dimension).get(id);
          return extractionFn == null ? value : extractionFn.apply(value);
        }

        @Override
        public int lookupId(String name)
        {
          if (extractionFn != null) {
            throw new UnsupportedOperationException("cannot perform lookup when applying an extraction function");
          }
          return in.get().getDimension(dimension).indexOf(name);
        }
      };
    }
  }
}
