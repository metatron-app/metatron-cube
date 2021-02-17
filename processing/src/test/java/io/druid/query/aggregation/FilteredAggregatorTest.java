/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation;

import com.google.common.collect.Lists;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.js.JavaScriptConfig;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.JavaScriptExtractionFn;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.InDimFilter;
import io.druid.query.filter.JavaScriptDimFilter;
import io.druid.query.filter.NotDimFilter;
import io.druid.query.filter.OrDimFilter;
import io.druid.query.filter.RegexDimFilter;
import io.druid.query.filter.SearchQueryDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.search.search.ContainsSearchQuerySpec;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.ColumnSelectors;
import io.druid.segment.DimensionSelector;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.data.ArrayBasedIndexedInts;
import io.druid.segment.data.IndexedInts;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class FilteredAggregatorTest
{
  private Object aggregate(TestFloatColumnSelector selector, Aggregator agg, Object aggregate)
  {
    aggregate = agg.aggregate(aggregate);
    selector.increment();
    return aggregate;
  }

  @Test
  public void testAggregate()
  {
    final float[] values = {0.15f, 0.27f};
    final TestFloatColumnSelector selector = new TestFloatColumnSelector(values);

    FilteredAggregatorFactory factory = new FilteredAggregatorFactory(
        new DoubleSumAggregatorFactory("billy", "value"),
        new SelectorDimFilter("dim", "a", null)
    );

    Aggregator agg = factory.factorize(makeColumnSelector(selector));

    double expectedFirst = new Float(values[0]).doubleValue();
    double expectedSecond = new Float(values[1]).doubleValue() + expectedFirst;
    double expectedThird = expectedSecond;

    assertValues(agg, selector, expectedFirst, expectedSecond, expectedThird);
  }

  private ColumnSelectorFactory makeColumnSelector(final TestFloatColumnSelector selector){

    return new ColumnSelectorFactory.ExprUnSupport()
    {
      @Override
      public Iterable<String> getColumnNames()
      {
        return Arrays.asList("dim");
      }

      @Override
      public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
      {
        final String dimensionName = dimensionSpec.getDimension();

        if (dimensionName.equals("dim")) {
          return dimensionSpec.decorate(
              new DimensionSelector()
              {
                @Override
                public IndexedInts getRow()
                {
                  if (selector.getIndex() % 3 == 2) {
                    return new ArrayBasedIndexedInts(new int[]{1});
                  } else {
                    return new ArrayBasedIndexedInts(new int[]{0});
                  }
                }

                @Override
                public int getValueCardinality()
                {
                  return 2;
                }

                @Override
                public Comparable lookupName(int id)
                {
                  switch (id) {
                    case 0:
                      return "a";
                    case 1:
                      return "b";
                    default:
                      throw new IllegalArgumentException();
                  }
                }

                @Override
                public ValueDesc type()
                {
                  return ValueDesc.STRING;
                }

                @Override
                public int lookupId(Comparable name)
                {
                  switch ((String) name) {
                    case "a":
                      return 0;
                    case "b":
                      return 1;
                    default:
                      throw new IllegalArgumentException();
                  }
                }

                @Override
                public boolean withSortedDictionary()
                {
                  return true;
                }
              },
              this
          );
        } else {
          throw new UnsupportedOperationException();
        }
      }

      @Override
      public LongColumnSelector makeLongColumnSelector(String columnName)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public FloatColumnSelector makeFloatColumnSelector(String columnName)
      {
        if (columnName.equals("value")) {
          return selector;
        } else {
          throw new UnsupportedOperationException();
        }
      }

      @Override
      public DoubleColumnSelector makeDoubleColumnSelector(String columnName)
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public ObjectColumnSelector makeObjectColumnSelector(String columnName)
      {
        if (columnName.equals("value")) {
          return new ObjectColumnSelector()
          {
            @Override
            public Object get()
            {
              return selector.get();
            }

            @Override
            public ValueDesc type()
            {
              return ValueDesc.FLOAT;
            }
          };
        }
        if (columnName.equals("dim")) {
          return ColumnSelectors.asMultiValued(makeDimensionSelector(DefaultDimensionSpec.of(columnName)));
        }
        throw new UnsupportedOperationException();
      }

      @Override
      public ValueDesc resolve(String columnName)
      {
        if (columnName.equals("value")) {
          return ValueDesc.FLOAT;
        }
        if (columnName.equals("dim")) {
          return ValueDesc.DIM_STRING;
        }
        return null;
      }
    };
  }

  private void assertValues(Aggregator agg,TestFloatColumnSelector selector, double... expectedVals){
    Object aggregate = null;
    Assert.assertEquals(0.0d, agg.get(aggregate));
    Assert.assertEquals(0.0d, agg.get(aggregate));
    Assert.assertEquals(0.0d, agg.get(aggregate));
    for(double expectedVal : expectedVals){
      aggregate = aggregate(selector, agg, aggregate);
      Assert.assertEquals(expectedVal, agg.get(aggregate));
      Assert.assertEquals(expectedVal, agg.get(aggregate));
      Assert.assertEquals(expectedVal, agg.get(aggregate));
    }
  }

  @Test
  public void testAggregateWithNotFilter()
  {
    final float[] values = {0.15f, 0.27f};
    final TestFloatColumnSelector selector = new TestFloatColumnSelector(values);

    FilteredAggregatorFactory factory = new FilteredAggregatorFactory(
        new DoubleSumAggregatorFactory("billy", "value"),
        new NotDimFilter(new SelectorDimFilter("dim", "b", null))
    );

    validateFilteredAggs(factory, values, selector);
  }

  @Test
  public void testAggregateWithOrFilter()
  {
    final float[] values = {0.15f, 0.27f, 0.14f};
    final TestFloatColumnSelector selector = new TestFloatColumnSelector(values);

    FilteredAggregatorFactory factory = new FilteredAggregatorFactory(
        new DoubleSumAggregatorFactory("billy", "value"),
        new OrDimFilter(Lists.<DimFilter>newArrayList(new SelectorDimFilter("dim", "a", null), new SelectorDimFilter("dim", "b", null)))
    );

    Aggregator agg = factory.factorize(makeColumnSelector(selector));

    double expectedFirst = new Float(values[0]).doubleValue();
    double expectedSecond = new Float(values[1]).doubleValue() + expectedFirst;
    double expectedThird = expectedSecond + new Float(values[2]).doubleValue();
    assertValues(agg, selector, expectedFirst, expectedSecond, expectedThird);
  }

  @Test
  public void testAggregateWithAndFilter()
  {
    final float[] values = {0.15f, 0.27f};
    final TestFloatColumnSelector selector = new TestFloatColumnSelector(values);

    FilteredAggregatorFactory factory = new FilteredAggregatorFactory(
        new DoubleSumAggregatorFactory("billy", "value"),
        new AndDimFilter(Lists.<DimFilter>newArrayList(new NotDimFilter(new SelectorDimFilter("dim", "b", null)), new SelectorDimFilter("dim", "a", null))));

    validateFilteredAggs(factory, values, selector);
  }

  @Test
  public void testAggregateWithPredicateFilters()
  {
    final float[] values = {0.15f, 0.27f};
    TestFloatColumnSelector selector;
    FilteredAggregatorFactory factory;

    factory = new FilteredAggregatorFactory(
        new DoubleSumAggregatorFactory("billy", "value"),
        new BoundDimFilter("dim", "a", "a", false, false, true, null)
    );
    selector = new TestFloatColumnSelector(values);
    validateFilteredAggs(factory, values, selector);

    factory = new FilteredAggregatorFactory(
        new DoubleSumAggregatorFactory("billy", "value"),
        new RegexDimFilter("dim", "a", null)
    );
    selector = new TestFloatColumnSelector(values);
    validateFilteredAggs(factory, values, selector);

    factory = new FilteredAggregatorFactory(
        new DoubleSumAggregatorFactory("billy", "value"),
        new SearchQueryDimFilter("dim", new ContainsSearchQuerySpec("a", true), null)
    );
    selector = new TestFloatColumnSelector(values);
    validateFilteredAggs(factory, values, selector);

    String jsFn = "function(x) { return(x === 'a') }";
    factory = new FilteredAggregatorFactory(
        new DoubleSumAggregatorFactory("billy", "value"),
        new JavaScriptDimFilter("dim", jsFn, null, JavaScriptConfig.getDefault())
    );
    selector = new TestFloatColumnSelector(values);
    validateFilteredAggs(factory, values, selector);
  }

  @Test
  public void testAggregateWithExtractionFns()
  {
    final float[] values = {0.15f, 0.27f};
    TestFloatColumnSelector selector;
    FilteredAggregatorFactory factory;

    String extractionJsFn = "function(str) { return str + 'AARDVARK'; }";
    ExtractionFn extractionFn = new JavaScriptExtractionFn(extractionJsFn, false, JavaScriptConfig.getDefault());

    factory = new FilteredAggregatorFactory(
        new DoubleSumAggregatorFactory("billy", "value"),
        new SelectorDimFilter("dim", "aAARDVARK", extractionFn)
    );
    selector = new TestFloatColumnSelector(values);
    validateFilteredAggs(factory, values, selector);

    factory = new FilteredAggregatorFactory(
        new DoubleSumAggregatorFactory("billy", "value"),
        new InDimFilter("dim", Arrays.asList("NOT-aAARDVARK", "FOOBAR", "aAARDVARK"), extractionFn)
    );
    selector = new TestFloatColumnSelector(values);
    validateFilteredAggs(factory, values, selector);
    
    factory = new FilteredAggregatorFactory(
        new DoubleSumAggregatorFactory("billy", "value"),
        new BoundDimFilter("dim", "aAARDVARK", "aAARDVARK", false, false, true, extractionFn)
    );
    selector = new TestFloatColumnSelector(values);
    validateFilteredAggs(factory, values, selector);

    factory = new FilteredAggregatorFactory(
        new DoubleSumAggregatorFactory("billy", "value"),
        new RegexDimFilter("dim", "aAARDVARK", extractionFn)
    );
    selector = new TestFloatColumnSelector(values);
    validateFilteredAggs(factory, values, selector);

    factory = new FilteredAggregatorFactory(
        new DoubleSumAggregatorFactory("billy", "value"),
        new SearchQueryDimFilter("dim", new ContainsSearchQuerySpec("aAARDVARK", true), extractionFn)
    );
    selector = new TestFloatColumnSelector(values);
    validateFilteredAggs(factory, values, selector);

    String jsFn = "function(x) { return(x === 'aAARDVARK') }";
    factory = new FilteredAggregatorFactory(
        new DoubleSumAggregatorFactory("billy", "value"),
        new JavaScriptDimFilter("dim", jsFn, extractionFn, JavaScriptConfig.getDefault())
    );
    selector = new TestFloatColumnSelector(values);
    validateFilteredAggs(factory, values, selector);
  }

  private void validateFilteredAggs(
      FilteredAggregatorFactory factory,
      float[] values,
      TestFloatColumnSelector selector
  )
  {
    Aggregator agg = factory.factorize(makeColumnSelector(selector));

    double expectedFirst = new Float(values[0]).doubleValue();
    double expectedSecond = new Float(values[1]).doubleValue() + expectedFirst;
    double expectedThird = expectedSecond;

    assertValues(agg, selector, expectedFirst, expectedSecond, expectedThird);
  }
}
