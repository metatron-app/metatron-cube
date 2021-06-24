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

package io.druid.segment.filter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ConciseBitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import com.metamx.collections.spatial.ImmutableRTree;
import io.druid.common.KeyBuilder;
import io.druid.data.ValueDesc;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.DimFilters;
import io.druid.query.filter.ExtractionDimFilter;
import io.druid.query.filter.Filter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.segment.ColumnPartProvider;
import io.druid.segment.ColumnPartProviders;
import io.druid.segment.bitmap.RoaringBitmapFactory;
import io.druid.segment.column.BitmapIndex;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.HistogramBitmap;
import io.druid.segment.column.LuceneIndex;
import io.druid.segment.data.BitSlicedBitmap;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.ConciseBitmapSerdeFactory;
import io.druid.segment.data.Dictionary;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.ObjectStrategy;
import io.druid.segment.data.RoaringBitmapSerdeFactory;
import io.druid.segment.data.VSizedInt;
import io.druid.segment.serde.BitmapIndexColumnPartSupplier;
import io.druid.segment.serde.DictionaryEncodedColumnSupplier;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Map;

/**
 *
 */
@RunWith(Parameterized.class)
public class ExtractionDimFilterTest
{
  @Parameterized.Parameters
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{new ConciseBitmapFactory(), new ConciseBitmapSerdeFactory()},
        new Object[]{new RoaringBitmapFactory(), new RoaringBitmapSerdeFactory()}
    );
  }

  public ExtractionDimFilterTest(BitmapFactory factory, BitmapSerdeFactory serdeFactory)
  {
    MutableBitmap mutableBitmap = factory.makeEmptyMutableBitmap();
    mutableBitmap.add(1);
    ImmutableBitmap bitmap = factory.makeImmutableBitmap(mutableBitmap);

    ColumnPartProvider<Dictionary<String>> dictionary = GenericIndexed.fromIterable(
        Arrays.asList("foo1"), ObjectStrategy.STRING_STRATEGY).asColumnPartProvider();
    ColumnPartProvider<IndexedInts> values = ColumnPartProviders.with(VSizedInt.fromArray(new int[] {0}));
    ColumnPartProvider<BitmapIndex> bitmaps = new BitmapIndexColumnPartSupplier(
        factory,
        GenericIndexed.fromIterable(Arrays.asList(bitmap), serdeFactory.getObjectStrategy()),
        dictionary
    );
    Column column = new ColumnBuilder().setType(ValueDesc.STRING)
                                       .setBitmapIndex(bitmaps)
                                       .setDictionaryEncodedColumn(
                                           new DictionaryEncodedColumnSupplier(dictionary, null, values, null))
                                       .build("foo");

    BitmapIndexSelector selector = new BitmapIndexSelector()
    {
      @Override
      public int getNumRows()
      {
        return 100;
      }

      @Override
      public BitmapFactory getBitmapFactory()
      {
        return factory;
      }

      @Override
      public ImmutableBitmap getBitmapIndex(String dimension, String value)
      {
        return null;
      }

      @Override
      public ImmutableBitmap getBitmapIndex(String dimension, Boolean value)
      {
        return null;
      }

      @Override
      public BitmapIndex getBitmapIndex(String dimension)
      {
        return bitmaps.get();
      }

      @Override
      public ImmutableRTree getSpatialIndex(String dimension)
      {
        return null;
      }

      @Override
      public LuceneIndex getLuceneIndex(String dimension)
      {
        return null;
      }

      @Override
      public HistogramBitmap getMetricBitmap(String dimension)
      {
        return null;
      }

      @Override
      public BitSlicedBitmap getBitSlicedBitmap(String dimension)
      {
        return null;
      }

      @Override
      public ColumnCapabilities getCapabilities(String dimension)
      {
        return null;
      }

      @Override
      public Column getColumn(String dimension)
      {
        return "foo".equals(dimension) ? column : null;
      }
    };
    this.context = new FilterContext(selector);
  }

  private final FilterContext context;

  private static final ExtractionFn DIM_EXTRACTION_FN = new ExtractionFn()
  {
    final Map<String, String> EXTRACTION_VALUES = ImmutableMap.of("foo1", "extractDimVal");

    @Override
    public KeyBuilder getCacheKey(KeyBuilder builder)
    {
      return builder;
    }

    @Override
    public String apply(String dimValue)
    {
      return EXTRACTION_VALUES.getOrDefault(dimValue, dimValue);
    }
  };

  @Test
  public void testEmpty()
  {
    Filter extractionFilter = new SelectorDimFilter(
        "foo", "NFDJUKFNDSJFNS", DIM_EXTRACTION_FN
    ).toFilter(null);
    BitmapHolder holder = extractionFilter.getBitmapIndex(context);
    Assert.assertEquals(0, holder.size());
  }

  @Test
  public void testNull()
  {
    Filter extractionFilter = new SelectorDimFilter(
        "FDHJSFFHDS", "extractDimVal", DIM_EXTRACTION_FN
    ).toFilter(null);
    BitmapHolder holder = extractionFilter.getBitmapIndex(context);
    Assert.assertEquals(0, holder.size());
  }

  @Test
  public void testNormal()
  {
    Filter extractionFilter = new SelectorDimFilter(
        "foo", "extractDimVal", DIM_EXTRACTION_FN
    ).toFilter(null);
    BitmapHolder holder = extractionFilter.getBitmapIndex(context);
    Assert.assertEquals(1, holder.size());
  }

  @Test
  public void testOr()
  {
    Assert.assertEquals(
        1, Filters.toFilter(
            DimFilters.or(
                new ExtractionDimFilter(
                    "foo",
                    "extractDimVal",
                    DIM_EXTRACTION_FN,
                    null
                )
            ),
            null
        ).getBitmapIndex(context).size()
    );

    Assert.assertEquals(
        1,
        Filters.toFilter(
            DimFilters.or(
                new ExtractionDimFilter(
                    "foo",
                    "extractDimVal",
                    DIM_EXTRACTION_FN,
                    null
                ),
                new ExtractionDimFilter(
                    "foo",
                    "DOES NOT EXIST",
                    DIM_EXTRACTION_FN,
                    null
                )
            ),
            null
        ).getBitmapIndex(context).size()
    );
  }

  @Test
  public void testAnd()
  {
    Assert.assertEquals(
        1, Filters.toFilter(
            DimFilters.or(
                new ExtractionDimFilter(
                    "foo",
                    "extractDimVal",
                    DIM_EXTRACTION_FN,
                    null
                )
            ),
            null
        ).getBitmapIndex(context).size()
    );

    Assert.assertEquals(
        1,
        Filters.toFilter(
            DimFilters.and(
                new ExtractionDimFilter(
                    "foo",
                    "extractDimVal",
                    DIM_EXTRACTION_FN,
                    null
                ),
                new ExtractionDimFilter(
                    "foo",
                    "extractDimVal",
                    DIM_EXTRACTION_FN,
                    null
                )
            ),
            null
        ).getBitmapIndex(context).size()
    );
  }

  @Test
  public void testNot()
  {

    Assert.assertEquals(
        1, Filters.toFilter(
            DimFilters.or(
                new ExtractionDimFilter(
                    "foo",
                    "extractDimVal",
                    DIM_EXTRACTION_FN,
                    null
                )
            ),
            null
        ).getBitmapIndex(context).size()
    );

    Assert.assertEquals(
        100,
        Filters.toFilter(
            DimFilters.not(
                new ExtractionDimFilter(
                    "foo",
                    "DOES NOT EXIST",
                    DIM_EXTRACTION_FN,
                    null
                )
            ),
            null
        ).getBitmapIndex(context).size()
    );
  }
}
