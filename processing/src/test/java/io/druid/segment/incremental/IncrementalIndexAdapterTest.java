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

package io.druid.segment.incremental;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.data.input.MapBasedInputRow;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.IndexSpec;
import io.druid.segment.IndexableAdapter;
import io.druid.segment.Rowboat;
import io.druid.segment.TestHelper;
import io.druid.segment.data.CompressedObjectStrategy;
import io.druid.segment.data.ConciseBitmapSerdeFactory;
import io.druid.segment.data.IncrementalIndexTest;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class IncrementalIndexAdapterTest
{
  private static final IndexSpec INDEX_SPEC = new IndexSpec(
      new ConciseBitmapSerdeFactory(),
      CompressedObjectStrategy.CompressionStrategy.LZ4.name().toLowerCase(),
      CompressedObjectStrategy.CompressionStrategy.LZ4.name().toLowerCase()
  );

  @Test
  public void testGetBitmapIndex() throws Exception
  {
    final long timestamp = System.currentTimeMillis();
    IncrementalIndex incrementalIndex = IncrementalIndexTest.createIndex(null);
    IncrementalIndexTest.populateIndex(timestamp, incrementalIndex);
    IndexableAdapter adapter = new IncrementalIndexAdapter(
        incrementalIndex.getInterval(),
        incrementalIndex,
        INDEX_SPEC.getBitmapSerdeFactory().getBitmapFactory()
    );
    String dimension = "dim1";
    for (int i = 0; i < adapter.getDimValueLookup(dimension).size(); i++) {
      ImmutableBitmap bitmap = adapter.getBitmaps(dimension).apply(i);
      Assert.assertEquals(1, bitmap.size());
    }
  }

  @Test
  public void testGetRowsIterable() throws Exception
  {
    final long timestamp = System.currentTimeMillis();
    IncrementalIndex toPersist1 = IncrementalIndexTest.createIndex(null);
    IncrementalIndexTest.populateIndex(timestamp, toPersist1);

    final IndexableAdapter incrementalAdapter = new IncrementalIndexAdapter(
        toPersist1.getInterval(),
        toPersist1,
        INDEX_SPEC.getBitmapSerdeFactory()
                  .getBitmapFactory()
    );

    Iterable<Rowboat> boats = incrementalAdapter.getRows();
    List<Rowboat> boatList = new ArrayList<>();
    for (Rowboat boat : boats) {
      boatList.add(boat);
    }
    Assert.assertEquals(2, boatList.size());
    Assert.assertEquals(0, boatList.get(0).getRowNum());
    Assert.assertEquals(1, boatList.get(1).getRowNum());

    /* Iterate through the Iterable a few times, check that boat row numbers are correct afterwards */
    boatList = new ArrayList<>();
    for (Rowboat boat : boats) {
      boatList.add(boat);
    }
    boatList = new ArrayList<>();
    for (Rowboat boat : boats) {
      boatList.add(boat);
    }
    boatList = new ArrayList<>();
    for (Rowboat boat : boats) {
      boatList.add(boat);
    }
    boatList = new ArrayList<>();
    for (Rowboat boat : boats) {
      boatList.add(boat);
    }

    Assert.assertEquals(2, boatList.size());
    Assert.assertEquals(0, boatList.get(0).getRowNum());
    Assert.assertEquals(1, boatList.get(1).getRowNum());

  }

  @Test
  public void testGetRowsIterableWithNoRollup() throws Exception
  {
    final long timestamp1 = System.currentTimeMillis();
    final long timestamp2 = timestamp1 + 100;
    IncrementalIndex index = IncrementalIndexTest.createNoRollupIndex(new AggregatorFactory[0]);

    List<String> dimensions = Arrays.asList("dim1");
    index.add(new MapBasedInputRow(timestamp1, dimensions, ImmutableMap.<String, Object>of("dim1", "a")));
    index.add(new MapBasedInputRow(timestamp1, dimensions, ImmutableMap.<String, Object>of("dim1", "a")));
    index.add(new MapBasedInputRow(timestamp2, dimensions, ImmutableMap.<String, Object>of("dim1", "a")));
    index.add(new MapBasedInputRow(timestamp2, dimensions, ImmutableMap.<String, Object>of("dim1", "b")));
    index.add(new MapBasedInputRow(timestamp2, dimensions, ImmutableMap.<String, Object>of("dim1", "b")));

    Assert.assertEquals(5, index.size());
    final IndexableAdapter incrementalAdapter = new IncrementalIndexAdapter(
        index.getInterval(),
        index,
        INDEX_SPEC.getBitmapSerdeFactory()
                  .getBitmapFactory()
    );

    Iterable<Rowboat> boats = incrementalAdapter.getRows();
    Assert.assertEquals(5, Iterables.size(boats));
    File tempFile = File.createTempFile("asd", "asd");
    tempFile.delete();
    tempFile.mkdirs();
    TestHelper.getTestIndexMergerV9().persist(index, tempFile, INDEX_SPEC);
  }
}
