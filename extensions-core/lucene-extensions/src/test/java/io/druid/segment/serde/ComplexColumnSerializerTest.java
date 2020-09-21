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

package io.druid.segment.serde;

import com.google.common.collect.ImmutableMap;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.math.expr.Parser;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.query.filter.LuceneSpatialFilter;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnDescriptor;
import io.druid.segment.column.LuceneIndex;
import io.druid.segment.data.IOPeon;
import io.druid.segment.data.RoaringBitmapSerdeFactory;
import io.druid.segment.data.TmpFileIOPeon;
import io.druid.segment.filter.FilterContext;
import io.druid.segment.lucene.LuceneIndexingSpec;
import io.druid.segment.lucene.Lucenes;
import io.druid.segment.lucene.ShapeFormat;
import io.druid.segment.lucene.ShapeIndexingStrategy;
import io.druid.segment.lucene.SpatialOperations;
import io.druid.segment.lucene.TextIndexingStrategy;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class ComplexColumnSerializerTest
{
  static {
    if (ComplexMetrics.getSerdeForType("string") == null) {
      ComplexMetrics.registerSerde("string", StringMetricSerde.INSTANCE);
    }
    if (ComplexMetrics.getSerdeFactory(ValueDesc.STRUCT_TYPE) == null) {
      ComplexMetrics.registerSerdeFactory(ValueDesc.STRUCT_TYPE, new StructMetricSerde.Factory());
    }
  }

  private final IOPeon ioPeon = new TmpFileIOPeon();

  @Test
  public void testLucene() throws Exception
  {
    ComplexColumnSerializer serializer = ComplexColumnSerializer.create(
        ioPeon,
        "test-lucene",
        StringMetricSerde.INSTANCE,
        LuceneIndexingSpec.of(null, new TextIndexingStrategy("test-lucene")),
        null
    );
    serializer.open();

    serializer.serialize(0, "navis manse");
    serializer.serialize(1, "");
    serializer.serialize(2, "navis banzai");
    serializer.serialize(3, null);
    serializer.serialize(4, "나비스 만세");

    serializer.close();

    ColumnDescriptor descriptor = serializer.buildDescriptor(ValueDesc.STRING, new ColumnDescriptor.Builder()).build();

    long length = descriptor.numBytes();

    ByteBuffer payload = serialize(descriptor);

    Assert.assertEquals(length, payload.remaining());

    final RoaringBitmapSerdeFactory factory = new RoaringBitmapSerdeFactory();
    Column column = descriptor.read("test", payload, factory);
    FilterContext context = new FilterContext(new BitmapIndexSelector.Abstract()
    {
      public BitmapFactory getBitmapFactory()
      {
        return factory.getBitmapFactory();
      }
    });
    LuceneIndex luceneIndex = column.getLuceneIndex();

    Assert.assertNotNull(luceneIndex);

    QueryParser parser = new QueryParser("test-lucene", Lucenes.createAnalyzer("standard"));
    Query query = parser.parse("\"navis\"");
    ImmutableBitmap bitmap = luceneIndex.filterFor(query, context);

    Assert.assertEquals(2, bitmap.size());
    Assert.assertEquals(true, bitmap.get(0));
    Assert.assertEquals(false, bitmap.get(1));
    Assert.assertEquals(true, bitmap.get(2));
    Assert.assertEquals(false, bitmap.get(3));
    Assert.assertEquals(false, bitmap.get(4));
  }

  private ByteBuffer serialize(ColumnDescriptor descriptor) throws IOException
  {
    final ByteArrayOutputStream bout = new ByteArrayOutputStream();
    descriptor.write(
        new WritableByteChannel()
        {
          @Override
          public int write(ByteBuffer src) throws IOException
          {
            byte[] array = new byte[src.remaining()];
            src.get(array);
            bout.write(array);
            return array.length;
          }

          @Override
          public boolean isOpen()
          {
            return true;
          }

          @Override
          public void close() throws IOException
          {
          }
        }
    );
    return ByteBuffer.wrap(bout.toByteArray());
  }

  @Test
  public void testSpatial() throws Exception
  {
    ValueDesc type = ValueDesc.STRING;
    ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(type);
    ComplexColumnSerializer serializer = ComplexColumnSerializer.create(
        ioPeon,
        "test-lucene",
        serde,
        LuceneIndexingSpec.of(
            null,
            new ShapeIndexingStrategy("geom", ShapeFormat.WKT, null, -1)
        ),
        null
    );
    serializer.open();

    // lon-lat ?
    String 서초1동 = "POLYGON(("
                  + "127.013760 37.493559, 127.014645 37.488400, 127.022991 37.490962, 127.021168 37.495816, "
                  + "127.013760 37.493559))";

    String 교대 = "POLYGON(("
                + "127.014203 37.491219, 127.014694 37.488470, 127.018743 37.489694, 127.018016 37.490979, "
                + "127.017976 37.491006, 127.017876 37.491037, 127.017415 37.491063, 127.017417 37.491056, "
                + "127.017282 37.491099, 127.017148 37.491165, 127.016992 37.491315, 127.016871 37.491521, "
                + "127.014203 37.491219))";
    String 서이초 = "POLYGON(("
                 + "127.024938 37.491432, 127.025457 37.490409, 127.027007 37.490946, 127.026522 37.491909, "
                 + "127.024938 37.491432))";
    String 서초성당 = "POLYGON(("
                + "127.021456 37.493129, 127.021951 37.493261, 127.022137 37.492740, 127.021639 37.492607, "
                + "127.021456 37.493129))";

    serializer.serialize(0, 교대);
    serializer.serialize(1, 서이초);
    serializer.serialize(2, 서초성당);

    serializer.close();

    ColumnDescriptor descriptor = serializer.buildDescriptor(type, new ColumnDescriptor.Builder()).build();

    long length = descriptor.numBytes();

    ByteBuffer payload = serialize(descriptor);

    Assert.assertEquals(length, payload.remaining());

    final RoaringBitmapSerdeFactory factory = new RoaringBitmapSerdeFactory();
    Column column = descriptor.read("test", payload, factory);

    final LuceneIndex luceneIndex = column.getLuceneIndex();

    Assert.assertNotNull(luceneIndex);

    BitmapIndexSelector selector = new BitmapIndexSelector.Abstract()
    {
      @Override
      public LuceneIndex getLuceneIndex(String dimension)
      {
        return luceneIndex;
      }

      @Override
      public BitmapFactory getBitmapFactory()
      {
        return factory.getBitmapFactory();
      }
    };
    LuceneSpatialFilter filter = new LuceneSpatialFilter("geom", SpatialOperations.COVEREDBY, ShapeFormat.WKT, 서초1동);
    TypeResolver resolver = Parser.withTypeMap(ImmutableMap.of("geom", ValueDesc.STRING));
    ImmutableBitmap bitmap = filter.toFilter(resolver).getBitmapIndex(new FilterContext(selector));
    Assert.assertEquals(2, bitmap.size());
    Assert.assertEquals(true, bitmap.get(0));
    Assert.assertEquals(true, bitmap.get(2));
  }
}
