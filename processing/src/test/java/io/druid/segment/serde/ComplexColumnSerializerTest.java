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

import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.data.ValueDesc;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnDescriptor;
import io.druid.segment.column.LuceneIndex;
import io.druid.segment.data.IOPeon;
import io.druid.segment.data.RoaringBitmapSerdeFactory;
import io.druid.segment.data.TmpFileIOPeon;
import io.druid.segment.filter.FilterContext;
import io.druid.segment.lucene.LatLonPointIndexingStrategy;
import io.druid.segment.lucene.LuceneIndexingSpec;
import io.druid.segment.lucene.Lucenes;
import io.druid.segment.lucene.TextIndexingStrategy;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.geo.Polygon;
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
    ImmutableBitmap bitmap = luceneIndex.filterFor(query, context).bitmap();

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
  public void testLatLon() throws Exception
  {
    ValueDesc type = ValueDesc.of("struct(address:string,lat:double,lon:double)");
    ComplexMetricSerde serde = ComplexMetrics.getSerdeForType(type);
    ComplexColumnSerializer serializer = ComplexColumnSerializer.create(
        ioPeon,
        "test-lucene",
        serde,
        LuceneIndexingSpec.of(
            null,
            new LatLonPointIndexingStrategy("coord", "lat", "lon", null),
            new TextIndexingStrategy("address")
        ),
        null
    );
    serializer.open();

    serializer.serialize(0, new Object[] {"home", 37.492929d, 127.020784d});
    serializer.serialize(1, new Object[] {"school", 37.491055d, 127.026020d});
    serializer.serialize(2, new Object[] {"cathedral", 37.492899d, 127.021772d});
    serializer.serialize(3, new Object[] {"college", 37.489955d, 127.016485d});
    serializer.serialize(4, new Object[] {"subway", 37.493021d, 127.013834d});

    serializer.close();

    ColumnDescriptor descriptor = serializer.buildDescriptor(type, new ColumnDescriptor.Builder()).build();

    long length = descriptor.numBytes();

    ByteBuffer payload = serialize(descriptor);

    Assert.assertEquals(length, payload.remaining());

    final RoaringBitmapSerdeFactory factory = new RoaringBitmapSerdeFactory();
    Column column = descriptor.read("test", payload, factory);
    LuceneIndex luceneIndex = column.getLuceneIndex();

    Assert.assertNotNull(luceneIndex);

    FilterContext context = new FilterContext(new BitmapIndexSelector.Abstract()
    {
      public BitmapFactory getBitmapFactory()
      {
        return factory.getBitmapFactory();
      }
    });
    QueryParser parser = new QueryParser("address", Lucenes.createAnalyzer("standard"));
    Query query = parser.parse("\"school\"");
    ImmutableBitmap bitmap = luceneIndex.filterFor(query, context).bitmap();
    Assert.assertEquals(1, bitmap.size());
    Assert.assertEquals(true, bitmap.get(1));

    // from home
    query = LatLonPoint.newDistanceQuery("coord", 37.492929d, 127.020784d, 10);
    bitmap = luceneIndex.filterFor(query, context).bitmap();
    Assert.assertEquals(1, bitmap.size());
    Assert.assertEquals(true, bitmap.get(0));

    query = LatLonPoint.newDistanceQuery("coord", 37.492929d, 127.020784d, 100);
    bitmap = luceneIndex.filterFor(query, context).bitmap();
    Assert.assertEquals(2, bitmap.size());
    Assert.assertEquals(true, bitmap.get(0));
    Assert.assertEquals(true, bitmap.get(2));

    query = LatLonPoint.newDistanceQuery("coord", 37.492929d, 127.020784d, 600);
    bitmap = luceneIndex.filterFor(query, context).bitmap();
    Assert.assertEquals(4, bitmap.size());
    Assert.assertEquals(true, bitmap.get(0));
    Assert.assertEquals(true, bitmap.get(1));
    Assert.assertEquals(true, bitmap.get(2));
    Assert.assertEquals(true, bitmap.get(3));

    query = LatLonPoint.newDistanceQuery("coord", 37.492929d, 127.020784d, 800);
    bitmap = luceneIndex.filterFor(query, context).bitmap();
    Assert.assertEquals(5, bitmap.size());

    // 래미안 ~ 우성
    query = LatLonPoint.newBoxQuery("coord", 37.490215, 37.493298, 127.019866, 127.028222);
    bitmap = luceneIndex.filterFor(query, context).bitmap();
    Assert.assertEquals(3, bitmap.size());
    Assert.assertEquals(true, bitmap.get(0));
    Assert.assertEquals(true, bitmap.get(1));
    Assert.assertEquals(true, bitmap.get(2));

    // 래미안 ~ 성당놀이터 ~ 한일
    // 37.493296, 127.019841, 37.492742, 127.021931, 37.494323, 127.021177
    Polygon polygon = new Polygon(
        new double[] {37.493296, 37.492742, 37.494323, 37.493296},
        new double[] {127.019841, 127.021931, 127.021177, 127.019841}
    );
    query = LatLonPoint.newPolygonQuery("coord", polygon);
    bitmap = luceneIndex.filterFor(query, context).bitmap();
    Assert.assertEquals(1, bitmap.size());
    Assert.assertEquals(true, bitmap.get(2));
  }
}
