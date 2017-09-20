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

package io.druid.segment.serde;

import com.metamx.collections.bitmap.ImmutableBitmap;
import io.druid.data.ValueType;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.column.LuceneIndex;
import io.druid.segment.column.Lucenes;
import io.druid.segment.data.BitmapSerdeFactory;
import io.druid.segment.data.IOPeon;
import io.druid.segment.data.RoaringBitmapSerdeFactory;
import io.druid.segment.data.TmpFileIOPeon;
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
  }

  private final IOPeon ioPeon = new TmpFileIOPeon();

  @Test
  public void testLucene() throws Exception
  {
    ComplexColumnSerializer serializer = ComplexColumnSerializer.create(
        ioPeon,
        "test-lucene",
        StringMetricSerde.INSTANCE,
        "standard"
    );
    serializer.open();

    serializer.serialize("navis manse");
    serializer.serialize("");
    serializer.serialize("navis banzai");
    serializer.serialize(null);
    serializer.serialize("나비스 만세");

    serializer.close();

    long length = serializer.getSerializedSize();

    final ByteArrayOutputStream bout = new ByteArrayOutputStream();
    serializer.writeToChannel(
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
    ColumnPartSerde serde = new StringGenericColumnPartSerde();
    ColumnPartSerde.Deserializer deserializer = serde.getDeserializer();
    BitmapSerdeFactory serdeFactory = new RoaringBitmapSerdeFactory();

    ColumnBuilder builder = new ColumnBuilder();
    builder.setType(ValueType.STRING);
    ByteBuffer payload = ByteBuffer.wrap(bout.toByteArray());
    Assert.assertEquals(length, payload.remaining());

    deserializer.read(payload, builder, serdeFactory);

    Column column = builder.build();
    LuceneIndex luceneIndex = column.getLuceneIndex();

    Assert.assertNotNull(luceneIndex);

    QueryParser parser = new QueryParser("test-lucene", Lucenes.createAnalyzer("standard"));
    Query query = parser.parse("\"navis\"");
    ImmutableBitmap bitmap = luceneIndex.filterFor(query);

    Assert.assertEquals(2, bitmap.size());
    Assert.assertEquals(true, bitmap.get(0));
    Assert.assertEquals(false, bitmap.get(1));
    Assert.assertEquals(true, bitmap.get(2));
    Assert.assertEquals(false, bitmap.get(3));
    Assert.assertEquals(false, bitmap.get(4));
  }
}