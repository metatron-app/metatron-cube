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

import com.google.common.base.Strings;
import com.google.common.primitives.Ints;
import io.druid.segment.GenericColumnSerializer;
import io.druid.segment.column.Lucenes;
import io.druid.segment.data.GenericIndexedWriter;
import io.druid.segment.data.IOPeon;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Map;

public class ComplexColumnSerializer implements GenericColumnSerializer, ColumnPartSerde.Serializer
{
  public static ComplexColumnSerializer create(
      IOPeon ioPeon,
      String filenameBase,
      ComplexMetricSerde serde,
      String luceneAnalyzer
  )
  {
    return new ComplexColumnSerializer(ioPeon, filenameBase, serde, luceneAnalyzer);
  }

  private final IOPeon ioPeon;
  private final String filenameBase;
  private final ComplexMetricSerde serde;

  private final IndexWriter luceneIndexer;
  private ByteBuffer lucenePayload;

  private GenericIndexedWriter writer;

  public ComplexColumnSerializer(
      IOPeon ioPeon,
      String filenameBase,
      ComplexMetricSerde serde,
      String luceneAnalyzer
  )
  {
    this.ioPeon = ioPeon;
    this.filenameBase = filenameBase;
    this.serde = serde;
    this.luceneIndexer =
        luceneAnalyzer != null && serde == StringMetricSerde.INSTANCE ? Lucenes.buildRamWriter(luceneAnalyzer) : null;
  }

  @SuppressWarnings(value = "unchecked")
  @Override
  public void open() throws IOException
  {
    writer = new GenericIndexedWriter(
        ioPeon, String.format("%s.complex_column", filenameBase), serde.getObjectStrategy()
    );
    writer.open();
  }

  @SuppressWarnings(value = "unchecked")
  @Override
  public void serialize(Object obj) throws IOException
  {
    writer.write(obj);
    if (luceneIndexer != null) {
      Document doc = new Document();
      doc.add(new TextField(filenameBase, Strings.nullToEmpty((String) obj), Field.Store.NO));
      luceneIndexer.addDocument(doc);
    }
  }

  @Override
  public void close() throws IOException
  {
    writer.close();
    if (luceneIndexer != null) {
      luceneIndexer.commit();
    }
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    long size = writer.getSerializedSize();
    if (luceneIndexer != null && luceneIndexer.numDocs() > 0) {
      lucenePayload = ByteBuffer.wrap(Lucenes.serializeAndClose(luceneIndexer));
      size += Ints.BYTES + lucenePayload.remaining();
    }
    return size;
  }

  @Override
  public void writeToChannel(WritableByteChannel channel) throws IOException
  {
    writer.writeToChannel(channel);
    if (lucenePayload != null && lucenePayload.hasRemaining()) {
      channel.write(ByteBuffer.wrap(Ints.toByteArray(lucenePayload.remaining())));
      channel.write(lucenePayload);
    }
  }

  @Override
  public Map<String, Object> getSerializeStats()
  {
    return null;
  }
}
