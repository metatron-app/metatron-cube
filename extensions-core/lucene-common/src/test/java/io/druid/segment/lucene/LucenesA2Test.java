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

package io.druid.segment.lucene;

import io.druid.java.util.common.StringUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RangeFetchIndexInput;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A2 (hot-first layout): {@code writeTo} places the small metadata / term-index files first + contiguous so the
 * read-side open coalesces them into a single GET. Verifies the ordering logic and that the reordered layout still
 * round-trips through {@code rangeReader} with identical search results.
 */
public class LucenesA2Test
{
  private static final int THRESHOLD = 4 * 1024 * 1024;   // == Lucenes.WHOLE_FILE_THRESHOLD

  @Test
  public void testHotFirstOrder() throws Exception
  {
    // interleaved by name so listAll() (sorted) alternates small/big — exactly the layout A2 must fix
    final Directory dir = new ByteBuffersDirectory();
    writeFile(dir, "a_small", 1_000);
    writeFile(dir, "b_big", 5 << 20);
    writeFile(dir, "c_small", 2_000);
    writeFile(dir, "d_big", 5 << 20);
    writeFile(dir, "e_small", 500);

    // sanity: listAll is interleaved
    Assert.assertArrayEquals(
        new String[]{"a_small", "b_big", "c_small", "d_big", "e_small"}, dir.listAll()
    );

    // hot-first: all small files (in original order) THEN all big files (in original order)
    Assert.assertArrayEquals(
        new String[]{"a_small", "c_small", "e_small", "b_big", "d_big"},
        Lucenes.hotFirstOrder(dir, dir.listAll())
    );
  }

  @Test
  public void testWriteToReorderRoundTripAndLayout() throws Exception
  {
    final Directory dir = new ByteBuffersDirectory();
    final IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
    config.setUseCompoundFile(false);              // discrete files, like buildRamWriter
    config.setMergePolicy(NoMergePolicy.INSTANCE);
    final IndexWriter writer = new IndexWriter(dir, config);
    final int docs = 2000;
    for (int i = 0; i < docs; i++) {
      final Document doc = new Document();
      doc.add(new TextField("text", "navis doc" + i, Field.Store.NO));   // small indexed field: all match "navis"
      doc.add(new StoredField("blob", incompressible(i)));               // ~4KB incompressible each -> .fdt > 4MB
      writer.addDocument(doc);
    }
    writer.commit();

    // ground truth from the raw (pre-serialization) index
    final int expected;
    try (DirectoryReader raw = DirectoryReader.open(dir)) {
      expected = new IndexSearcher(raw).count(new TermQuery(new Term("text", "navis")));
    }
    Assert.assertEquals(docs, expected);

    // serialize with the A2 hot-first writeTo
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (WritableByteChannel channel = Channels.newChannel(baos)) {
      Lucenes.writeTo(writer, channel);
    }
    writer.close();
    final ByteBuffer payload = ByteBuffer.wrap(baos.toByteArray());

    // the real segment must be hot-first: every small file ends before any big file begins (contiguous small prefix)
    final Map<String, int[]> table = parseTable(payload.duplicate());
    int maxSmallEnd = 0;
    int minBigStart = Integer.MAX_VALUE;
    int bigCount = 0;
    for (int[] off : table.values()) {
      if (off[1] <= THRESHOLD) {
        maxSmallEnd = Math.max(maxSmallEnd, off[0] + off[1]);
      } else {
        bigCount++;
        minBigStart = Math.min(minBigStart, off[0]);
      }
    }
    Assert.assertTrue("expected an interleaving big file (.fdt) to make the test meaningful", bigCount >= 1);
    Assert.assertTrue("small files must precede all big files", maxSmallEnd <= minBigStart);

    // and the reordered layout must still read correctly through the range reader
    final ByteBuffer head = payload.duplicate();
    head.getInt();                                        // length prefix (column byte 0)
    final Lucenes.RangeTable rangeTable = Lucenes.parseRangeTable(head);
    final RangeFetchIndexInput.RangeSource source = (o, len) -> {
      final ByteBuffer s = payload.duplicate();
      s.position((int) o).limit((int) o + len);
      return s.slice();
    };
    try (DirectoryReader ranged = Lucenes.rangeReader(rangeTable, source)) {
      final int actual = new IndexSearcher(ranged).count(new TermQuery(new Term("text", "navis")));
      Assert.assertEquals(expected, actual);
    }
  }

  private static void writeFile(Directory dir, String name, int bytes) throws Exception
  {
    try (IndexOutput out = dir.createOutput(name, IOContext.DEFAULT)) {
      out.writeBytes(new byte[bytes], bytes);
    }
  }

  // deterministic high-entropy printable ASCII (LZ4/deflate resistant) so the stored-field file stays > 4MB
  private static String incompressible(int seed)
  {
    final StringBuilder sb = new StringBuilder(4096);
    long x = seed * 2654435761L + 1;
    for (int i = 0; i < 4096; i++) {
      x = x * 6364136223846793005L + 1442695040888963407L;
      sb.append((char) ('!' + (int) ((x >>> 40) % 94)));
    }
    return sb.toString();
  }

  // mirrors Lucenes.parseRangeTable but returns the raw name -> {offset, length} map for layout assertions
  private static Map<String, int[]> parseTable(ByteBuffer payload)
  {
    payload.getInt();                                    // length prefix
    final int fileNum = payload.getInt();
    final Map<String, int[]> offsets = new LinkedHashMap<>();
    for (int i = 0; i < fileNum; i++) {
      final String name = StringUtils.fromUtf8(payload, payload.getInt());
      offsets.put(name, new int[]{payload.getInt(), payload.getInt()});
    }
    return offsets;
  }
}
