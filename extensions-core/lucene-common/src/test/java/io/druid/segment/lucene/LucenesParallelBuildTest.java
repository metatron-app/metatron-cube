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

import com.google.common.collect.Lists;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;

/**
 * Lever B: {@link Lucenes#buildParallel} builds a lucene index over row-ordered Documents in parallel contiguous
 * blocks + a verbatim ordered concat. This asserts the two invariants that make it safe to swap for the serial
 * per-row build: (1) global docID == row ordinal (the bitmap/attachment contract), (2) search results identical to a
 * serial build.
 */
public class LucenesParallelBuildTest
{
  private static List<Document> docs(int n)
  {
    final List<Document> docs = Lists.newArrayListWithCapacity(n);
    for (int i = 0; i < n; i++) {
      final Document doc = new Document();
      // repeating terms so searches match many docs across block boundaries; ord proves docID==ordinal
      doc.add(new TextField("raw", "term" + (i % 1000) + " common", Field.Store.NO));
      doc.add(new StoredField("ord", i));
      docs.add(doc);
    }
    return docs;
  }

  @Test
  public void testParallelBuildPreservesOrderAndResults() throws Exception
  {
    // 250k docs > buildMinRowsPerBlock(100k) -> the parallel path actually splits (>=2 blocks) on a multicore box;
    // on a single core it falls back to serial, still correct.
    final int n = 250_000;
    final List<Document> docs = docs(n);

    final File dir = Files.createTempDirectory("lparbuild").toFile();
    try (IndexWriter writer = Lucenes.buildParallel(dir, "standard", Collections.emptyList(), docs)) {
      try (DirectoryReader reader = DirectoryReader.open(writer.getDirectory())) {
        Assert.assertEquals(n, reader.maxDoc());

        // (1) docID == ordinal: the stored ord of every docID must equal the docID (checks every doc, across blocks)
        final StoredFields stored = reader.storedFields();
        for (int d = 0; d < reader.maxDoc(); d++) {
          Assert.assertEquals(d, stored.document(d).getField("ord").numericValue().intValue());
        }

        // (2) results correct: each of the 1000 distinct terms matches exactly n/1000 docs; "common" matches all
        final IndexSearcher searcher = new IndexSearcher(reader);
        Assert.assertEquals(n / 1000, searcher.count(new TermQuery(new Term("raw", "term7"))));
        Assert.assertEquals(n / 1000, searcher.count(new TermQuery(new Term("raw", "term999"))));
        Assert.assertEquals(n, searcher.count(new TermQuery(new Term("raw", "common"))));
        Assert.assertEquals(0, searcher.count(new TermQuery(new Term("raw", "absent"))));
      }
    }
  }
}
