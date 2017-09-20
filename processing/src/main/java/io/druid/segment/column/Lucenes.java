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

package io.druid.segment.column;

import com.google.common.base.Throwables;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;
import com.metamx.common.StringUtils;
import com.metamx.common.logger.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.ar.ArabicAnalyzer;
import org.apache.lucene.analysis.bg.BulgarianAnalyzer;
import org.apache.lucene.analysis.br.BrazilianAnalyzer;
import org.apache.lucene.analysis.ca.CatalanAnalyzer;
import org.apache.lucene.analysis.cjk.CJKAnalyzer;
import org.apache.lucene.analysis.ckb.SoraniAnalyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.core.UnicodeWhitespaceAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.cz.CzechAnalyzer;
import org.apache.lucene.analysis.da.DanishAnalyzer;
import org.apache.lucene.analysis.de.GermanAnalyzer;
import org.apache.lucene.analysis.el.GreekAnalyzer;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.es.SpanishAnalyzer;
import org.apache.lucene.analysis.eu.BasqueAnalyzer;
import org.apache.lucene.analysis.fa.PersianAnalyzer;
import org.apache.lucene.analysis.fi.FinnishAnalyzer;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.ga.IrishAnalyzer;
import org.apache.lucene.analysis.gl.GalicianAnalyzer;
import org.apache.lucene.analysis.hi.HindiAnalyzer;
import org.apache.lucene.analysis.hu.HungarianAnalyzer;
import org.apache.lucene.analysis.hy.ArmenianAnalyzer;
import org.apache.lucene.analysis.id.IndonesianAnalyzer;
import org.apache.lucene.analysis.it.ItalianAnalyzer;
import org.apache.lucene.analysis.lt.LithuanianAnalyzer;
import org.apache.lucene.analysis.lv.LatvianAnalyzer;
import org.apache.lucene.analysis.nl.DutchAnalyzer;
import org.apache.lucene.analysis.no.NorwegianAnalyzer;
import org.apache.lucene.analysis.pt.PortugueseAnalyzer;
import org.apache.lucene.analysis.ro.RomanianAnalyzer;
import org.apache.lucene.analysis.ru.RussianAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.UAX29URLEmailAnalyzer;
import org.apache.lucene.analysis.sv.SwedishAnalyzer;
import org.apache.lucene.analysis.th.ThaiAnalyzer;
import org.apache.lucene.analysis.tr.TurkishAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoDeletionPolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.NoMergeScheduler;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 */
public class Lucenes
{
  private static final Logger LOGGER = new Logger(Lucenes.class);
  private static final int IO_BUFFER = 65536;

  public static IndexWriter buildRamWriter(String analyzer)
  {
    IndexWriterConfig writerConfig = new IndexWriterConfig(Lucenes.createAnalyzer(analyzer));
    writerConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
    writerConfig.setRAMBufferSizeMB(256);
    writerConfig.setUseCompoundFile(false);
    writerConfig.setCommitOnClose(true);
    writerConfig.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);
    writerConfig.setMergePolicy(NoMergePolicy.INSTANCE);
    writerConfig.setMergeScheduler(NoMergeScheduler.INSTANCE);
    try {
      return new IndexWriter(new RAMDirectory(), writerConfig);
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public static byte[] serializeAndClose(IndexWriter writer) throws IOException
  {
    ByteArrayDataOutput bout = ByteStreams.newDataOutput();
    Directory directory = writer.getDirectory();
    String[] files = directory.listAll();
    bout.writeInt(files.length);

    byte[] buffer = new byte[IO_BUFFER];
    for (String file : files) {
      byte[] bytes = StringUtils.toUtf8(file);
      bout.writeInt(bytes.length);
      bout.write(bytes);
      final int length = Ints.checkedCast(directory.fileLength(file));
      bout.writeInt(length);
      IndexInput input = directory.openInput(file, null);
      int offset = 0;
      while (offset < length) {
        int toRead = Math.min((length - offset), buffer.length);
        input.readBytes(buffer, 0, toRead);
        bout.write(buffer, 0, toRead);
        offset += toRead;
      }
      LOGGER.debug("-----------------> %s, %,d", file, length);
    }
    writer.close();
    directory.close();
    return bout.toByteArray();
  }

  public static DirectoryReader deserializeWithRuntimeException(ByteBuffer bufferToUse)
  {
    try {
      return deserialize(bufferToUse);
    }
    catch (Throwable e) {
      throw Throwables.propagate(e);
    }
  }

  public static DirectoryReader deserialize(ByteBuffer bufferToUse) throws IOException
  {
    byte[] temp = new byte[IO_BUFFER];
    RAMDirectory directory = new RAMDirectory();
    int fileNum = bufferToUse.getInt();
    for (int i = 0; i < fileNum; i++) {
      final String fileName = io.druid.common.utils.StringUtils.fromUtf8(bufferToUse, bufferToUse.getInt());
      final int length = bufferToUse.getInt();
      LOGGER.debug("-----------------> %s, %,d", fileName, length);
      IndexOutput output = directory.createOutput(fileName, null);
      int offset = 0;
      while (offset < length) {
        int toRead = Math.min((length - offset), temp.length);
        bufferToUse.get(temp, 0, toRead);
        output.writeBytes(temp, toRead);
        offset += toRead;
      }
      output.close();
    }
    return DirectoryReader.open(directory);
  }

  // gt
  public static Query greaterThan(String column, Comparable constant)
  {
    return new TermRangeQuery(column, new BytesRef((String) constant), null, false, false);
  }

  // gte
  public static Query atLeast(String column, Comparable constant)
  {
    return new TermRangeQuery(column, new BytesRef((String) constant), null, true, false);
  }

  // lt
  public static Query lessThan(String column, Comparable constant)
  {
    return new TermRangeQuery(column, null, new BytesRef((String) constant), false, false);
  }

  // lte
  public static Query atMost(String column, Comparable constant)
  {
    return new TermRangeQuery(column, null, new BytesRef((String) constant), false, true);
  }

  public static Query point(String column, Comparable constant)
  {
    return new TermQuery(new Term(column, (String) constant));
  }

  public static Query closed(String column, Comparable value1, Comparable value2)
  {
    return new TermRangeQuery(column, new BytesRef((String) value1), new BytesRef((String) value2), true, true);
  }

  public static Analyzer createAnalyzer(String analyzer)
  {
    switch (analyzer.toLowerCase()) {
      case "simple": return new SimpleAnalyzer();
      case "standard": return new StandardAnalyzer();
      case "english": return new EnglishAnalyzer();
      case "cjk": return new CJKAnalyzer();
      case "whitespace": return new WhitespaceAnalyzer();
      case "unicode_whitespace": return new UnicodeWhitespaceAnalyzer();
      case "keyword": return new KeywordAnalyzer();
      case "dutch": return new DutchAnalyzer();
      case "hungarian": return new HungarianAnalyzer();
      case "bulgarian": return new BulgarianAnalyzer();
      case "czech": return new CzechAnalyzer();
      case "irish": return new IrishAnalyzer();
      case "brazilian": return new BrazilianAnalyzer();
      case "danish": return new DanishAnalyzer();
      case "turkish": return new TurkishAnalyzer();
      case "russian": return new RussianAnalyzer();
      case "indonesian": return new IndonesianAnalyzer();
      case "basque": return new BasqueAnalyzer();
      case "sorani": return new SoraniAnalyzer();
      case "german": return new GermanAnalyzer();
      case "arabic": return new ArabicAnalyzer();
      case "greek": return new GreekAnalyzer();
      case "swedish": return new SwedishAnalyzer();
      case "french": return new FrenchAnalyzer();
      case "catalan": return new CatalanAnalyzer();
      case "norwegian": return new NorwegianAnalyzer();
      case "lithuanian": return new LithuanianAnalyzer();
      case "persian": return new PersianAnalyzer();
      case "hindi": return new HindiAnalyzer();
      case "galician": return new GalicianAnalyzer();
      case "portuguese": return new PortugueseAnalyzer();
      case "thai": return new ThaiAnalyzer();
      case "spanish": return new SpanishAnalyzer();
      case "latvian": return new LatvianAnalyzer();
      case "armenian": return new ArmenianAnalyzer();
      case "romanian": return new RomanianAnalyzer();
      case "italian": return new ItalianAnalyzer();
      case "finnish": return new FinnishAnalyzer();
      case "uax29_url_email": return new UAX29URLEmailAnalyzer();
      // etc..
    }
    try {
      return (Analyzer) Class.forName(analyzer).newInstance();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
