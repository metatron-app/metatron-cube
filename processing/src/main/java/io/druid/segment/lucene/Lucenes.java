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

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import io.druid.common.utils.StringUtils;
import io.druid.data.Pair;
import io.druid.data.ValueDesc;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.filter.BitmapIndexSelector;
import io.druid.segment.column.Column;
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
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoDeletionPolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.NoMergeScheduler;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LuceneIndexInput;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.apache.lucene.util.BytesRef;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

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

  public static Function<Object, Field[]> makeTextFieldGenerator(final String fieldName)
  {
    return new Function<Object, Field[]>()
    {
      @Override
      public Field[] apply(Object input)
      {
        // to string whatever..
        return new Field[]{new TextField(fieldName, Objects.toString(input, ""), Field.Store.NO)};
      }
    };
  }

  public static Function<LuceneIndexingStrategy, Function<Object, Field[]>> makeGenerator(final ValueDesc type)
  {
    return new Function<LuceneIndexingStrategy, Function<Object, Field[]>>()
    {
      @Override
      public Function<Object, Field[]> apply(LuceneIndexingStrategy input)
      {
        return input.createIndexableField(type);
      }
    };
  }

  public static int sizeOf(IndexWriter writer) throws IOException
  {
    Directory directory = writer.getDirectory();
    String[] files = directory.listAll();
    int length = Integer.BYTES + Integer.BYTES; // total size + number of files
    for (String file : files) {
      length += Integer.BYTES;  // length of file-name
      length += StringUtils.estimatedBinaryLengthAsUTF8(file);  // file-name
      length += Integer.BYTES + Integer.BYTES;  // offset + length
      length += directory.fileLength(file);
    }
    return length;
  }

  @SuppressWarnings("unchecked")
  public static void writeTo(IndexWriter writer, WritableByteChannel channel) throws IOException
  {
    Directory directory = writer.getDirectory();
    String[] files = directory.listAll();

    int headerOffset = Integer.BYTES;  // number of files
    int dataOffset = 0;
    Pair<byte[], int[]>[] dataOffsets = new Pair[files.length];
    for (int i = 0; i < files.length; i++) {
      byte[] binary = StringUtils.toUtf8WithNullToEmpty(files[i]);
      int dataLength = Ints.checkedCast(directory.fileLength(files[i]));
      dataOffsets[i] = Pair.of(binary, new int[]{dataOffset, dataLength});
      headerOffset += Integer.BYTES + binary.length + Integer.BYTES + Integer.BYTES;
      dataOffset += dataLength;
    }
    DataOutputStream output = new DataOutputStream(Channels.newOutputStream(channel));
    output.writeInt(headerOffset + dataOffset);

    output.writeInt(files.length);
    for (Map.Entry<byte[], int[]> entry : dataOffsets) {
      final byte[] key = entry.getKey();
      final int[] value = entry.getValue();
      output.writeInt(key.length);
      output.write(key);
      output.writeInt(value[0]);
      output.writeInt(value[1]);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("-----------------> %s, %,d, %,d", new String(key), value[0], value[1]);
      }
    }
    final byte[] buffer = new byte[IO_BUFFER];
    for (int i = 0; i < files.length; i++) {
      IndexInput input = directory.openInput(files[i], null);
      int offset = 0;
      int length = dataOffsets[i].rhs[1];
      while (offset < length) {
        int toRead = Math.min(length - offset, buffer.length);
        input.readBytes(buffer, 0, toRead);
        output.write(buffer, 0, toRead);
        offset += toRead;
      }
    }
    output.flush();
    output.close();
  }

  @SuppressWarnings("unchecked")
  public static DirectoryReader readFrom(final ByteBuffer buffer)
  {
    int fileNum = buffer.getInt();
    final Map<String, int[]> dataOffsets = Maps.newLinkedHashMap();
    for (int i = 0; i < fileNum; i++) {
      String fileName = StringUtils.fromUtf8(buffer, buffer.getInt());
      int[] offsetLength = {buffer.getInt(), buffer.getInt()};
      dataOffsets.put(fileName, offsetLength);
      LOGGER.debug("-----------------> %s, %,d, %,d", fileName, offsetLength[0], offsetLength[1]);
    }
    final ByteBuffer datum = buffer.slice();
    final BaseDirectory directory = new BaseDirectory(new SingleInstanceLockFactory())
    {
      @Override
      public String[] listAll() throws IOException
      {
        return dataOffsets.keySet().toArray(new String[0]);
      }

      @Override
      public void deleteFile(String name) throws IOException
      {
        throw new UnsupportedOperationException("deleteFile");
      }

      @Override
      public long fileLength(String name) throws IOException
      {
        int[] offsetLength = dataOffsets.get(name);
        if (offsetLength == null) {
          throw new FileNotFoundException(name);
        }
        return offsetLength[1];
      }

      @Override
      public IndexOutput createOutput(String name, IOContext context) throws IOException
      {
        throw new UnsupportedOperationException("createOutput");
      }

      @Override
      public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException
      {
        throw new UnsupportedOperationException("createTempOutput");
      }

      @Override
      public void sync(Collection<String> names) throws IOException
      {
        throw new UnsupportedOperationException("sync");
      }

      @Override
      public void rename(String source, String dest) throws IOException
      {
        throw new UnsupportedOperationException("rename");
      }

      @Override
      public void syncMetaData() throws IOException
      {
        throw new UnsupportedOperationException("syncMetaData");
      }

      @Override
      public IndexInput openInput(String name, IOContext context) throws IOException
      {
        final int[] offsets = dataOffsets.get(name);
        if (offsets == null) {
          throw new FileNotFoundException(name);
        }
        LOGGER.debug("-------> %s : %d~%d(%d)", name, offsets[0], offsets[0] + offsets[1], offsets[1]);
        datum.limit(offsets[0] + offsets[1]).position(offsets[0]);
        return LuceneIndexInput.newInstance("LuceneIndex(name=" + name + ")", datum.slice(), offsets[1]);
      }

      @Override
      public void close() throws IOException
      {
      }
    };
    try {
      return DirectoryReader.open(directory);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Deprecated
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
      final String fileName = StringUtils.fromUtf8(bufferToUse, bufferToUse.getInt());
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

  public static ImmutableBitmap toBitmap(BitmapFactory factory, TopDocs searched)
  {
    if (searched.totalHits == 0) {
      return factory.makeEmptyImmutableBitmap();
    }
    MutableBitmap bitmap = factory.makeEmptyMutableBitmap();
    for (ScoreDoc scoreDoc : searched.scoreDocs) {
      bitmap.add(scoreDoc.doc);   // can be slow
    }
    return factory.makeImmutableBitmap(bitmap);
  }

  public static Column findLuceneColumn(String field, BitmapIndexSelector selector)
  {
    Column column = selector.getColumn(field);
    if (column != null && column.getCapabilities().hasLuceneIndex()) {
      return column;
    }
    for (int index = field.lastIndexOf('.', field.length()); index > 0; index = field.lastIndexOf('.', index - 1)) {
      column = selector.getColumn(field.substring(0, index));
      if (column != null && column.getCapabilities().hasLuceneIndex()) {
        return column;
      }
    }
    return null;
  }

  public static String findLuceneField(String field, Column column, String expected)
  {
    final String columnName = column.getName();
    final Map<String, String> columnDesc = column.getColumnDescs();
    if (!field.equals(columnName) && columnDesc.containsKey(field.substring(columnName.length() + 1))) {
      return field.substring(columnName.length() + 1);
    }
    for (Map.Entry<String, String> desc : columnDesc.entrySet()) {
      if (desc.getValue().startsWith(expected)) {
        return desc.getKey();
      }
    }
    return null;
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
