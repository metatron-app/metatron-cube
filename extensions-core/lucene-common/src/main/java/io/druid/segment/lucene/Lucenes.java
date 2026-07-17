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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import io.druid.java.util.common.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;
import com.metamx.collections.bitmap.BitmapFactory;
import com.metamx.collections.bitmap.ImmutableBitmap;
import com.metamx.collections.bitmap.MutableBitmap;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.StringUtils;
import io.druid.data.Pair;
import io.druid.data.input.BytesOutputStream;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.GeomUtils;
import io.druid.query.ShapeFormat;
import io.druid.segment.QueryableIndex;
import io.druid.segment.column.Column;
import io.druid.segment.filter.FilterContext;
import it.unimi.dsi.fastutil.ints.Int2FloatRBTreeMap;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.cjk.CJKBigramFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.icu.ICUFoldingFilter;
import org.apache.lucene.analysis.icu.segmentation.ICUTokenizer;
import org.apache.lucene.analysis.miscellaneous.LengthFilter;
import org.apache.lucene.analysis.ngram.NGramTokenFilter;
import org.apache.lucene.analysis.pattern.PatternTokenizer;
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
import org.apache.lucene.analysis.sv.SwedishAnalyzer;
import org.apache.lucene.analysis.th.ThaiAnalyzer;
import org.apache.lucene.analysis.tr.TurkishAnalyzer;
import org.apache.lucene.sandbox.document.BigIntegerPoint;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoDeletionPolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.NoMergeScheduler;
import org.apache.lucene.index.SlowCodecReaderWrapper;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.flexible.standard.config.PointsConfig;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.BaseDirectory;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LuceneIndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.store.RangeFetchIndexInput;
import org.apache.lucene.store.SingleInstanceLockFactory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOConsumer;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.shape.Shape;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 */
public class Lucenes
{
  private static final Logger LOGGER = new Logger(Lucenes.class);
  private static final int IO_BUFFER = 65536;

  public static IndexWriter buildRamWriter(File file, String analyzer, List<LuceneIndexingStrategy> strategies)
  {
    IndexWriterConfig config = new IndexWriterConfig(Lucenes.createAnalyzer(analyzer));
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
    config.setRAMBufferSizeMB(256);
    config.setUseCompoundFile(false);
    config.setCommitOnClose(true);
    config.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);
    config.setMergePolicy(NoMergePolicy.INSTANCE);
    config.setMergeScheduler(NoMergeScheduler.INSTANCE);
    for (LuceneIndexingStrategy strategy : strategies) {
      config = strategy.configure(config);
    }
    try {
      return new IndexWriter(new MMapDirectory(file.toPath()), config);
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  // Physically merge existing lucene indexes (no re-analysis): addIndexes the source readers in order,
  // then forceMerge(1) into a single segment. Preserves document order (no index sort / no deletions), so
  // docID stays == the merged row ordinal for a concat/time-disjoint merge. Required for index-only columns
  // whose original text is gone and therefore cannot be re-indexed. `sources` MUST be in merged-row order.
  public static IndexWriter mergeTo(File file, List<DirectoryReader> sources) throws IOException
  {
    final IndexWriter writer = buildMergeWriter(file);
    final List<CodecReader> readers = Lists.newArrayList();
    for (DirectoryReader source : sources) {
      for (LeafReaderContext leaf : source.leaves()) {
        readers.add(SlowCodecReaderWrapper.wrap(leaf.reader()));
      }
    }
    writer.addIndexes(readers.toArray(new CodecReader[0]));
    writer.forceMerge(1);   // one segment: dedups term dicts, keeps doc order
    writer.commit();
    return writer;
  }

  // Like buildRamWriter but with the DEFAULT merge policy so forceMerge(1) actually merges (buildRamWriter
  // uses NoMergePolicy). No index sort, no deletions -> merged docIDs stay in addIndexes order.
  private static IndexWriter buildMergeWriter(File file)
  {
    IndexWriterConfig config = new IndexWriterConfig();
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    config.setRAMBufferSizeMB(256);
    config.setUseCompoundFile(false);
    config.setCommitOnClose(true);
    try {
      return new IndexWriter(new MMapDirectory(file.toPath()), config);
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public static LuceneFieldGenerator makeTextFieldGenerator(final String fieldName)
  {
    // to string whatever..
    return input -> new Field[]{new TextField(fieldName, Objects.toString(input, ""), Field.Store.NO)};
  }

  public static LuceneFieldGenerator makeJsonFieldGenerator(
      ObjectMapper mapper, String fieldName, List<String> indexing
  )
  {
    // to string whatever..
    return input -> generate(mapper, Objects.toString(input, ""), indexing);
  }

  private static Field[] generate(final ObjectMapper mapper, final String input, List<String> indexing)
  {
    if (StringUtils.isNullOrEmpty(input)) {
      return new Field[]{};
    }
    List<Field> list = Lists.newArrayList();
    try {
      addTo(mapper.readTree(input), "", list, includer(indexing));
    }
    catch (Throwable t) {
      throw Throwables.propagate(t);
    }
    return list.toArray(new Field[0]);
  }

  private static void addTo(JsonNode node, String prefix, List<Field> list, Predicate<String> includer)
  {
    Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> entry = fields.next();
      String name = entry.getKey();
      JsonNode value = entry.getValue();
      if (value.isNull() || (value.isValueNode() && !includer.apply(prefix + name))) {
        continue;
      }
      if (value.isTextual()) {
        list.add(new StringField(prefix + name, value.textValue(), Field.Store.NO));
      } else if (value.isBoolean()) {
        list.add(new StringField(prefix + name, value.booleanValue() ? "true" : "false", Field.Store.NO));
      } else if (value.isShort() || value.isInt()) {
        list.add(new IntPoint(prefix + name, value.intValue()));
      } else if (value.isLong()) {
        list.add(new LongPoint(prefix + name, value.longValue()));
      } else if (value.isBigInteger()) {
        list.add(new BigIntegerPoint(prefix + name, value.bigIntegerValue()));
      } else if (value.isFloat()) {
        list.add(new FloatPoint(prefix + name, value.floatValue()));
      } else if (value.isDouble()) {
        list.add(new DoublePoint(prefix + name, value.doubleValue()));
      } else if (value.isObject()) {
        addTo(value, prefix.isEmpty() ? name + "." : prefix + "." + name + ".", list, includer);
      } else if (value.isArray()) {
        // todo: no idea
      }
    }
  }

  private static Predicate<String> includer(Collection<String> includes)
  {
    if (GuavaUtils.isNullOrEmpty(includes)) {
      return Predicates.alwaysTrue();
    }
    final List<Matcher> machers = GuavaUtils.transform(includes, p -> Pattern.compile(p).matcher(""));
    return key -> {
      for (Matcher matcher : machers) {
        if (matcher.reset(key).matches()) {
          return true;
        }
      }
      return false;
    };
  }

  public static Class<?> typeOf(String type)
  {
    if (type == null) {
      return Object.class;
    }
      return switch (type.toUpperCase()) {
          case "STRING" -> String.class;
          case "INT" -> Integer.class;
          case "LONG" -> Long.class;
          case "FLOAT" -> Float.class;
          case "DOUBLE" -> Double.class;
          case "BIGINT" -> BigInteger.class;
          default -> Object.class;
      };
  }

  public static byte[] serialize(IOConsumer<DataOutput> writer) throws IOException
  {
    BytesOutputStream out = new BytesOutputStream();
    OutputStreamDataOutput dout = new OutputStreamDataOutput(out);
    out.writeInt(0);
    writer.accept(dout);
    byte[] contents = out.toByteArray();
    System.arraycopy(Ints.toByteArray(contents.length - Integer.BYTES), 0, contents, 0, Integer.BYTES);
    return contents;
  }

  @SuppressWarnings("unchecked")
  public static Map<String, PointsConfig> asPointConfig(Map<String, String> types)
  {
    if (GuavaUtils.isNullOrEmpty(types)) {
      return ImmutableMap.of();
    }
    Map<String, PointsConfig> pointConfigs = Maps.newHashMap();
    for (Map.Entry<String, String> entry : types.entrySet()) {
      Class<?> clazz = Lucenes.typeOf(entry.getValue());
      if (Number.class.isAssignableFrom(clazz)) {
        pointConfigs.put(entry.getKey(), new PointsConfig(new DecimalFormat(), (Class<? extends Number>) clazz));
      }
    }
    return pointConfigs;
  }

  public static int sizeOf(IndexWriter writer)
  {
    Directory directory = writer.getDirectory();
    try {
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
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * A2 hot-first layout: order the index files so all small (&le; {@link #WHOLE_FILE_THRESHOLD}) metadata / term-index
   * files come FIRST and contiguous, the big term-dict/postings files after — each group preserving {@code listAll()}
   * order. Physical order is transparent to readers (they locate every file by name via the offset table), but it
   * lays the open-time small files (exactly the set {@link #prefetchSmallFiles} pulls) into ONE contiguous span, so
   * the read-side coalescing fetches them in a SINGLE GET per segment instead of one per interleaved run. Same
   * {@code WHOLE_FILE_THRESHOLD} on both sides (same class) keeps the write partition and read partition identical.
   * Backward/forward compatible — old readers still work (name lookup), and the benefit is realized on rewrite only.
   */
  static String[] hotFirstOrder(final Directory directory, final String[] files) throws IOException   // package-private for test
  {
    final List<String> hot = Lists.newArrayListWithCapacity(files.length);
    final List<String> cold = Lists.newArrayList();
    for (String file : files) {
      (directory.fileLength(file) <= WHOLE_FILE_THRESHOLD ? hot : cold).add(file);
    }
    hot.addAll(cold);
    return hot.toArray(new String[0]);
  }

  @SuppressWarnings("unchecked")
  public static long writeTo(IndexWriter writer, WritableByteChannel channel) throws IOException
  {
    Directory directory = writer.getDirectory();
    // A2: small files first + contiguous so the read-side open (prefetchSmallFiles) coalesces them into one GET.
    String[] files = hotFirstOrder(directory, directory.listAll());

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
      IndexInput input = directory.openInput(files[i], IOContext.DEFAULT);
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
    return sizeOf(writer);
  }

  // Row count of a serialized lucene column: every row contributes exactly one document (see the
  // per-row addDocument in LuceneIndexingSpec), so maxDoc == segment numRows. Lets a lucene column
  // report its own row count without a base value column (index-only).
  public static int maxDoc(final ByteBuffer buffer)
  {
    try (DirectoryReader reader = readFrom(buffer)) {
      return reader.maxDoc();
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public static DirectoryReader readFrom(final ByteBuffer buffer)
  {
    final int fileNum = buffer.getInt();
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
      public Set<String> getPendingDeletions()
      {
        return Collections.emptySet();
      }

      @Override
      public void close() throws IOException
      {
      }
    };
    try {
      final long _t0 = System.nanoTime();
      final DirectoryReader reader = DirectoryReader.open(directory);
      io.druid.java.util.common.RangeProf.openNanos.addAndGet(System.nanoTime() - _t0);
      io.druid.java.util.common.RangeProf.openCount.incrementAndGet();
      return reader;
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  // Read-ahead window for a range-served BIG lucene file (term dict / postings), sub-ranged to pull only the term's
  // block + postings. Amortizes per-GET round-trip latency (seaweed ~5ms/GET dominated the first cut — 94k tiny GETs)
  // while staying tiny vs the whole ~tens-of-MB file. lucene's access here is 100% RANDOM (FST traversal + seeks),
  // which read-ahead can't coalesce, so a bigger window only over-fetches. A 2279-seg cold sweep (RangeProf buffer-fill
  // probe) measured the range-read GET count PERFECTLY INVARIANT across 16/32/64/128K (23,670 every time — reads never
  // span the buffer), while fetched bytes scaled linearly with the window: 64K over-fetched ~2x the bytes of 32K for
  // zero fewer GETs and no measurable latency change. 32K is the knee — halves 64K's over-fetch with margin above the
  // point where reads would start to span. Override with -Ddruid.lucene.rangeBufferSize=<bytes> (no rebuild).
  private static final int RANGE_BUFFER_SIZE = Integer.getInteger("druid.lucene.rangeBufferSize", 32 * 1024);

  // Range-prefetch: forward Lucene's prefetch(fp) hints (term-dict/postings) to an async warm so a cold scan's
  // latency-bound GETs overlap instead of running one at a time on the query's critical path. Off by default; enable
  // with -Ddruid.lucene.rangePrefetch=true. REQUIRES the range disk cache to be on (the coalescing relies on the
  // fetch write-through) — without it, a hint that lands after its read would re-fetch from deep storage.
  private static final boolean RANGE_PREFETCH = Boolean.getBoolean("druid.lucene.rangePrefetch");
  private static final int PREFETCH_THREADS = Integer.getInteger("druid.lucene.rangePrefetchThreads", 16);
  private static final ExecutorService PREFETCH_POOL = RANGE_PREFETCH ? newPrefetchPool() : null;

  private static ExecutorService newPrefetchPool()
  {
    final ThreadFactory tf = new ThreadFactory()
    {
      private final AtomicInteger n = new AtomicInteger();

      @Override
      public Thread newThread(Runnable r)
      {
        final Thread t = new Thread(r, "lucene-range-prefetch-" + n.getAndIncrement());
        t.setDaemon(true);
        return t;
      }
    };
    // Bounded queue + discard: a saturated pool DROPS the hint (the read then does its own GET) rather than block the
    // query thread or let warm work pile up unboundedly behind the reads it's meant to get ahead of.
    return new ThreadPoolExecutor(
        PREFETCH_THREADS, PREFETCH_THREADS, 60L, TimeUnit.SECONDS,
        new ArrayBlockingQueue<>(4096), tf, new ThreadPoolExecutor.DiscardPolicy()
    );
  }

  private record RangeKey(long offset, int length) {}

  /**
   * Wrap a range source so a {@code prefetch()} hint warms its {@code (offset,length)} range in the background and
   * coalesces with the subsequent synchronous {@code fetch}: at most ONE deep-storage GET per range. A caller that
   * finds a range already in flight waits for it, then re-reads the range — a local disk-cache hit, since the fetch
   * write-through completes before the latch opens — so no ByteBuffer is shared across threads. Degrades to at-worst-
   * neutral: if a read claims a range before its prefetch task runs, it fetches synchronously as it would have anyway,
   * and the late prefetch task then serves from the disk cache the read just populated (still no second GET).
   */
  private static RangeFetchIndexInput.RangeSource prefetchingSource(final RangeFetchIndexInput.RangeSource delegate)
  {
    final ConcurrentHashMap<RangeKey, CountDownLatch> inflight = new ConcurrentHashMap<>();
    return new RangeFetchIndexInput.RangeSource()
    {
      @Override
      public ByteBuffer fetch(long offset, int length) throws IOException
      {
        final RangeKey key = new RangeKey(offset, length);
        final CountDownLatch mine = new CountDownLatch(1);
        final CountDownLatch running = inflight.putIfAbsent(key, mine);
        if (running != null) {
          try {
            running.await(10, TimeUnit.SECONDS);   // wait out the in-flight fetch, then read its disk-cached result
          }
          catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          return delegate.fetch(offset, length);
        }
        try {
          return delegate.fetch(offset, length);
        }
        finally {
          inflight.remove(key, mine);
          mine.countDown();
        }
      }

      @Override
      public void prefetch(long offset, int length)
      {
        if (PREFETCH_POOL == null || inflight.containsKey(new RangeKey(offset, length))) {
          return;
        }
        PREFETCH_POOL.execute(() -> {
          try {
            fetch(offset, length);
          }
          catch (Throwable ignored) {
            // best-effort warm; a failure just resurfaces as the real read's own fetch
          }
        });
      }
    };
  }

  // Files at or below this size are fetched WHOLE in a single GET (buffer-backed) instead of sub-ranged: the term
  // index (.tip FST), field infos, segment info, norms etc. are read (near-)fully anyway, so one coarse GET beats
  // many latency-bound small ones. Only the genuinely large term-dict/postings files get the range-read treatment.
  private static final int WHOLE_FILE_THRESHOLD = 4 * 1024 * 1024;

  /**
   * The lucene file-offset table parsed from a range-served column's head — tiny (a name + two ints per index file),
   * retained for the segment's life so each query can reopen a {@link DirectoryReader} without re-fetching the head.
   * {@code datumBase} is the column-relative offset where the concatenated index files begin (after the table).
   */
  public static final class RangeTable
  {
    private final Map<String, int[]> dataOffsets;
    private final long datumBase;

    private RangeTable(Map<String, int[]> dataOffsets, long datumBase)
    {
      this.dataOffsets = dataOffsets;
      this.datumBase = datumBase;
    }
  }

  /**
   * Parse the lucene file-offset table from a range-served column's head buffer. {@code head}'s byte 0 is the
   * enclosing column's byte 0, so its positions ARE column-relative offsets — the post-table datum base is simply
   * {@code head.position()} once the table is consumed. Cheap and one-time; the returned {@link RangeTable} is reused.
   */
  public static RangeTable parseRangeTable(final ByteBuffer head)
  {
    final int fileNum = head.getInt();
    final Map<String, int[]> dataOffsets = Maps.newLinkedHashMap();
    for (int i = 0; i < fileNum; i++) {
      String fileName = StringUtils.fromUtf8(head, head.getInt());
      int[] offsetLength = {head.getInt(), head.getInt()};
      dataOffsets.put(fileName, offsetLength);
    }
    return new RangeTable(dataOffsets, head.position());
  }

  /**
   * A1 open-time GET coalescing. {@code DirectoryReader.open} reads many small (&le; {@link #WHOLE_FILE_THRESHOLD})
   * metadata / term-index files (.si/.fnm/.tip/.tmd/norms/segments_N), each previously its own GET — measured ~32
   * GETs and ~8MB/segment on a cold range scan, ~94% of the whole scan's GETs and latency-bound (per-GET round-trip,
   * not bytes). The files are laid out gaplessly in {@link #writeTo} order, but big term-dict/postings files are
   * interleaved by name, so we prefetch only maximal CONTIGUOUS RUNS of small files — one GET per run, sliced per
   * file — and never pull a big file. The per-file slices are retained for the reader's (segment's) life so re-opens
   * during open/search are free; big files stay lazy ({@link RangeFetchIndexInput}). Best-effort: a run whose fetch
   * fails or short-reads is left out and {@code openInput} falls back to a per-file GET for those names.
   */
  private static Map<String, ByteBuffer> prefetchSmallFiles(
      final Map<String, int[]> dataOffsets,
      final long datumBase,
      final RangeFetchIndexInput.RangeSource source
  )
  {
    final List<Map.Entry<String, int[]>> small = Lists.newArrayList();
    for (Map.Entry<String, int[]> e : dataOffsets.entrySet()) {
      if (e.getValue()[1] <= WHOLE_FILE_THRESHOLD) {
        small.add(e);
      }
    }
    small.sort(Comparator.comparingInt(e -> e.getValue()[0]));   // by data offset: adjacent == contiguous (gapless)
    final Map<String, ByteBuffer> prefetched = Maps.newHashMapWithExpectedSize(small.size());
    int i = 0;
    while (i < small.size()) {
      final int runStart = small.get(i).getValue()[0];
      int runEnd = runStart + small.get(i).getValue()[1];
      int j = i + 1;
      // extend the run while the next small file abuts the current end; a gap means a big file sits between them
      while (j < small.size() && small.get(j).getValue()[0] == runEnd) {
        runEnd += small.get(j).getValue()[1];
        j++;
      }
      final int runLen = runEnd - runStart;
      try {
        final ByteBuffer run = source.fetch(datumBase + runStart, runLen);
        if (run != null && run.remaining() == runLen) {
          final int base = run.position();
          for (int k = i; k < j; k++) {
            final int[] off = small.get(k).getValue();
            final int pos = base + (off[0] - runStart);
            final ByteBuffer slice = run.duplicate();
            slice.limit(pos + off[1]).position(pos);
            prefetched.put(small.get(k).getKey(), slice.slice());
          }
        }
      }
      catch (IOException ignored) {
        // leave this run unfetched; openInput GETs each of its files on demand
      }
      i = j;
    }
    return prefetched;
  }

  /**
   * Range-served twin of {@link #readFrom}: open a {@link DirectoryReader} whose index files are range-read on demand
   * via {@code source} (each file opened as a {@link RangeFetchIndexInput} over its column-relative extent) instead of
   * a whole-index buffer, so opening + searching pulls only the term dictionary block + a term's postings.
   */
  public static DirectoryReader rangeReader(final RangeTable table, final RangeFetchIndexInput.RangeSource rawSource)
  {
    // One coalescing wrapper per column (shared across all this reader's clones/slices), so a prefetch hint and its
    // read collapse to one GET. No-op wrapper when range-prefetch is off — the raw source is used directly.
    final RangeFetchIndexInput.RangeSource source = RANGE_PREFETCH ? prefetchingSource(rawSource) : rawSource;
    final Map<String, int[]> dataOffsets = table.dataOffsets;
    final long datumBase = table.datumBase;
    // A1: coalesce the open-time small-file GET storm into one GET per contiguous run (see prefetchSmallFiles).
    final Map<String, ByteBuffer> prefetched = prefetchSmallFiles(dataOffsets, datumBase, source);
    final BaseDirectory directory = new BaseDirectory(new SingleInstanceLockFactory())
    {
      @Override
      public String[] listAll()
      {
        return dataOffsets.keySet().toArray(new String[0]);
      }

      @Override
      public void deleteFile(String name)
      {
        throw new UnsupportedOperationException("deleteFile");
      }

      @Override
      public long fileLength(String name) throws IOException
      {
        final int[] offsetLength = dataOffsets.get(name);
        if (offsetLength == null) {
          throw new FileNotFoundException(name);
        }
        return offsetLength[1];
      }

      @Override
      public IndexOutput createOutput(String name, IOContext context)
      {
        throw new UnsupportedOperationException("createOutput");
      }

      @Override
      public IndexOutput createTempOutput(String prefix, String suffix, IOContext context)
      {
        throw new UnsupportedOperationException("createTempOutput");
      }

      @Override
      public void sync(Collection<String> names)
      {
        throw new UnsupportedOperationException("sync");
      }

      @Override
      public void rename(String source1, String dest)
      {
        throw new UnsupportedOperationException("rename");
      }

      @Override
      public void syncMetaData()
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
        final long fileBase = datumBase + offsets[0];
        final int len = offsets[1];
        if (len <= WHOLE_FILE_THRESHOLD) {
          // small file (term index / metadata / norms): served from the coalesced A1 prefetch (no GET) when present,
          // else a single whole GET — it's read (near-)fully anyway. duplicate() so each open gets its own cursor.
          final ByteBuffer pre = prefetched.get(name);
          final ByteBuffer whole = pre != null ? pre.duplicate() : source.fetch(fileBase, len);
          if (whole == null || whole.remaining() != len) {
            throw new IOException("short whole-file read of " + name + ": wanted " + len);
          }
          return LuceneIndexInput.newInstance("LuceneIndex(name=" + name + ")", whole, len);
        }
        // big file (term dict / postings): range-read a term's block + postings on demand
        return new RangeFetchIndexInput("LuceneIndex(name=" + name + ")", source, fileBase, len, RANGE_BUFFER_SIZE);
      }

      @Override
      public Set<String> getPendingDeletions()
      {
        return Collections.emptySet();
      }

      @Override
      public void close()
      {
      }
    };
    try {
      final long _t0 = System.nanoTime();
      final DirectoryReader reader = DirectoryReader.open(directory);
      io.druid.java.util.common.RangeProf.openNanos.addAndGet(System.nanoTime() - _t0);
      io.druid.java.util.common.RangeProf.openCount.incrementAndGet();
      return reader;
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
      IndexInput input = directory.openInput(file, IOContext.DEFAULT);
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

  /**
   * Collect ALL docs matching {@code query} into a bitmap WITHOUT scoring — the filter/count path. Iterates each
   * segment's {@link Scorer} under {@link ScoreMode#COMPLETE_NO_SCORES}, so lucene skips the score computation, the
   * norms reads, and (crucially) the maxDoc-sized {@code TopScoreDocCollector} priority queue that
   * {@code search(query, numRows)} builds and prunes — which profiling showed was the dominant per-segment CPU cost
   * of a wide filter. Use this whenever no score ranking is needed (unlimited filter, no scoreField).
   */
  public static ImmutableBitmap collectAll(IndexSearcher searcher, Query query, FilterContext context)
      throws IOException
  {
    final BitmapFactory factory = context.bitmapFactory();
    final MutableBitmap bitmap = factory.makeEmptyMutableBitmap();
    final Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
    for (LeafReaderContext leaf : searcher.getIndexReader().leaves()) {
      final Scorer scorer = weight.scorer(leaf);
      if (scorer == null) {
        continue;   // no match in this segment
      }
      final int base = leaf.docBase;
      final DocIdSetIterator it = scorer.iterator();
      for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
        bitmap.add(base + doc);
      }
    }
    return factory.makeImmutableBitmap(bitmap);
  }

  public static ImmutableBitmap toBitmap(TopDocs searched, FilterContext context, String scoreField)
  {
    final BitmapFactory factory = context.bitmapFactory();
    final MutableBitmap bitmap = factory.makeEmptyMutableBitmap();
    for (ScoreDoc scoreDoc : searched.scoreDocs) {
      bitmap.add(scoreDoc.doc);   // can be slow
    }
    if (scoreField != null) {
      final Int2FloatRBTreeMap mapping = new Int2FloatRBTreeMap();
      for (ScoreDoc scoreDoc : searched.scoreDocs) {
        mapping.put(scoreDoc.doc, scoreDoc.score);
      }
      context.attach(scoreField, index -> mapping.getOrDefault(index, Float.NaN));
    }
    return factory.makeImmutableBitmap(bitmap);
  }

  // lucene index only exists in QueryableIndex, for now
  public static Column findColumnWithLuceneIndex(String field, QueryableIndex selector)
  {
    return findColumnWith(field, selector, c -> c.getCapabilities().hasLuceneIndex());
  }

  public static Column findColumnWithFST(String field, QueryableIndex selector)
  {
    return findColumnWith(field, selector, c -> c.getCapabilities().hasDictionaryFST());
  }

  private static Column findColumnWith(String field, QueryableIndex selector, Predicate<Column> predicate)
  {
    if (selector == null) {
      return null;
    }
    Column column = selector.getColumn(field);
    if (column != null && predicate.apply(column)) {
      return column;
    }
    for (int index = field.lastIndexOf('.'); index > 0; index = field.lastIndexOf('.', index - 1)) {
      column = selector.getColumn(field.substring(0, index));
      if (column != null && predicate.apply(column)) {
        return column;
      }
    }
    return null;
  }

  public static Pair<String, String> findLuceneField(String field, Column column, String... expected)
  {
    final String columnName = column.getName();
    final Map<String, String> columnDesc = column.getColumnDescs();
    if (!field.equals(columnName)) {
      String key = field.substring(columnName.length() + 1);
      if (columnDesc.containsKey(key)) {
        return Pair.of(key, columnDesc.get(key));
      }
    }
    for (Map.Entry<String, String> desc : columnDesc.entrySet()) {
      for (String prefix : expected) {
        if (desc.getValue().startsWith(prefix)) {
          return Pair.of(desc.getKey(), desc.getValue());
        }
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

  // Analyzers are thread-safe and designed for reuse (they keep per-thread TokenStreamComponents via a reuse
  // strategy), and IndexWriter.close() does not close its analyzer — so one instance per name is safe to share across
  // every query and segment build. Caching avoids rebuilding the expensive ones (icu/ngram/multilang each load
  // resources) on every getBitmapIndex call. Keyed by the raw name so a custom Class.forName analyzer keeps its case;
  // the distinct-name set is tiny, so the map needs no eviction.
  private static final ConcurrentHashMap<String, Analyzer> ANALYZER_CACHE = new ConcurrentHashMap<>();

  public static Analyzer createAnalyzer(String analyzer)
  {
    return ANALYZER_CACHE.computeIfAbsent(analyzer == null ? "standard" : analyzer, Lucenes::newAnalyzer);
  }

  private static Analyzer newAnalyzer(String analyzer)
  {
    switch (analyzer.toLowerCase()) {
      case "simple": return new SimpleAnalyzer();
      case "standard": return new StandardAnalyzer();
      case "english": return new EnglishAnalyzer();
      case "cjk": return new CJKAnalyzer();
      case "whitespace": return new WhitespaceAnalyzer();
      case "unicode_whitespace": return new UnicodeWhitespaceAnalyzer();
      case "keyword": return new KeywordAnalyzer();
      // split on runs of non-alphanumeric (nid.naver.com -> nid, naver, com; keeps digits), so a bare
      // keyword like "naver" matches domain-embedded tokens. ICU-folded, drops CRED_STOP_WORDS noise.
      case "delimiter": return delimiterAnalyzer(0, 0);
      // delimiter + n-grams of each token -> substring matching within a token (like LIKE '%kw%').
      case "ngram": return delimiterAnalyzer(2, 4);
      // ICU word segmentation + CJK bigrams (Korean/CJK) + ICU folding (NFKC normalize, case-fold,
      // accent-fold). Best for the mixed-language free text; ~ the reference ES "standard" analyzer.
      case "icu": return icuAnalyzer();
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
      // etc..
    }
    try {
      return (Analyzer) Class.forName(analyzer).getDeclaredConstructor().newInstance();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  // Noise tokens that are near-ubiquitous in credential-dump text (field labels, URL scheme, TLD):
  // dropped from the index so they don't bloat it or match everything.
  private static final CharArraySet CRED_STOP_WORDS = new CharArraySet(
      java.util.Arrays.asList("url", "user", "pass", "soft", "http", "https", "www", "profile", "com"), true
  );

  private static final java.util.regex.Pattern NON_ALPHANUM = java.util.regex.Pattern.compile("[^\\p{L}\\p{N}]+");
  private static final int MAX_TOKEN_LEN = 255;   // matches StandardTokenizer's default; keeps terms < 32766 bytes

  // Tokenizes on runs of non-alphanumeric characters, ICU-folds (NFKC normalize + case/accent fold),
  // and drops CRED_STOP_WORDS. When maxGram > 0, additionally emits n-grams [minGram, maxGram] of each
  // token for substring matching. Note: PatternTokenizer doesn't set CJK token types, so this does NOT
  // bigram CJK — use "icu" for Korean/CJK-heavy text.
  private static Analyzer delimiterAnalyzer(final int minGram, final int maxGram)
  {
    return new Analyzer()
    {
      @Override
      protected TokenStreamComponents createComponents(String fieldName)
      {
        final Tokenizer source = new PatternTokenizer(NON_ALPHANUM, -1);
        // drop absurdly long tokens (base64/hash blobs) — they aren't useful search terms and would
        // exceed Lucene's 32766-byte term limit (PatternTokenizer, unlike StandardTokenizer, doesn't cap).
        TokenStream stream = new LengthFilter(source, 1, MAX_TOKEN_LEN);
        stream = new ICUFoldingFilter(stream);
        stream = new StopFilter(stream, CRED_STOP_WORDS);
        if (maxGram > 0) {
          stream = new NGramTokenFilter(stream, minGram, maxGram, false);
        }
        return new TokenStreamComponents(source, stream);
      }
    };
  }

  // ICU word segmentation (script-aware, keeps CJK as words) -> CJK bigrams -> ICU folding -> stop words.
  // Approximates the reference ES "standard" analyzer (icu_tokenizer + cjk_bigram + icu_folding).
  private static Analyzer icuAnalyzer()
  {
    return new Analyzer()
    {
      @Override
      protected TokenStreamComponents createComponents(String fieldName)
      {
        final Tokenizer source = new ICUTokenizer();
        TokenStream stream = new CJKBigramFilter(source);
        stream = new LengthFilter(stream, 1, MAX_TOKEN_LEN);   // guard against immense blob tokens
        stream = new ICUFoldingFilter(stream);
        stream = new StopFilter(stream, CRED_STOP_WORDS);
        return new TokenStreamComponents(source, stream);
      }
    };
  }

  public static Polygon[] toLucenePolygons(SpatialContext context, ShapeFormat format, String shapeString)
      throws IOException, ParseException
  {
    if (format == ShapeFormat.GEOJSON) {
      return Polygon.fromGeoJSON(shapeString);
    }
    final Shape shape = ShapeFormat.WKT.newReader(context).read(shapeString);
    final Geometry geometry = GeomUtils.toGeometry(shape);
    if (geometry != null) {
      if (geometry instanceof org.locationtech.jts.geom.Polygon) {
        return new Polygon[]{toLucenePolygon((org.locationtech.jts.geom.Polygon) geometry)};
      }
      if (geometry instanceof MultiPolygon multiPolygon) {
        Polygon[] polygons = new Polygon[multiPolygon.getNumGeometries()];
        for (int i = 0; i < polygons.length; i++) {
          polygons[i] = toLucenePolygon((org.locationtech.jts.geom.Polygon) multiPolygon.getGeometryN(i));
        }
        return polygons;
      }
      // todo box?
      throw new IllegalArgumentException("invalid polygon");
    }
    // this fuck uses CRS84
    return Polygon.fromGeoJSON(ShapeFormat.GEOJSON.newWriter(context).toString(shape));
  }

  private static Polygon toLucenePolygon(org.locationtech.jts.geom.Polygon polygon)
  {
    LineString exterior = polygon.getExteriorRing();
    Polygon shell = toPolygon(exterior);

    int numHoles = polygon.getNumInteriorRing();
    if (numHoles == 0) {
      return shell;
    }
    Polygon[] holes = new Polygon[numHoles];
    for (int i = 0; i < numHoles; i++) {
      holes[i] = toPolygon(polygon.getInteriorRingN(i));
    }
    return new Polygon(shell.getPolyLats(), shell.getPolyLons(), holes);
  }

  private static Polygon toPolygon(LineString shell)
  {
    double[] x = new double[shell.getNumPoints()];
    double[] y = new double[shell.getNumPoints()];
    for (int i = 0; i < x.length; i++) {
      final Coordinate coordinate = shell.getCoordinateN(i);
      x[i] = coordinate.x;
      y[i] = coordinate.y;
    }
    return new Polygon(y, x);
  }
}
