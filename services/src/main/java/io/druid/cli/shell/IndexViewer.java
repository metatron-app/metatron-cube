/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package io.druid.cli.shell;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.Pair;
import com.metamx.common.StringUtils;
import com.metamx.common.guava.CloseQuietly;
import com.metamx.common.logger.Logger;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.JodaUtils;
import io.druid.data.ValueDesc;
import io.druid.data.ValueType;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.IndexIO;
import io.druid.segment.Metadata;
import io.druid.segment.QueryableIndex;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ComplexColumn;
import io.druid.segment.column.DictionaryEncodedColumn;
import io.druid.segment.column.GenericColumn;
import io.druid.segment.column.HistogramBitmap;
import io.druid.segment.data.CompressedObjectStrategy.CompressionStrategy;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.loading.DataSegmentPusherUtil;
import io.druid.segment.loading.StorageLocationConfig;
import io.druid.timeline.DataSegment;
import org.apache.commons.io.FileUtils;
import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.ParsedLine;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.DefaultParser;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 */
public class IndexViewer extends CommonShell.WithUtils
{
  private static final Logger LOG = new Logger(DruidShell.class);

  private static final String DEFAULT_PROMPT = "> ";
  private static final DateTimeZone DEFAULT_TIMEZONE = DateTimeZone.UTC;

  private final IndexIO indexIO;
  private final ObjectMapper jsonMapper;

  @Inject
  public IndexViewer(IndexIO indexIO)
  {
    this.indexIO = indexIO;
    this.jsonMapper = indexIO.getObjectMapper();
  }

  public void run(List<String> arguments) throws Exception
  {
    String prompt = DEFAULT_PROMPT;
    DateTimeZone timeZone = DEFAULT_TIMEZONE;
    if (!GuavaUtils.isNullOrEmpty(arguments)) {
      for (int i = 0; i < arguments.size() - 1; i++) {
        final String argument = arguments.get(i);
        if (argument.equals("-z") || argument.equals("--zone")) {
          timeZone = JodaUtils.toTimeZone(arguments.get(++i));
        } else if (argument.equals("-p")) {
          prompt = arguments.get(++i);
        }
      }
    }
    // segmentId to index
    Map<String, IndexMeta> mapping1 = Maps.newHashMap();
    // ds to indices
    Map<String, List<IndexMeta>> mapping2 = Maps.newHashMap();

    String props = loadNodeProperties("historical").getProperty("druid.segmentCache.locations");
    List<StorageLocationConfig> locationConfs = jsonMapper.readValue(
        props, new TypeReference<List<StorageLocationConfig>>() {}
    );

    if (GuavaUtils.isNullOrEmpty(locationConfs)) {
      LOG.info("druid.segmentCache.locations is not specified.. use -p <property-file>");
    } else {
      File baseDir = new File(locationConfs.get(0).getPath(), "info_dir");
      if (!baseDir.exists() && !baseDir.mkdirs()) {
        return;
      }
      File[] segmentsToLoad = baseDir.listFiles();
      if (segmentsToLoad == null) {
        return;
      }
      List<File> locations = Lists.newArrayList();
      for (StorageLocationConfig conf : locationConfs) {
        locations.add(conf.getPath());
      }
      LOG.info("Total %d segments found in %s", segmentsToLoad.length, baseDir);
      int fails = 0;
      for (File file : segmentsToLoad) {
        DataSegment segment = jsonMapper.readValue(file, DataSegment.class);
        IndexMeta index = find(locations, segment, timeZone);
        if (index == null) {
          if (fails++ > 100) {
            throw new IllegalArgumentException("too many fails.. invalid timezone?");
          }
          continue;
        }
        List<IndexMeta> indices = mapping2.get(segment.getDataSource());
        if (indices == null) {
          mapping2.put(segment.getDataSource(), indices = Lists.<IndexMeta>newArrayList());
        }
        indices.add(index);
        mapping1.put(segment.getIdentifier(), index);
      }
    }

    // sort on time
    for (List<IndexMeta> index : mapping2.values()) {
      Collections.sort(
          index,
          Ordering.from(JodaUtils.intervalsByStartThenEnd())
                  .onResultOf(
                      new Function<IndexMeta, Interval>()
                      {
                        @Override
                        public Interval apply(IndexMeta input) { return input.getDataInterval(); }
                      }
                  )
      );
    }

    try (Terminal terminal = TerminalBuilder.builder().build()) {
      execute(mapping1, mapping2, terminal, prompt);
    }
  }

  private IndexMeta find(List<File> locations, DataSegment segment, DateTimeZone timeZone) throws IOException
  {
    String storageDir = DataSegmentPusherUtil.getStorageDir(segment, timeZone);
    for (File location : locations) {
      File localStorageDir = new File(location, storageDir);
      if (localStorageDir.exists()) {
        return new IndexMeta(localStorageDir, segment);
      }
    }
    LOG.info("Failed to locate segment %s in %s", segment, storageDir);
    return null;
  }

  private void execute(
      final Map<String, IndexMeta> mapping1,
      final Map<String, List<IndexMeta>> mapping2,
      final Terminal terminal,
      final String prompt
  )
      throws Exception
  {
    final PrintWriter writer = terminal.writer();

    final Function<String, Candidate> toCandidate = new Function<String, Candidate>()
    {
      public Candidate apply(String input) { return new Candidate(input); }
    };

    Completer dsCompleter = new Completer()
    {
      @Override
      public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates)
      {
        if (line.wordIndex() == 0) {
          candidates.addAll(Lists.newArrayList(Iterables.transform(mapping2.keySet(), toCandidate)));
        }
      }
    };

    LineReader reader = LineReaderBuilder.builder()
                                         .terminal(terminal)
                                         .parser(new DefaultParser())
                                         .completer(dsCompleter)
                                         .build();

    while (true) {
      String line = readLine(reader, prompt);
      if (line == null || line.equals(";")) {
        break;
      }
      if (line.equalsIgnoreCase("help")) {
        writer.println(Arrays.asList(";", "?", "<datasource>", "<segment-id>"));
        continue;
      }
      String[] commands = line.split(" ");
      if (commands[0].trim().equalsIgnoreCase("segment")) {
        IndexMeta index = new IndexMeta(new File(commands[1].trim()), null);
        dumpIndex(index, writer);
        continue;
      }
      if (line.equalsIgnoreCase("?")) {
        for (String ds : mapping2.keySet()) {
          writer.println("  " + ds);
        }
        continue;
      }
      IndexMeta index = mapping1.get(line);
      if (index != null) {
        dumpIndex(index, writer);
        continue;
      }
      String matchedDataSource = null;
      List<IndexMeta> indices = mapping2.get(line);
      if (indices != null && !indices.isEmpty()) {
        matchedDataSource = line;
      } else {
        int i = -1;
        do {
          i = line.indexOf('_', i + 1);
        }
        while (i >= 0 && !mapping2.containsKey(line.substring(0, i)));
        if (i > 0) {
          matchedDataSource = line.substring(0, i);
        }
      }

      if (matchedDataSource != null) {
        List<IndexMeta> values = mapping2.get(matchedDataSource);
        for (int j = 0; j < values.size(); j++) {
          IndexMeta indexMeta = values.get(j);
          writer.println(format("  > %d : %s (%,d bytes)", j, indexMeta.segment.getIdentifier(), indexMeta.size));
        }
        String format = format("  which one ? (0 ~ %d, ';' for quit) > ", values.size() - 1);
        while (true) {
          line = readLine(reader, format);
          if (line == null || line.contentEquals(";")) {
            break;
          }
          try {
            dumpIndex(values.get(Integer.valueOf(line)), writer);
          }
          catch (NumberFormatException e) {
            // ignore
          }
          catch (Exception e) {
            LOG.info("! Exception occurred.. %s", e);
          }
        }
        continue;
      }
      writer.println("Cannot find the segment or even datasource of it");
    }
  }

  private String readLine(LineReader reader, String prompt)
  {
    while (true) {
      String line = null;
      try {
        line = reader.readLine(prompt);
      }
      catch (UserInterruptException e) {
        // Ignore
      }
      catch (EndOfFileException e) {
        return null;
      }
      if (line == null) {
        continue;
      }
      line = line.trim();
      if (line.isEmpty()) {
        continue;
      }
      return line;
    }
  }

  private void dumpIndex(IndexMeta indexMeta, PrintWriter writer)
  {
    try (QueryableIndex index = indexMeta.index()) {
      dumpIndex(index, indexMeta.offsets.get(), writer);
    }
    catch (IOException e) {
      // ignore
    }
  }

  private void dumpIndex(QueryableIndex index, Map<String, int[]> offsets, PrintWriter writer)
  {
    long totalSize = 0;
    List<Pair<String, int[]>> values = Lists.newArrayList();
    Iterable<String> columns = Iterables.concat(Arrays.asList(Column.TIME_COLUMN_NAME), index.getColumnNames());
    for (String column : columns) {
      values.add(Pair.of(column, offsets.get(column)));
      totalSize += index.getColumn(column).getSerializedSize();
    }
    Collections.sort(
        values, new Comparator<Pair<String, int[]>>()
        {
          @Override
          public int compare(Pair<String, int[]> o1, Pair<String, int[]> o2)
          {
            int compare = Ints.compare(o1.rhs[0], o2.rhs[0]);
            if (compare == 0) {
              compare = Ints.compare(o1.rhs[1], o2.rhs[1]);
            }
            return compare;
          }
        }
    );
    writer.println();
    writer.println(format("> Size of Index (except metadata) : %,d bytes", totalSize));
    writer.println(
        format("  Number of Rows in index : %,d", index.getColumn(Column.TIME_COLUMN_NAME).getNumRows())
    );
    Metadata metadata = index.getMetadata();
    if (metadata == null) {
      writer.println("  No metadata");
    } else {
      if (!metadata.getContainer().isEmpty()) {
        writer.println("  Container");
        for (Map.Entry<String, Object> entry : metadata.getContainer().entrySet()) {
          writer.println("  " + entry);
        }
      }
      writer.println("  Query Granularity : " + metadata.getQueryGranularity());
      String rollup = Objects.toString(metadata.isRollup(), "unknown");
      writer.println(
          format("  Ingested Number of Rows : %,d (rolled-up ? %s)", metadata.getIngestedNumRows(), rollup)
      );
      if (metadata.getAggregators() != null && metadata.getAggregators().length > 0) {
        writer.println("  Aggregators");
        for (AggregatorFactory aggregator : metadata.getAggregators()) {
          writer.println("    " + aggregator.toString());
        }
      }
    }
    String bitmapFactory = index.getBitmapFactoryForDimensions().getClass().getSimpleName();
    writer.println(format("  Bitmap Factory : %s", bitmapFactory));
    writer.println();

    Set<String> dimensions = Sets.newHashSet(index.getAvailableDimensions());
    dimensions.add(Column.TIME_COLUMN_NAME);

    for (Pair<String, int[]> value : values) {
      String columnName = value.lhs;
      int[] offset = value.rhs;

      Column column = index.getColumn(columnName);
      ColumnCapabilities capabilities = column.getCapabilities();

      long columnSize = column.getSerializedSize();
      boolean dimensionsType = dimensions.contains(columnName);

      String columnType = dimensionsType ? "dimension" : "metric";
      writer.println(
          format(
              "> %s '%s' (%s, %,d ~ %,d : %3.1f%% of total)",
              columnType, columnName, toChunkFile(offset[0]), offset[1], offset[2], (columnSize * 100f / totalSize)
          )
      );
      ValueDesc desc;
      ValueType type = capabilities.getType();
      if (!type.isPrimitive()) {
        ComplexColumn complexColumn = column.getComplexColumn();
        desc = complexColumn.getType();   // more specific for complex type
        CloseQuietly.close(complexColumn);
      } else {
        desc = ValueDesc.of(type);
      }
      if (dimensionsType) {
        boolean multipleValued = capabilities.hasMultipleValues();
        writer.print(format("  type : %s (hasMultiValue = %s, size = %,d bytes)", desc, multipleValued, columnSize));
      } else {
        CompressionStrategy compressionType = CompressionStrategy.UNCOMPRESSED;
        GenericColumn genericColumn = column.getGenericColumn();
        if (genericColumn != null) {
          compressionType = genericColumn.compressionType();
          CloseQuietly.close(genericColumn);
        }
        writer.print(format("  type : %s (compression = %s, size = %,d bytes)", desc, compressionType, columnSize));
      }
      Map<String, Object> columnStats = column.getColumnStats();
      if (!GuavaUtils.isNullOrEmpty(columnStats)) {
        for (Map.Entry<String, Object> entry : columnStats.entrySet()) {
          String stat = Objects.toString(entry.getValue(), null);
          if (stat != null && stat.length() > 16) {
            entry.setValue(stat.substring(0, 12) + "...(abbreviated)");
          }
        }
        writer.println(format(", stats %s", columnStats));
      } else {
        writer.println();
      }

      StringBuilder builder = new StringBuilder().append("  ");
      if (capabilities.isDictionaryEncoded()) {
        DictionaryEncodedColumn dictionaryEncoded = column.getDictionaryEncoding();
        GenericIndexed<String> dictionary = dictionaryEncoded.dictionary();
        boolean hasSketch = dictionaryEncoded.hasSketch();
        long dictionarySize = dictionary.getSerializedSize();
        long encodedSize = column.getSerializedSize(Column.EncodeType.DICTIONARY_ENCODED);
        String hasNull = dictionary.isSorted() ? String.valueOf(dictionary.indexOf(null) >= 0) : "unknown";
        builder.append(
            format(
                "dictionary (cardinality = %d, hasNull = %s, hasSketch = %s, size = %,d bytes), rows (%,d bytes)",
                dictionary.size(), hasNull, hasSketch, dictionarySize, encodedSize - dictionarySize
            )
        );
        CloseQuietly.close(dictionaryEncoded);
      }
      if (capabilities.hasBitmapIndexes()) {
        if (builder.length() > 2) {
          builder.append(", ");
        }
        builder.append(format("bitmap (%,d bytes)", column.getSerializedSize(Column.EncodeType.BITMAP)));
      }
      if (capabilities.hasSpatialIndexes()) {
        if (builder.length() > 2) {
          builder.append(", ");
        }
        builder.append(format("spatial indexed (%,d bytes)", column.getSerializedSize(Column.EncodeType.SPATIAL)));
      }
      if (capabilities.isRunLengthEncoded()) {
        if (builder.length() > 2) {
          builder.append(", ");
        }
        builder.append(format("RLE encoded (%,d bytes)", column.getSerializedSize(Column.EncodeType.RUNLENGTH_ENCODED)));
      }
      if (capabilities.hasMetricBitmap()) {
        if (builder.length() > 2) {
          builder.append(", ");
        }
        HistogramBitmap bitmap = column.getMetricBitmap();
        builder.append(
            format(
                "metric bitmap (%d bitmaps, %,d zeros, %,d bytes)",
                bitmap.numBins(), bitmap.zeroRows(), column.getSerializedSize(Column.EncodeType.METRIC_BITMAP)
            )
        );
      }
      if (capabilities.hasBitSlicedBitmap()) {
        if (builder.length() > 2) {
          builder.append(", ");
        }
        builder.append(
            format("bit sliced bitmap (%,d bytes)", column.getSerializedSize(Column.EncodeType.BITSLICED_BITMAP))
        );
      }
      if (capabilities.hasLuceneIndex()) {
        if (builder.length() > 2) {
          builder.append(", ");
        }
        builder.append(format("lucene index (%,d bytes)", column.getSerializedSize(Column.EncodeType.LUCENE_INDEX)));
        Map<String, String> columnDescs = column.getColumnDescs();
        if (!GuavaUtils.isNullOrEmpty(columnDescs)) {
          builder.append(format(", descs %s", columnDescs));
        }
      }
      if (builder.length() > 2) {
        writer.println(builder.toString());
      }
      writer.println();
    }
  }

  private String format(String format, Object... arguments)
  {
    return StringUtils.safeFormat(format, arguments);
  }

  private Map<String, int[]> load(File baseDir) throws IOException
  {
    File metaFile = new File(baseDir, toMetaFile());

    BufferedReader in = null;
    try {
      in = new BufferedReader(new InputStreamReader(new FileInputStream(metaFile), Charsets.UTF_8));

      String line = in.readLine();
      if (line == null) {
        throw new ISE("First line should be version,maxChunkSize,numChunks, got null.");
      }

      String[] splits = line.split(",");
      if (!"v1".equals(splits[0])) {
        throw new ISE("Unknown version[%s], v1 is all I know.", splits[0]);
      }
      if (splits.length != 3) {
        throw new ISE("Wrong number of splits[%d] in line[%s]", splits.length, line);
      }

      Map<String, int[]> internalFiles = Maps.newTreeMap();
      while ((line = in.readLine()) != null) {
        splits = line.split(",");

        if (splits.length != 4) {
          throw new ISE("Wrong number of splits[%d] in line[%s]", splits.length, line);
        }
        internalFiles.put(
            splits[0],
            new int[]{Integer.parseInt(splits[1]), Integer.parseInt(splits[2]), Integer.parseInt(splits[3])}
        );
      }
      return internalFiles;
    }
    finally {
      Closeables.close(in, false);
    }
  }

  private String toMetaFile() {return String.format("meta.%s", "smoosh");}

  private String toChunkFile(int i) {return String.format("%05d.%s", i, "smoosh");}

  private class IndexMeta
  {
    private final File location;
    private final long size;
    private final DataSegment segment;
    private final Supplier<QueryableIndex> index;

    private IndexMeta(final File location, final DataSegment segment) throws IOException
    {
      this.location = location;
      this.size = FileUtils.sizeOfDirectory(location);
      this.segment = segment;
      this.index = new Supplier<QueryableIndex>()
      {
        @Override
        public QueryableIndex get()
        {
          try {
            return indexIO.loadIndex(location);
          }
          catch (Exception e) {
            LOG.info(e, "Failed to load segment %s from %s", Objects.toString(segment, "x"), location);
            throw Throwables.propagate(e);
          }
        }
      };
    }

    private final Supplier<Map<String, int[]>> offsets = Suppliers.memoize(
        new Supplier<Map<String, int[]>>()
        {
          @Override
          public Map<String, int[]> get()
          {
            try {
              return load(location);
            }
            catch (Exception e) {
              throw Throwables.propagate(e);
            }
          }
        }
    );

    public QueryableIndex index()
    {
      return index.get();
    }

    public Interval getDataInterval()
    {
      return segment != null ? segment.getInterval() : index().getDataInterval();
    }
  }
}
