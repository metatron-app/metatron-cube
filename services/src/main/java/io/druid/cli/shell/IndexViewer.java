package io.druid.cli.shell;

import com.fasterxml.jackson.annotation.JacksonInject;
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
import io.druid.common.utils.JodaUtils;
import io.druid.data.ValueType;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.IndexIO;
import io.druid.segment.Metadata;
import io.druid.segment.SimpleQueryableIndex;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ComplexColumn;
import io.druid.segment.column.DictionaryEncodedColumn;
import io.druid.segment.column.MetricBitmap;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.loading.DataSegmentPusherUtil;
import io.druid.segment.loading.SegmentLoaderConfig;
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
public class IndexViewer implements CommonShell
{
  private static final Logger LOG = new Logger(DruidShell.class);

  private final IndexIO indexIO;
  private final SegmentLoaderConfig config;
  private final ObjectMapper jsonMapper;

  @Inject
  public IndexViewer(
      IndexIO indexIO,
      SegmentLoaderConfig config,
      @JacksonInject ObjectMapper jsonMapper
  )
  {
    this.indexIO = indexIO;
    this.config = config;
    this.jsonMapper = jsonMapper;
  }

  public void run(List<String> arguments) throws Exception
  {
    File baseDir = config.getInfoDir();
    if (!baseDir.exists() && !config.getInfoDir().mkdirs()) {
      return;
    }
    File[] segmentsToLoad = baseDir.listFiles();
    if (segmentsToLoad == null) {
      return;
    }
    List<File> locations = Lists.newArrayList();
    for (StorageLocationConfig conf : config.getLocations()) {
      locations.add(conf.getPath());
    }
    // segmentId to index
    Map<String, IndexMeta> mapping1 = Maps.newHashMap();
    // ds to indices
    Map<String, List<IndexMeta>> mapping2 = Maps.newHashMap();
    LOG.info("Total %d segments found in %s", segmentsToLoad.length, baseDir);
    for (File file : segmentsToLoad) {
      DataSegment segment = jsonMapper.readValue(file, DataSegment.class);
      IndexMeta index = find(locations, segment);
      if (index == null) {
        continue;
      }
      List<IndexMeta> indices = mapping2.get(segment.getDataSource());
      if (indices == null) {
        mapping2.put(segment.getDataSource(), indices = Lists.<IndexMeta>newArrayList());
      }
      indices.add(index);
      mapping1.put(segment.getIdentifier(), index);
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
      execute(mapping1, mapping2, terminal);
    }
  }

  private IndexMeta find(List<File> locations, DataSegment segment) throws IOException
  {
    String storageDir = DataSegmentPusherUtil.getStorageDir(segment);
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
      final Terminal terminal
  )
      throws Exception
  {
    final String prompt = "> ";
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
      }
      if (line.equalsIgnoreCase("?")) {
        for (String ds : mapping2.keySet()) {
          writer.println("> " + ds);
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

  private void dumpIndex(IndexMeta index, PrintWriter writer)
  {
    long totalSize = 0;
    Map<String, int[]> offsets = index.offsets.get();
    List<Pair<String, int[]>> values = Lists.newArrayList();
    Iterable<String> columns = Iterables.concat(Arrays.asList(Column.TIME_COLUMN_NAME), index.index.getColumnNames());
    for (String column : columns) {
      values.add(Pair.of(column, offsets.get(column)));
      totalSize += index.index.getColumn(column).getSerializedSize();
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
          "  Ingested NumRows : " + metadata.getIngestedNumRows() + " (rolled-up ? " + rollup + ")"
      );
      if (metadata.getAggregators() != null && metadata.getAggregators().length > 0) {
        writer.println("  Aggregators");
        for (AggregatorFactory aggregator : metadata.getAggregators()) {
          writer.println("    " + aggregator.toString());
        }
      }
    }
    String bitmapFactory = index.index.getBitmapFactoryForDimensions().getClass().getSimpleName();
    writer.println(format("  Bitmap Factory : %s", bitmapFactory));
    writer.println();

    Set<String> dimensions = Sets.newHashSet(index.index.getAvailableDimensions());
    dimensions.add(Column.TIME_COLUMN_NAME);

    for (Pair<String, int[]> value : values) {
      String columnName = value.lhs;
      int[] offset = value.rhs;
      String columnType = dimensions.contains(columnName) ? "dimension" : "metric";
      writer.println(
          format("> %s '%s' (%s, %,d ~ %,d)", columnType, columnName, toChunkFile(offset[0]), offset[1], offset[2])
      );
      Column column = index.index.getColumn(columnName);
      ColumnCapabilities capabilities = column.getCapabilities();

      int numRows = column.getLength();
      long columnSize = column.getSerializedSize();

      ValueType type = capabilities.getType();
      ComplexColumn complexColumn = column.getComplexColumn();
      if (complexColumn != null) {
        type = ValueType.of(complexColumn.getTypeName());   // more specific for complex type
        CloseQuietly.close(complexColumn);
      }
      Map<String, Object> columnStats = column.getColumnStats();
      writer.print(
          format(
              "  type : %s, numRows : %d, hasMultiValue = %s, (%,d bytes, %3.1f%% of total)",
              type, numRows, capabilities.hasMultipleValues(), columnSize, (columnSize * 100f / totalSize)
          )
      );
      if (columnStats != null) {
        writer.println(format(", stats %s", columnStats));
      } else {
        writer.println();
      }
      StringBuilder builder = new StringBuilder().append("  ");
      if (capabilities.isDictionaryEncoded()) {
        DictionaryEncodedColumn dictionaryEncoded = column.getDictionaryEncoding();
        GenericIndexed<String> dictionary = dictionaryEncoded.dictionary();
        long dictionarySize = dictionary.getSerializedSize();
        long encodedSize = column.getSerializedSize(Column.EncodeType.DICTIONARY_ENCODED);
        builder.append(
            format(
                "dictionary encoded (cardinality = %d, hasNull = %s, dictionary = %,d bytes, rows = %,d bytes)",
                dictionary.size(), dictionary.indexOf(null) >= 0, dictionarySize, (encodedSize - dictionarySize)
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
        MetricBitmap bitmap = column.getMetricBitmap();
        builder.append(
            format(
                "metric bitmap (%d bitmaps, %,d bytes)",
                bitmap.size(), column.getSerializedSize(Column.EncodeType.METRIC_BITMAP)
            )
        );
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
      final Integer numFiles = Integer.valueOf(splits[2]);
      List<File> outFiles = Lists.newArrayListWithExpectedSize(numFiles);

      for (int i = 0; i < numFiles; ++i) {
        outFiles.add(new File(baseDir, toChunkFile(i)));
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
    private final SimpleQueryableIndex index;

    private IndexMeta(File location, DataSegment segment) throws IOException
    {
      this.location = location;
      this.size = FileUtils.sizeOfDirectory(location);
      this.segment = segment;
      this.index = (SimpleQueryableIndex) indexIO.loadIndex(location);
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

    public Interval getDataInterval()
    {
      return index.getDataInterval();
    }

    public Metadata getMetadata()
    {
      return index.getMetadata();
    }
  }
}
