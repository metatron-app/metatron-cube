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

package io.druid.indexer.path;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.logger.Logger;
import io.druid.data.Pair;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.hadoop.DatasourceInputFormat;
import io.druid.indexer.hadoop.InputFormatWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.druid.data.input.InputRow.CURRENT_PARTITION;

/**
 * This makes FileSplit cause it's FileInputFormat (todo)
 */
public class HadoopCombineInputFormat extends FileInputFormat
{
  private static final Logger log = new Logger(HadoopCombineInputFormat.class);

  public static final int COMBINE_PER_ELEMENT = 0;
  public static final int NO_COMBINE = -1;

  public static final ThreadLocal<String> CURRENT_DATASOURCE = new ThreadLocal<>();
  public static final ThreadLocal<Path> CURRENT_PATH = new ThreadLocal<>();

  private transient Boolean splitable;

  @Override
  protected boolean isSplitable(JobContext context, Path file)
  {
    if (splitable == null) {
      Configuration configuration = context.getConfiguration();
      if (configuration.getLong(HadoopPathSpec.SPLIT_SIZE, COMBINE_PER_ELEMENT) != COMBINE_PER_ELEMENT) {
        Class clazz = getInputFormatClass(configuration);
        if (TextInputFormat.class.isAssignableFrom(clazz) ||
            org.apache.hadoop.mapred.TextInputFormat.class.isAssignableFrom(clazz)) {
          CompressionCodec codec = new CompressionCodecFactory(configuration).getCodec(file);
          return splitable = codec == null || codec instanceof SplittableCompressionCodec;
        }
      }
      splitable = false;
    }
    return splitable;
  }

  @Override
  @SuppressWarnings("unchecked")
  public List<InputSplit> getSplits(JobContext context) throws IOException
  {
    Configuration conf = context.getConfiguration();

    // splitSize < 0 : no combine, splitSize == 0 : combine per elements
    final long splitSize = conf.getLong(HadoopPathSpec.SPLIT_SIZE, COMBINE_PER_ELEMENT);
    log.info("Start splitting on target size %,d", splitSize);

    List<PathSpecElement> elements = HadoopDruidIndexerConfig.JSON_MAPPER.readValue(
        conf.get(HadoopPathSpec.PATH_ELEMENTS_JSON), new TypeReference<List<PathSpecElement>>() { }
    );

    Map<String, List<String>> dsPathMap = Maps.newLinkedHashMap();
    for (PathSpecElement element : elements) {
      String dataSource = element.getDataSource();
      List<String> paths = dsPathMap.get(dataSource);
      if (paths == null) {
        dsPathMap.put(dataSource, paths = Lists.newArrayList());
      }
      paths.add(element.getPaths());
    }
    Job cloned = Job.getInstance(conf);

    List<InputSplit> result = Lists.newArrayList();

    long currentSize = 0;
    Map<String, List<FileSplit>> combined = Maps.newHashMap();
    for (Map.Entry<String, List<String>> entry : dsPathMap.entrySet()) {
      String dataSource = entry.getKey();
      if (splitSize == COMBINE_PER_ELEMENT) {
        for (String paths : entry.getValue()) {
          FileInputFormat.setInputPaths(cloned, paths);
          List splits = super.getSplits(cloned);
          result.add(new HadoopSplit(compact(ImmutableMap.<String, List<InputSplit>>of(dataSource, splits))));
        }
        continue;
      }
      List<FileSplit> splitList = combined.get(dataSource);
      if (splitList == null) {
        combined.put(dataSource, splitList = Lists.newArrayList());
      }
      FileInputFormat.setInputPaths(cloned, StringUtils.join(",", entry.getValue()));
      List<FileSplit> splits = super.getSplits(cloned);
      for (FileSplit split : splits) {
        splitList.add(split);
        currentSize += split.getLength();
        if (currentSize > splitSize) {
          log.info("..split with size %,d", currentSize);
          result.add(new HadoopSplit(compact(combined)));
          combined.clear();
          currentSize = 0;
          combined.put(dataSource, splitList = Lists.newArrayList());
        }
      }
    }
    Map<String, List<FileSplit>> compact = compact(combined);
    if (!compact.isEmpty()) {
      result.add(new HadoopSplit(compact));
    }
    for (int i = 0; i < result.size(); i++) {
      log.info("Split-[%04d] : [%s]", i, ((HadoopSplit) result.get(i)).splits);
    }
    return result;
  }

  private Map<String, List<FileSplit>> compact(Map<String, List<FileSplit>> mapping)
  {
    Map<String, List<FileSplit>> compact = Maps.newHashMap();
    for (Map.Entry<String, List<FileSplit>> entry : mapping.entrySet()) {
      List<FileSplit> splits = Lists.newArrayList();
      for (FileSplit split : entry.getValue()) {
        if (split.getLength() > 0) {
          splits.add(split);
        }
      }
      if (!splits.isEmpty()) {
        compact.put(entry.getKey(), splits);
      }
    }
    return compact;
  }

  public static class HadoopSplit extends InputSplit implements Writable
  {
    // datasource --> splits
    private final Map<String, List<FileSplit>> splits;

    public HadoopSplit()
    {
      splits = Maps.newHashMap();
    }

    public HadoopSplit(Map<String, List<FileSplit>> splits)
    {
      this.splits = splits;
    }

    @Override
    public long getLength() throws IOException, InterruptedException
    {
      long length = 0;
      for (List<FileSplit> splitList : splits.values()) {
        for (FileSplit split : splitList) {
          length += split.getLength();
        }
      }
      return length;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException
    {
      List<String> hosts = Lists.newArrayList();
      for (List<FileSplit> splitList : splits.values()) {
        for (FileSplit split : splitList) {
          hosts.addAll(Arrays.asList(split.getLocations()));
        }
      }
      return DatasourceInputFormat.getFrequentLocations(hosts, 3);
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
      out.writeInt(splits.size());
      for (Map.Entry<String, List<FileSplit>> entry : splits.entrySet()) {
        out.writeUTF(entry.getKey());
        out.writeInt(entry.getValue().size());
        for (FileSplit split : entry.getValue()) {
          split.write(out);
        }
      }
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
      int dataSourceLength = in.readInt();
      for (int i = 0; i < dataSourceLength; i++) {
        String dataSource = in.readUTF();
        int splitLength = in.readInt();
        List<FileSplit> splitList = Lists.newArrayListWithCapacity(splitLength);
        for (int j = 0; j < splitLength; j++) {
          FileSplit split = new FileSplit();
          split.readFields(in);
          splitList.add(split);
        }
        splits.put(dataSource, splitList);
      }
    }

    @Override
    public String toString()
    {
      return splits.toString();
    }
  }

  @Override
  public RecordReader createRecordReader(
      final InputSplit split,
      final TaskAttemptContext context
  ) throws IOException
  {
    final Configuration conf = context.getConfiguration();

    final HadoopSplit hadoopSplit = (HadoopSplit) split;
    final InputFormat format = getInputFormat(conf);

    final String extractPartitionRegex = conf.get(HadoopPathSpec.EXTRACT_PARTITION_REGEX, null);
    final boolean extractPartition = conf.getBoolean(HadoopPathSpec.EXTRACT_PARTITION, false);
    final Pair<Matcher, Set<String>> matcher = makeExtractor(extractPartitionRegex);

    return new RecordReader()
    {
      private int index;
      private final Iterator<Map.Entry<String, List<FileSplit>>> iterator = hadoopSplit.splits.entrySet().iterator();
      private List<FileSplit> splits;

      private RecordReader reader;
      private long progress;
      private long totalLength;

      @Override
      public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
      {
        totalLength = split.getLength();
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException
      {
        while (reader == null || !reader.nextKeyValue()) {
          if (!initNextRecordReader()) {
            return false;
          }
        }
        return true;
      }

      private boolean initNextRecordReader() throws IOException, InterruptedException
      {
        if (reader != null) {
          progress += splits.get(index - 1).getLength();    // done processing so far
          reader.close();
          reader = null;
        }
        if (splits == null || index == splits.size()) {
          if (!iterator.hasNext()) {
            return false;
          }
          Map.Entry<String, List<FileSplit>> next = iterator.next();
          CURRENT_DATASOURCE.set(next.getKey());
          log.info("Reading for datasource %s", CURRENT_DATASOURCE.get());
          splits = next.getValue();
          index = 0;
        }
        FileSplit split = splits.get(index++);

        CURRENT_PATH.set(split.getPath());
        if (extractPartition) {
          CURRENT_PARTITION.set(extractPartition(split.getPath(), matcher));
          log.info("Reading from path %s [%s]", CURRENT_PATH.get(), CURRENT_PARTITION.get());
        } else {
          log.info("Reading from path %s", CURRENT_PATH.get());
        }

        reader = format.createRecordReader(split, context);
        reader.initialize(split, context);
        return true;
      }

      @Override
      public Object getCurrentKey() throws IOException, InterruptedException
      {
        return reader.getCurrentKey();
      }

      @Override
      public Object getCurrentValue() throws IOException, InterruptedException
      {
        return reader.getCurrentValue();
      }

      @Override
      public float getProgress() throws IOException, InterruptedException
      {
        long subProgress = 0;
        if (reader != null) {
          subProgress = (long) (reader.getProgress() * splits.get(index - 1).getLength());
        }
        return Math.min(1.0f, (progress + subProgress) / (float) totalLength);
      }

      @Override
      public void close() throws IOException
      {
        if (reader != null) {
          reader.close();
          reader = null;
        }
      }
    };
  }

  private Class getInputFormatClass(Configuration conf)
  {
    if (conf.get(HadoopPathSpec.INPUT_FORMAT_NEW) != null) {
      return conf.getClass(HadoopPathSpec.INPUT_FORMAT_NEW, null, InputFormat.class);
    } else if (conf.get(HadoopPathSpec.INPUT_FORMAT_OLD) != null) {
      return conf.getClass(HadoopPathSpec.INPUT_FORMAT_OLD, null, org.apache.hadoop.mapred.InputFormat.class);
    }
    return TextInputFormat.class;
  }

  private InputFormat getInputFormat(Configuration conf)
  {
    final Class inputFormat = getInputFormatClass(conf);
    if (InputFormat.class.isAssignableFrom(inputFormat)) {
      return (InputFormat) ReflectionUtils.newInstance(inputFormat, conf);
    } else if (org.apache.hadoop.mapred.InputFormat.class.isAssignableFrom(inputFormat)) {
      return new InputFormatWrapper(
          (org.apache.hadoop.mapred.InputFormat) ReflectionUtils.newInstance(inputFormat, conf)
      );
    }
    throw new IllegalArgumentException("never");
  }

  private boolean logged;

  final Pair<Matcher, Set<String>> makeExtractor(String extractPartitionRegex)
  {
    if (extractPartitionRegex == null) {
      return null;
    }
    Pattern pattern = Pattern.compile(extractPartitionRegex);
    Set<String> namedGroups = getNamedGroupCandidates(extractPartitionRegex);
    if (namedGroups.isEmpty()) {
      throw new IAE("cannot find named groups in regex %s", extractPartitionRegex);
    }
    return Pair.of(pattern.matcher(""), namedGroups);
  }

  final Map<String, Object> extractPartition(Path path, Pair<Matcher, Set<String>> extractor)
  {
    Map<String, Object> partition = Maps.newLinkedHashMap();
    if (extractor != null) {
      Matcher matcher = extractor.lhs;
      if (!matcher.reset(path.toString()).find()) {
        if (!logged) {
          logged = true;
          log.info(
              "Failed to extract partition from %s by regex %s.. will ignore similar problems", path, matcher.pattern()
          );
        }
        return partition;
      }
      for (String groupName : extractor.rhs) {
        String groupValue = matcher.group(groupName);
        if (groupValue != null) {
          partition.put(groupName, groupValue);
        }
      }
      return partition;
    }
    for (; path != null; path = path.getParent()) {
      String pathName = path.getName();
      int index = pathName.indexOf('=');
      if (index < 0 && !partition.isEmpty()) {
        return partition;
      }
      if (index > 0) {
        partition.put(pathName.substring(0, index), pathName.substring(index + 1));
      }
    }
    return partition;
  }

  // hate this..
  private static Set<String> getNamedGroupCandidates(String regex)
  {
    Set<String> namedGroups = Sets.newTreeSet();
    Matcher m = Pattern.compile("\\(\\?<([a-zA-Z][a-zA-Z0-9]*)>").matcher(regex);

    while (m.find()) {
      namedGroups.add(m.group(1));
    }

    return namedGroups;
  }
}
