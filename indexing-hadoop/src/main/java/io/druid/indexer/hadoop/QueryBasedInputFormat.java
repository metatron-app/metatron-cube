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

package io.druid.indexer.hadoop;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metamx.common.Pair;
import com.metamx.common.StringUtils;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.HttpClientConfig;
import com.metamx.http.client.HttpClientInit;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.collections.CountingMap;
import io.druid.granularity.QueryGranularities;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.jackson.DruidDefaultSerializersModule;
import io.druid.query.Druids;
import io.druid.query.LocatedSegmentDescriptor;
import io.druid.query.Result;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.select.EventHolder;
import io.druid.query.select.PagingSpec;
import io.druid.query.select.SelectQuery;
import io.druid.query.select.SelectResultValue;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.segment.column.Column;
import io.druid.server.DruidNode;
import io.druid.server.coordination.DruidServerMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.Interval;

import javax.ws.rs.core.MediaType;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Reader;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public class QueryBasedInputFormat extends InputFormat<NullWritable, MapWritable>
    implements org.apache.hadoop.mapred.InputFormat<NullWritable, MapWritable>
{
  protected static final Logger logger = new Logger(QueryBasedInputFormat.class);

  public static final String CONF_DRUID_BROKER_ADDRESS = "druid.broker.address";
  public static final String CONF_DRUID_DATASOURCE = "druid.datasource";
  public static final String CONF_DRUID_INTERVALS = "druid.intervals";
  public static final String CONF_DRUID_COLUMNS = "druid.columns";
  public static final String CONF_DRUID_FILTERS = "druid.filters";
  public static final String CONF_DRUID_TIME_COLUMN_NAME = "druid.time.column.name";

  public static final String CONF_MAX_SPLIT_SIZE = "druid.max.split.size";
  public static final String CONF_SELECT_THRESHOLD = "druid.select.threshold";

  public static final int DEFAULT_SELECT_THRESHOLD = 10000;
  public static final int DEFAULT_MAX_SPLIT_SIZE = -1;  // split per segment

  protected Configuration configure(Configuration configuration, ObjectMapper mapper) throws IOException
  {
    return configuration;
  }

  @Override
  public org.apache.hadoop.mapred.InputSplit[] getSplits(JobConf job, int numSplits) throws IOException
  {
    return getInputSplits(job);
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException
  {
    return Arrays.<InputSplit>asList(getInputSplits(context.getConfiguration()));
  }

  protected DruidInputSplit[] getInputSplits(Configuration conf) throws IOException
  {
    ObjectMapper mapper = new ObjectMapper();
    mapper.registerModule(new DruidDefaultSerializersModule());

    conf = configure(conf, mapper);

    String brokerAddress = Preconditions.checkNotNull(conf.get(CONF_DRUID_BROKER_ADDRESS), "Missing broker address");
    String dataSource = Preconditions.checkNotNull(conf.get(CONF_DRUID_DATASOURCE), "Missing datasource name");
    String intervals = Preconditions.checkNotNull(conf.get(CONF_DRUID_INTERVALS), "Missing interval");
    String filters = conf.get(CONF_DRUID_FILTERS);

    String requestURL =
        String.format(
            "%s/druid/v2/candidates?datasource=%s&intervals=%s",
            brokerAddress,
            dataSource,
            URLEncoder.encode(intervals, StringUtils.UTF8_STRING)
        );


    Lifecycle lifecycle = new Lifecycle();
    HttpClient client = HttpClientInit.createClient(HttpClientConfig.builder().build(), lifecycle);

    StatusResponseHolder response;
    try {
      lifecycle.start();
      response = client.go(
          new Request(HttpMethod.GET, new URL(requestURL)),
          new StatusResponseHandler(Charsets.UTF_8)
      ).get();
    }
    catch (Exception e) {
      throw new IOException(e instanceof ExecutionException ? e.getCause() : e);
    }
    finally {
      lifecycle.stop();
    }

    final List<LocatedSegmentDescriptor> segments = mapper.readValue(
        response.getContent(),
        new TypeReference<List<LocatedSegmentDescriptor>>()
        {
        }
    );
    if (segments == null || segments.size() == 0) {
      throw new IllegalStateException("No segments found to read");
    }

    logger.info("segments to read [%s]", segments);

    long maxSize = conf.getLong(CONF_MAX_SPLIT_SIZE, DEFAULT_MAX_SPLIT_SIZE);

    if (maxSize > 0) {
      Collections.shuffle(segments);
      for (LocatedSegmentDescriptor segment : segments) {
        maxSize = Math.max(maxSize, segment.getSize());
      }
    }

    List<DruidInputSplit> splits = Lists.newArrayList();

    List<LocatedSegmentDescriptor> currentGroup = new ArrayList<>();
    long currentSize = 0;

    for (LocatedSegmentDescriptor segment : segments) {
      if (maxSize < 0) {
        splits.add(toSplit(dataSource, filters, Arrays.asList(segment)));
        continue;
      }
      if (maxSize > 0 && currentSize + segment.getSize() > maxSize) {
        splits.add(toSplit(dataSource, filters, currentGroup));
        currentGroup.clear();
        currentSize = 0;
      }

      currentGroup.add(segment);
      currentSize += segment.getSize();
    }

    if (!currentGroup.isEmpty()) {
      splits.add(toSplit(dataSource, filters, currentGroup));
    }

    logger.info("Number of splits [%d]", splits.size());
    return splits.toArray(new DruidInputSplit[splits.size()]);
  }

  @Override
  public org.apache.hadoop.mapred.RecordReader getRecordReader(
      org.apache.hadoop.mapred.InputSplit split, JobConf job, Reporter reporter
  ) throws IOException
  {
    DruidRecordReader reader = new DruidRecordReader();
    reader.initialize((DruidInputSplit) split, job);
    return reader;
  }

  @Override
  public RecordReader<NullWritable, MapWritable> createRecordReader(
      InputSplit split,
      TaskAttemptContext context
  ) throws IOException, InterruptedException
  {
    return new DruidRecordReader();
  }

  private DruidInputSplit toSplit(String dataSource, String filters, List<LocatedSegmentDescriptor> segments)
  {
    long size = 0;
    List<Interval> intervals = Lists.newArrayList();
    for (LocatedSegmentDescriptor segment : segments) {
      size += segment.getSize();
      intervals.add(segment.getInterval());
    }
    String[] locations = getFrequentLocations(segments);
    return new DruidInputSplit(dataSource, intervals, filters, locations, size);
  }

  private String[] getFrequentLocations(List<LocatedSegmentDescriptor> segments)
  {
    List<String> locations = Lists.newArrayList();
    for (LocatedSegmentDescriptor segment : segments) {
      for (DruidServerMetadata location : segment.getLocations()) {
        locations.add(location.getHost());
      }
    }
    return getMostFrequentLocations(locations);
  }

  private static String[] getMostFrequentLocations(Iterable<String> hosts)
  {
    final CountingMap<String> counter = new CountingMap<>();
    for (String location : hosts) {
      counter.add(location, 1);
    }

    final TreeSet<Pair<Long, String>> sorted = Sets.<Pair<Long, String>>newTreeSet(
        new Comparator<Pair<Long, String>>()
        {
          @Override
          public int compare(Pair<Long, String> o1, Pair<Long, String> o2)
          {
            int compare = o2.lhs.compareTo(o1.lhs); // descending
            if (compare == 0) {
              compare = o1.rhs.compareTo(o2.rhs);   // ascending
            }
            return compare;
          }
        }
    );

    for (Map.Entry<String, AtomicLong> entry : counter.entrySet()) {
      sorted.add(Pair.of(entry.getValue().get(), entry.getKey()));
    }

    // use default replication factor, if possible
    final List<String> locations = Lists.newArrayListWithCapacity(3);
    for (Pair<Long, String> frequent : Iterables.limit(sorted, 3)) {
      locations.add(frequent.rhs);
    }
    return locations.toArray(new String[locations.size()]);
  }

  public static final class DruidInputSplit extends InputSplit implements org.apache.hadoop.mapred.InputSplit
  {
    private String dataSource;
    private String filters;
    private List<Interval> intervals;
    private String[] locations;
    private long length;

    //required for deserialization
    public DruidInputSplit()
    {
    }

    public DruidInputSplit(
        String dataSource,
        List<Interval> intervals,
        String filters,
        String[] locations,
        long length
    )
    {
      this.dataSource = dataSource;
      this.intervals = intervals;
      this.filters = filters;
      this.locations = locations;
      this.length = length;
    }

    @Override
    public long getLength()
    {
      return length;
    }

    @Override
    public String[] getLocations()
    {
      return locations;
    }

    public String getDataSource()
    {
      return dataSource;
    }

    public String getFilters()
    {
      return filters;
    }

    public List<Interval> getIntervals()
    {
      return intervals;
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
      out.writeUTF(dataSource);
      out.writeInt(intervals.size());
      for (String interval : Lists.transform(intervals, Functions.toStringFunction())) {
        out.writeUTF(interval);
      }
      out.writeUTF(Strings.nullToEmpty(filters));
      out.writeInt(locations.length);
      for (String location : locations) {
        out.writeUTF(location);
      }
      out.writeLong(length);
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
      dataSource = in.readUTF();
      intervals = Lists.newArrayList();
      for (int i = in.readInt(); i > 0; i--) {
        intervals.add(new Interval(in.readUTF()));
      }
      filters = in.readUTF();
      locations = new String[in.readInt()];
      for (int i = 0; i < locations.length; i++) {
        locations[i] = in.readUTF();
      }
      length = in.readLong();
    }

    @Override
    public String toString()
    {
      return "DruidInputSplit{" +
             "dataSource=" + dataSource +
             ", intervals=" + intervals +
             ", filters=" + filters +
             ", locations=" + Arrays.toString(locations) +
             '}';
    }
  }

  public static class DruidRecordReader extends RecordReader<NullWritable, MapWritable>
      implements org.apache.hadoop.mapred.RecordReader<NullWritable, MapWritable>
  {
    private final Lifecycle lifecycle = new Lifecycle();

    private int threshold;
    private ObjectMapper mapper;
    private HttpClient client;
    private Druids.SelectQueryBuilder builder;
    private Request request;

    private int intervalIndex;
    private List<Interval> intervals;
    private Iterator<EventHolder> events = Iterators.emptyIterator();
    private Map<String, Integer> paging = null;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException
    {
      initialize((DruidInputSplit) split, context.getConfiguration());
    }

    public void initialize(DruidInputSplit split, Configuration configuration) throws IOException
    {
      logger.info("Start loading " + split);

      List<HostAndPort> locations = Lists.transform(
          Arrays.asList(split.getLocations()),
          new Function<String, HostAndPort>()
          {
            @Override
            public HostAndPort apply(String input)
            {
              return HostAndPort.fromString(input);
            }
          }
      );

      String localHost = DruidNode.getDefaultHost();

      String location = split.getLocations()[0];
      for (HostAndPort hostAndPort : locations) {
        if (hostAndPort.getHostText().equals(localHost)) {
          location = "localhost:" + hostAndPort.getPort();
          break;
        }
      }

      intervals = split.getIntervals();

      client = HttpClientInit.createClient(HttpClientConfig.builder().build(), lifecycle);

      mapper = new DefaultObjectMapper();
      threshold = configuration.getInt(CONF_SELECT_THRESHOLD, DEFAULT_SELECT_THRESHOLD);

      builder = new Druids.SelectQueryBuilder()
          .dataSource(split.getDataSource())
          .granularity(QueryGranularities.ALL);

      final String timeColumn = configuration.get(CONF_DRUID_TIME_COLUMN_NAME, Column.TIME_COLUMN_NAME);

      List<DimensionSpec> dimensionSpecs = Lists.newArrayList();
      for (String column : configuration.get(CONF_DRUID_COLUMNS).split(",")) {
        column = column.trim();
        if (!column.equals(timeColumn)) {
          // use EventHolder.timestampKey for time
          dimensionSpecs.add(new DefaultDimensionSpec(column, column));
        }
      }

      builder.dimensionSpecs(dimensionSpecs);

      String filters = split.getFilters();
      if (filters != null && !filters.isEmpty()) {
        builder.filters(mapper.readValue(filters, DimFilter.class));
      }

      request = new Request(
          HttpMethod.POST,
          new URL(String.format("%s/druid/v2", "http://" + location))
      );

      logger.info("Using queryable node " + request.getUrl());

      try {
        lifecycle.start();
      }
      catch (Exception e) {
        throw new IOException(e);
      }

      future = submitQuery(true);
    }

    private transient Future<SelectResultValue> future;
    private transient StatusResponseHandler handler = new StatusResponseHandler(Charsets.UTF_8);

    private void nextPage() throws IOException, InterruptedException
    {
      SelectResultValue response = retrieveResult();
      if (response != null && !response.getEvents().isEmpty()) {
        events = response.iterator();
        paging = response.getPagingIdentifiers();
      } else {
        events = Iterators.emptyIterator();
      }
      future = submitQuery(!events.hasNext());
    }

    private SelectResultValue retrieveResult() throws InterruptedException, IOException
    {
      long start = System.currentTimeMillis();
      SelectResultValue response;
      try {
        response = future.get();
      }
      catch (ExecutionException e) {
        throw new IOException(e.getCause());
      }
      finally {
        logger.info("Waited on future in %d msec", System.currentTimeMillis() - start);
      }
      return response;
    }

    private Future<SelectResultValue> submitQuery(boolean nextInterval) throws IOException
    {
      if (nextInterval && intervalIndex >= intervals.size()) {
        return null;
      }
      if (nextInterval) {
        builder.intervals(new MultipleIntervalSegmentSpec(Arrays.asList(intervals.get(intervalIndex++))));
        builder.pagingSpec(new PagingSpec(null, threshold, true));
      } else {
        builder.pagingSpec(new PagingSpec(paging, threshold, true));
      }
      SelectQuery query = builder.build();
      logger.info("Submitting.. " + query.getPagingSpec());

      long start = System.currentTimeMillis();
      try {
        return wrapWithParse(
            client.go(
                request.setContent(mapper.writeValueAsBytes(query))
                       .setHeader(
                           HttpHeaders.Names.CONTENT_TYPE,
                           MediaType.APPLICATION_JSON
                       ),
                handler
            )
        );
      }
      finally {
        logger.info("Submitted in %d msec", System.currentTimeMillis() - start);
      }
    }

    private Future<SelectResultValue> wrapWithParse(ListenableFuture<StatusResponseHolder> holder)
    {
      return Futures.transform(
          holder,
          new Function<StatusResponseHolder, SelectResultValue>()
          {
            @Override
            public SelectResultValue apply(StatusResponseHolder input)
            {
              HttpResponseStatus status = input.getStatus();
              if (!status.equals(HttpResponseStatus.OK)) {
                throw new RuntimeException(input.getContent());
              }
              logger.info("Parsing %d bytes datum", input.getBuilder().length());

              long start = System.currentTimeMillis();
              List<Result<SelectResultValue>> value = parse(new BuilderReader(input.getBuilder()));
              if (value.isEmpty()) {
                return null;
              }
              SelectResultValue result = value.get(0).getValue();
              logger.info("Parsed %d rows in %d msec", result.getEvents().size(), System.currentTimeMillis() - start);
              return result;
            }
          }
      );
    }

    private List<Result<SelectResultValue>> parse(Reader content)
    {
      try {
        return mapper.readValue(
            content,
            new TypeReference<List<Result<SelectResultValue>>>()
            {
            }
        );
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException
    {
      while (future != null && !events.hasNext()) {
        nextPage();
      }
      return events.hasNext();
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException
    {
      return NullWritable.get();
    }

    @Override
    public MapWritable getCurrentValue() throws IOException, InterruptedException
    {
      try {
        return new MapWritable(events.next().getEvent());
      }
      finally {
        events.remove();
      }
    }

    @Override
    public float getProgress() throws IOException
    {
      return intervalIndex / intervals.size();
    }

    @Override
    public NullWritable createKey()
    {
      return NullWritable.get();
    }

    @Override
    public MapWritable createValue()
    {
      return new MapWritable();
    }

    @Override
    public boolean next(NullWritable key, MapWritable value) throws IOException
    {
      try {
        if (nextKeyValue()) {
          try {
            value.setValue(events.next().getEvent());
            return true;
          }
          finally {
            events.remove();
          }
        }
      }
      catch (InterruptedException e) {
        // ignore
      }
      return false;
    }

    @Override
    public long getPos() throws IOException
    {
      return 0;
    }

    @Override
    public void close() throws IOException
    {
      lifecycle.stop();
    }
  }
}
