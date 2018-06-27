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
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.smile.SmileGenerator;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metamx.common.Pair;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.HttpClientConfig;
import com.metamx.http.client.HttpClientInit;
import com.metamx.http.client.Request;
import com.metamx.http.client.io.AppendableByteArrayInputStream;
import com.metamx.http.client.response.ClientResponse;
import com.metamx.http.client.response.HttpResponseHandler;
import com.metamx.http.client.response.InputStreamResponseHandler;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.client.JsonParserIterator;
import io.druid.client.StreamHandler;
import io.druid.client.StreamHandlerFactory;
import io.druid.collections.CountingMap;
import io.druid.common.utils.StringUtils;
import io.druid.granularity.QueryGranularities;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.jackson.DruidDefaultSerializersModule;
import io.druid.query.BaseQuery;
import io.druid.query.Druids;
import io.druid.query.LocatedSegmentDescriptor;
import io.druid.query.Query;
import io.druid.query.Result;
import io.druid.query.SegmentDescriptor;
import io.druid.query.filter.DimFilter;
import io.druid.query.select.EventHolder;
import io.druid.query.select.PagingSpec;
import io.druid.query.select.SelectQuery;
import io.druid.query.select.SelectResultValue;
import io.druid.query.select.StreamQuery;
import io.druid.query.spec.MultipleSpecificSegmentSpec;
import io.druid.query.spec.SpecificSegmentSpec;
import io.druid.server.DruidNode;
import io.druid.server.coordination.DruidServerMetadata;
import org.apache.commons.io.IOUtils;
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
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.joda.time.Interval;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
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
import java.util.UUID;
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
  public static final String CONF_DRUID_COLUMNS_UPPERCASE = "druid.columns.uppercase";
  public static final String CONF_DRUID_FILTERS = "druid.filters";
  public static final String CONF_DRUID_TIME_COLUMN_NAME = "druid.time.column.name";

  public static final String CONF_DRUID_MAX_SPLIT_SIZE = "druid.max.split.size";
  public static final String CONF_DRUID_REVERT_CAST = "druid.revert.cast";  // apply cast on const, instead of column

  public static final String CONF_DRUID_USE_STREAM = "druid.use.stream";
  public static final String CONF_DRUID_SELECT_THRESHOLD = "druid.select.threshold";
  public static final String CONF_DRUID_STREAM_THRESHOLD = "druid.stream.threshold";

  public static final int DEFAULT_SELECT_THRESHOLD = 10000;   // page num
  public static final int DEFAULT_STREAM_THRESHOLD = 10;      // chunk num
  public static final int DEFAULT_MAX_SPLIT_SIZE = -1;        // -1 : split per segment

  public static final String DEFAULT_INTERVAL = "0000-01-01/3000-01-01";

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
    String intervals = conf.get(CONF_DRUID_INTERVALS, DEFAULT_INTERVAL);
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
      logger.info("No segments found to read");
      return new DruidInputSplit[0];
    }

    logger.info("segments to read [%s]", segments);

    long maxSize = conf.getLong(CONF_DRUID_MAX_SPLIT_SIZE, DEFAULT_MAX_SPLIT_SIZE);

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
  @SuppressWarnings("unchecked")
  public org.apache.hadoop.mapred.RecordReader getRecordReader(
      org.apache.hadoop.mapred.InputSplit split, JobConf job, Reporter reporter
  ) throws IOException
  {
    DruidRecordReader<?, ?> reader = DruidRecordReader.create(job);
    reader.initialize((DruidInputSplit) split, job);
    return reader;
  }

  @Override
  public RecordReader<NullWritable, MapWritable> createRecordReader(
      InputSplit split,
      TaskAttemptContext context
  ) throws IOException, InterruptedException
  {
    DruidRecordReader<?, ?> reader = DruidRecordReader.create(context.getConfiguration());
    reader.initialize((DruidInputSplit) split, context.getConfiguration());
    return reader;
  }

  private DruidInputSplit toSplit(String dataSource, String filters, List<LocatedSegmentDescriptor> locatedSegments)
  {
    long size = 0;
    List<SegmentDescriptor> intervals = Lists.newArrayList();
    for (LocatedSegmentDescriptor segment : locatedSegments) {
      size += segment.getSize();
      intervals.add(segment.toSegmentDescriptor());
    }
    String[] locations = getFrequentLocations(locatedSegments);
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
    private List<SegmentDescriptor> segments;
    private String[] locations;
    private long length;

    //required for deserialization
    public DruidInputSplit()
    {
    }

    public DruidInputSplit(
        String dataSource,
        List<SegmentDescriptor> segments,
        String filters,
        String[] locations,
        long length
    )
    {
      this.dataSource = dataSource;
      this.segments = segments;
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

    public List<SegmentDescriptor> getSegments()
    {
      return segments;
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
      out.writeUTF(dataSource);
      out.writeInt(segments.size());
      for (SegmentDescriptor segment : segments) {
        out.writeUTF(segment.getInterval().toString());
        out.writeUTF(segment.getVersion());
        out.writeInt(segment.getPartitionNumber());
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
      segments = Lists.newArrayList();
      for (int i = in.readInt(); i > 0; i--) {
        segments.add(new SegmentDescriptor(new Interval(in.readUTF()), in.readUTF(), in.readInt()));
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
             ", segments=" + segments +
             ", filters=" + filters +
             ", locations=" + Arrays.toString(locations) +
             '}';
    }
  }

  public static abstract class DruidRecordReader<F, T> extends RecordReader<NullWritable, MapWritable>
      implements org.apache.hadoop.mapred.RecordReader<NullWritable, MapWritable>
  {
    public static DruidRecordReader<?, ?> create(Configuration configuration)
    {
      if (configuration.getBoolean(CONF_DRUID_USE_STREAM, true)) {
        return new DruidRecordReader.WithStreaming();
      }
      return new DruidRecordReader.WithPaging();
    }

    final Lifecycle lifecycle = new Lifecycle();

    ObjectMapper mapper;
    HttpClient client;
    Request request;

    String timeColumn;
    Druids.SelectQueryBuilder builder;

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

      client = HttpClientInit.createClient(HttpClientConfig.builder().build(), lifecycle);

      SmileFactory smileFactory = new SmileFactory();
      smileFactory.configure(SmileGenerator.Feature.ENCODE_BINARY_AS_7BIT, false);
      smileFactory.delegateToTextual(true);

      mapper = new DefaultObjectMapper(smileFactory);

      List<String> columns = Arrays.asList(configuration.get(CONF_DRUID_COLUMNS).split(","));
      if (configuration.getBoolean(CONF_DRUID_COLUMNS_UPPERCASE, false)) {
        columns = Lists.newArrayList(Lists.transform(columns, StringUtils.TO_UPPER));
      }

      builder = new Druids.SelectQueryBuilder()
          .dataSource(split.getDataSource())
          .columns(columns)
          .granularity(QueryGranularities.ALL)
          .context(ImmutableMap.<String, Object>of(
                       BaseQuery.ALL_DIMENSIONS_FOR_EMPTY, false,
                       BaseQuery.ALL_METRICS_FOR_EMPTY, false
                   )
          );
      String filters = split.getFilters();
      if (filters != null && !filters.isEmpty()) {
        builder = builder.filters(mapper.readValue(filters, DimFilter.class));
      }

      timeColumn = configuration.get(CONF_DRUID_TIME_COLUMN_NAME, EventHolder.timestampKey);

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
    }

    Iterator<Map<String, Object>> events = Iterators.emptyIterator();

    protected final ListenableFuture<F> submitQuery(Query query, HttpResponseHandler<?, F> handler) throws IOException
    {
      logger.info("Submitting.. %s", query);
      long start = System.currentTimeMillis();
      try {
        return client.go(
            request.setContent(mapper.writeValueAsBytes(query))
                   .setHeader(HttpHeaders.Names.CONTENT_TYPE, SmileMediaTypes.APPLICATION_JACKSON_SMILE),
            handler
        );
      }
      finally {
        logger.info("Submitted in %d msec", System.currentTimeMillis() - start);
      }
    }

    protected final T retrieveResult(Future<T> future) throws IOException
    {
      long start = System.currentTimeMillis();
      try {
        return future.get();
      }
      catch (ExecutionException e) {
        throw new IOException(e.getCause());
      }
      catch (InterruptedException e) {
        throw new InterruptedIOException(e.toString());
      }
      finally {
        logger.info("Waited on future in %d msec", System.currentTimeMillis() - start);
      }
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException
    {
      return NullWritable.get();
    }

    @Override
    public MapWritable getCurrentValue() throws IOException, InterruptedException
    {
      return new MapWritable(events.next());
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
    public final boolean next(NullWritable key, MapWritable value) throws IOException
    {
      try {
        if (nextKeyValue()) {
          value.setValue(events.next());
          return true;
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
    public float getProgress() throws IOException
    {
      return 0;
    }

    @Override
    public void close() throws IOException
    {
      lifecycle.stop();
    }

    public static class WithPaging extends DruidRecordReader<InputStream, SelectResultValue>
    {
      private int segmentIndex;
      private List<SegmentDescriptor> segments;

      private Future<SelectResultValue> future;
      private Map<String, Integer> paging = null;
      private int threshold;
      private JavaType type;

      @Override
      public void initialize(DruidInputSplit split, Configuration configuration) throws IOException
      {
        super.initialize(split, configuration);
        this.threshold = configuration.getInt(CONF_DRUID_SELECT_THRESHOLD, DEFAULT_SELECT_THRESHOLD);
        this.segments = split.getSegments();
        this.type = mapper.getTypeFactory().constructType(
            new TypeReference<List<Result<SelectResultValue>>>()
            {
            }
        );
        this.future = submitQuery(true);
      }

      private void nextPage() throws IOException
      {
        SelectResultValue response = retrieveResult(future);
        if (response != null && !response.getEvents().isEmpty()) {
          events = Iterators.transform(response.iterator(), EventHolder.EVENT_EXT);
          paging = response.getPagingIdentifiers();
        } else {
          events = Iterators.emptyIterator();
        }
        future = submitQuery(!events.hasNext());
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
      public float getProgress() throws IOException
      {
        return segmentIndex / segments.size();
      }

      // make it blocking
      final InputStreamResponseHandler handler = new InputStreamResponseHandler()
      {
        @Override
        public ClientResponse<AppendableByteArrayInputStream> handleResponse(HttpResponse response)
        {
          return ClientResponse.unfinished(super.handleResponse(response).getObj());
        }
      };

      private Future<SelectResultValue> submitQuery(boolean nextSegment) throws IOException
      {
        if (nextSegment && segmentIndex >= segments.size()) {
          return null;
        }
        if (nextSegment) {
          builder.intervals(new SpecificSegmentSpec(segments.get(segmentIndex++)));
          builder.pagingSpec(new PagingSpec(null, threshold, true));
        } else {
          builder.pagingSpec(new PagingSpec(paging, threshold, true));
        }
        SelectQuery query = builder.build();
        return Futures.transform(
            submitQuery(query, handler),
            new Function<InputStream, SelectResultValue>()
            {
              @Override
              public SelectResultValue apply(InputStream input)
              {
                long start = System.currentTimeMillis();
                List<Result<SelectResultValue>> value = parse(input);
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

      private List<Result<SelectResultValue>> parse(InputStream content)
      {
        try {
          return mapper.readValue(content, type);
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    }

    public static class WithStreaming extends DruidRecordReader<InputStream, Iterator<Map<String, Object>>>
    {
      private StreamHandler streamHandler;

      @Override
      public void initialize(DruidInputSplit split, Configuration configuration) throws IOException
      {
        super.initialize(split, configuration);
        Map<String, Object> tabularProcessor = ImmutableMap.<String, Object>of(
            BaseQuery.QUERYID, UUID.randomUUID().toString(),
            BaseQuery.POST_PROCESSING, ImmutableMap.of("type", "tabular", "timestampColumn", timeColumn)
        );
        StreamQuery query = builder.intervals(new MultipleSpecificSegmentSpec(split.getSegments()))
                                   .addContext(tabularProcessor)
                                   .streaming();
        int threshold = configuration.getInt(CONF_DRUID_STREAM_THRESHOLD, DEFAULT_STREAM_THRESHOLD);
        streamHandler = new StreamHandlerFactory(logger, mapper).create(
            query.getId(), request.getUrl(), threshold
        );
        events = new JsonParserIterator<Map<String, Object>>(
            mapper,
            mapper.getTypeFactory().constructType(
                new TypeReference<Map<String, Object>>()
                {
                }
            ),
            submitQuery(query, streamHandler),
            request.getUrl(),
            null
        );
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException
      {
        return events.hasNext();
      }

      @Override
      public void close() throws IOException
      {
        IOUtils.closeQuietly(streamHandler);
        super.close();
      }
    }
  }
}
