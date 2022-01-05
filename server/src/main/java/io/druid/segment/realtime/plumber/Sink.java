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

package io.druid.segment.realtime.plumber;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import io.druid.data.input.InputRow;
import io.druid.java.util.common.ISE;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.QueryableIndex;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.IndexSizeExceededException;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.GranularitySpec;
import io.druid.segment.realtime.FireHydrant;
import io.druid.segment.realtime.appenderator.AppenderatorConfig;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;

import javax.annotation.concurrent.GuardedBy;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class Sink implements Iterable<FireHydrant>
{
  private static final int ADD_FAILED = -1;

  private final Object hydrantLock = new Object();
  private final Interval interval;
  private final DataSchema schema;
  private final ShardSpec shardSpec;
  private final String version;
  private final int maxRowsInMemory;
  private final boolean ignoreInvalidRows;
  private final boolean reportParseExceptions;
  private final CopyOnWriteArrayList<FireHydrant> hydrants = new CopyOnWriteArrayList<FireHydrant>();
  private final LinkedHashSet<String> dimOrder = Sets.newLinkedHashSet();
  private final AtomicInteger numRowsExcludingCurrIndex = new AtomicInteger();
  private final DataSegment template;

  private final Map<String, Map<String, String>> columnDescs;
  private final ObjectMapper mapper;

  @GuardedBy("hydrantLock")
  private boolean writable = true;

  @GuardedBy("hydrantLock")
  private FireHydrant currHydrant;  // cannot be null after object init

  private long lastAccessTime = 0;

  public Sink(
      Interval interval,
      DataSchema schema,
      ShardSpec shardSpec,
      String version,
      AppenderatorConfig tuningConfig,
      ObjectMapper objectMapper
  )
  {
    this(interval, schema, shardSpec, version, tuningConfig, ImmutableList.<FireHydrant>of(), objectMapper);
  }

  public Sink(
      Interval interval,
      DataSchema schema,
      ShardSpec shardSpec,
      String version,
      AppenderatorConfig tuningConfig,
      List<FireHydrant> hydrants,
      ObjectMapper objectMapper
  )
  {
    this.schema = schema;
    this.shardSpec = shardSpec;
    this.interval = interval;
    this.version = version;
    this.maxRowsInMemory = tuningConfig.getMaxRowsInMemory();
    this.ignoreInvalidRows = tuningConfig.isIgnoreInvalidRows();
    this.reportParseExceptions = tuningConfig.isReportParseExceptions();
    this.columnDescs = tuningConfig.getIndexSpec().getColumnDescriptors();

    int maxCount = -1;
    for (int i = 0; i < hydrants.size(); ++i) {
      final FireHydrant hydrant = hydrants.get(i);
      if (hydrant.getCount() <= maxCount) {
        throw new ISE("hydrant[%s] not the right count[%s]", hydrant, i);
      }
      maxCount = hydrant.getCount();
      numRowsExcludingCurrIndex.addAndGet(hydrant.getSegment().asQueryableIndex(false).getNumRows());
    }
    this.hydrants.addAll(hydrants);
    this.mapper = objectMapper;
    this.template = new DataSegment(
        schema.getDataSource(),
        interval,
        version,
        ImmutableMap.of(),
        ImmutableList.of(),
        ImmutableList.of(),
        shardSpec,
        null,
        0
    );

    makeNewCurrIndex(interval.getStartMillis());
  }

  public String getVersion()
  {
    return version;
  }

  public int getPartitionNum()
  {
    return shardSpec == null ? 0 : shardSpec.getPartitionNum();
  }

  public Interval getInterval()
  {
    return interval;
  }

  public FireHydrant getCurrHydrant()
  {
    synchronized (hydrantLock) {
      return currHydrant;
    }
  }

  public long getLastAccessTime()
  {
    return lastAccessTime;
  }

  public int add(InputRow row) throws IndexSizeExceededException
  {
    lastAccessTime = System.currentTimeMillis();

    synchronized (hydrantLock) {
      if (!writable) {
        return ADD_FAILED;
      }

      IncrementalIndex index = currHydrant.getIndex();
      if (index == null) {
        return ADD_FAILED; // the hydrant was swapped without being replaced
      }
      return index.add(row);
    }
  }

  public boolean canAppendRow()
  {
    synchronized (hydrantLock) {
      return writable && currHydrant.canAppendRow();
    }
  }

  public int rowCountInMemory()
  {
    synchronized (hydrantLock) {
      return currHydrant.rowCountInMemory();
    }
  }

  public long occupationInMemory()
  {
    synchronized (hydrantLock) {
      return currHydrant.estimatedOccupation();
    }
  }

  public boolean isWritable()
  {
    synchronized (hydrantLock) {
      return writable;
    }
  }

  public void finishWriting()
  {
    synchronized (hydrantLock) {
      writable = false;
    }
  }

  /**
   * If currHydrant is A, creates a new index B, sets currHydrant to B and returns A.
   *
   * @return the current index after swapping in a new one. can be null
   */
  public FireHydrant swap()
  {
    return makeNewCurrIndex(interval.getStartMillis());
  }

  public boolean swappable()
  {
    synchronized (hydrantLock) {
      return writable && currHydrant.rowCountInMemory() > 0;
    }
  }

  public DataSegment getSegment()
  {
    return template;
  }

  public String getIdentifier()
  {
    return template.getIdentifier();
  }

  public int getNumRows()
  {
    synchronized (hydrantLock) {
      return numRowsExcludingCurrIndex.get() + getNumRowsInMemory();
    }
  }

  public int getNumRowsInMemory()
  {
    synchronized (hydrantLock) {
      IncrementalIndex index = currHydrant.getIndex();
      if (index == null) {
        return 0;
      }

      return currHydrant.getIndex().size();
    }
  }

  private FireHydrant makeNewCurrIndex(long minTimestamp)
  {
    final GranularitySpec granularitySpec = schema.getGranularitySpec();
    final IncrementalIndexSchema indexSchema = new IncrementalIndexSchema.Builder()
        .withMinTimestamp(minTimestamp)
        .withQueryGranularity(granularitySpec.getQueryGranularity())
        .withSegmentGranularity(granularitySpec.getSegmentGranularity())
        .withDimensionsSpec(schema.getParser(mapper, ignoreInvalidRows))
        .withDimensionFixed(schema.isDimensionFixed())
        .withMetrics(schema.getAggregators())
        .withRollup(granularitySpec.isRollup())
        .withColumnDescs(columnDescs)
        .build();

    final FireHydrant old;
    synchronized (hydrantLock) {
      if (!writable) {
        // Oops, someone called finishWriting while we were making this new index.
        throw new ISE("finishWriting() called during swap");
      }

      old = currHydrant;
      if (old != null && old.rowCountInMemory() == 0) {
        return null;
      }

      final IncrementalIndex newIndex = new OnheapIncrementalIndex(
          indexSchema, reportParseExceptions, maxRowsInMemory
      );

      int newCount = 0;
      if (old != null) {
        newCount = old.getCount() + 1;
        if (!indexSchema.getDimensionsSpec().hasCustomDimensions()) {
          IncrementalIndex oldIndex = old.getIndex();
          if (oldIndex == null) {
            QueryableIndex storedIndex = old.getSegment().asQueryableIndex(false);
            for (String dim : storedIndex.getAvailableDimensions()) {
              dimOrder.add(dim);
            }
          } else {
            dimOrder.addAll(oldIndex.getDimensionOrder());
          }
          newIndex.loadDimensionIterable(dimOrder);
        }
        numRowsExcludingCurrIndex.addAndGet(old.rowCountInMemory());
      }
      DataSegment segment = template.withDimensionsMetrics(newIndex.getDimensionNames(), newIndex.getMetricNames());
      currHydrant = new FireHydrant(new IncrementalIndexSegment(newIndex, segment), newCount);
      hydrants.add(currHydrant);
    }

    return old;
  }

  @Override
  public Iterator<FireHydrant> iterator()
  {
    return Iterators.filter(
        hydrants.iterator(),
        new Predicate<FireHydrant>()
        {
          @Override
          public boolean apply(FireHydrant input)
          {
            final IncrementalIndex index = input.getIndex();
            return index == null || index.size() != 0;
          }
        }
    );
  }

  @Override
  public String toString()
  {
    return "Sink{" +
           "interval=" + interval +
           ", schema=" + schema +
           '}';
  }
}
