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

package io.druid.query.aggregation.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.JodaUtils;
import io.druid.common.utils.Sequences;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;
import io.druid.granularity.Granularity;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Accumulator;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.logger.Logger;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Parser;
import io.druid.query.BaseAggregationQuery;
import io.druid.query.PostProcessingOperator;
import io.druid.query.Queries;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.PostAggregators;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.segment.ObjectArray;
import org.apache.commons.lang.mutable.MutableLong;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormatter;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 */
public abstract class PredictPostProcessor extends PostProcessingOperator.Abstract
{
  private static final Logger LOG = new Logger(PredictPostProcessor.class);

  private static final int DEFAULT_USE_LAST_N = 4096;
  private static final int DEFAULT_NUM_PREDICTION = 32;
  private static final int DEFAULT_CONFIDENCE = 95;

  final List<String> dimensions;
  final List<String> values;
  final int useLastN;
  final boolean keepValuesOnly;
  final int numPrediction;
  final int confidence;
  final List<PostAggregator> postAggregations;
  private final String timeExpression;
  private final String timeColumn;
  private final String timeLocale;
  private final String timeZone;
  final Granularity timeGranularity;

  final Function<Row, DateTime> timeFunction;

  public PredictPostProcessor(
      List<String> dimensions,
      List<String> values,
      boolean keepValuesOnly,
      Integer useLastN,
      Integer numPrediction,
      Integer confidence,
      List<PostAggregator> postAggregations,
      String timeExpression,
      String timeColumn,
      String timeFormat,
      String timeLocale,
      String timeZone,
      Granularity timeGranularity
  )
  {
    this.dimensions = dimensions == null ? ImmutableList.<String>of() : dimensions;
    this.values = Preconditions.checkNotNull(values, "'columns' cannot be null");
    this.keepValuesOnly = keepValuesOnly;
    this.useLastN = useLastN == null ? DEFAULT_USE_LAST_N : useLastN;
    this.numPrediction = numPrediction == null ? DEFAULT_NUM_PREDICTION : numPrediction;
    this.confidence = confidence == null ? DEFAULT_CONFIDENCE : confidence;
    this.postAggregations = postAggregations;
    this.timeExpression = timeExpression;
    this.timeColumn = timeColumn;
    this.timeLocale = timeLocale;
    this.timeZone = timeZone;
    this.timeGranularity = timeGranularity;
    Preconditions.checkArgument(
        timeGranularity == null && timeColumn == null && timeFormat == null && timeExpression == null ||
        timeGranularity != null && timeColumn != null && timeFormat != null && timeExpression == null ||
        timeGranularity != null && timeColumn == null && timeFormat == null && timeExpression != null
    );
    if (timeExpression != null) {
      final Expr expr = Parser.parse(timeExpression);
      timeFunction = new Function<Row, DateTime>()
      {
        @Override
        public DateTime apply(Row input)
        {
          return expr.eval(input).asDateTime();
        }
      };
    } else if (timeFormat != null) {
      final DateTimeFormatter formatter = JodaUtils.toTimeFormatter(timeFormat, timeZone, timeLocale);
      timeFunction = new Function<Row, DateTime>()
      {
        @Override
        public DateTime apply(Row input)
        {
          return formatter.parseDateTime(Objects.toString(input.getRaw(timeColumn), null));
        }
      };
    } else {
      timeFunction = new Function<Row, DateTime>()
      {
        @Override
        public DateTime apply(Row input)
        {
          return input.getTimestamp();
        }
      };
    }
  }

  @Override
  public QueryRunner postProcess(final QueryRunner baseRunner)
  {
    return new QueryRunner()
    {
      private final List<PostAggregator.Processor> postProcessors = PostAggregators.toProcessors(postAggregations);

      @Override
      @SuppressWarnings("unchecked")
      public Sequence run(Query query, Map responseContext)
      {
        final String[] valueColumns = values.toArray(new String[0]);
        if (query instanceof BaseAggregationQuery) {
          final BaseAggregationQuery aggregation = (BaseAggregationQuery) query;
          final Granularity granularity = Optional.fromNullable(timeGranularity).or(aggregation.getGranularity());
          final List<String> copy = Lists.newArrayList(dimensions);
          copy.retainAll(DimensionSpecs.toOutputNames(aggregation.getDimensions()));
          final String[] dimensions = copy.toArray(new String[0]);

          final Map<ObjectArray<Object>, BoundedTimeseries[]> numbersMap = Maps.newHashMap();
          final MutableLong lastTimestamp = new MutableLong();
          final Sequence<Row> sequence = Queries.convertToRow(
              aggregation,
              baseRunner.run(aggregation, responseContext)
          );

          Sequence<Row> tapping = new Sequences.PeekingSequence<Row>(sequence)
          {
            @Override
            protected final Row peek(final Row in)
            {
              final DateTime timestamp = Optional.fromNullable(timeFunction.apply(in)).or(in.getTimestamp());
              for (int i = 0; i < valueColumns.length; i++) {
                final Object[] tags = new Object[dimensions.length];
                for (int d = 0; d < dimensions.length; d++) {
                  tags[d] = internIfString(in.getRaw(dimensions[d]));
                }
                final ObjectArray key = new ObjectArray(tags);
                final BoundedTimeseries[] values = numbersMap.computeIfAbsent(key, k -> makeReservoir(valueColumns.length, granularity));
                final Object value = in.getRaw(valueColumns[i]);
                if (value instanceof Number) {
                  values[i].add(((Number) value).doubleValue(), timestamp.getMillis());
                }
              }
              lastTimestamp.setValue(timestamp.getMillis());
              if (!keepValuesOnly && postProcessors.isEmpty()) {
                return in;
              }
              Map<String, Object> event = Rows.asMap(in);
              if (keepValuesOnly) {
                event = Rows.retain(event, values);
              }
              for (PostAggregator.Processor postAggregator : postProcessors) {
                event.put(postAggregator.getName(), postAggregator.compute(timestamp, event));
              }
              return new MapBasedRow(in.getTimestamp(), event);
            }
          };

          Supplier<Sequence<Row>> supplier = new Supplier<Sequence<Row>>()
          {
            @Override
            public Sequence<Row> get()
            {
              LOG.info("Calculating %d predictions.. ", numPrediction);
              return Sequences.simple(
                  makeRowedPrediction(
                      valueColumns,
                      dimensions,
                      numbersMap,
                      lastTimestamp.longValue(),
                      granularity,
                      postProcessors
                  )
              );
            }
          };
          return Sequences.concat(Arrays.asList(tapping, Sequences.lazy(supplier)));
        } else if (query instanceof Query.ArrayOutputSupport && query.estimatedOutputColumns() != null) {
          final List<String> outputColumns = query.estimatedOutputColumns();
          final int timeIdx = outputColumns.indexOf(Row.TIME_COLUMN_NAME);
          final int[] indices = GuavaUtils.indexOf(outputColumns, values);
          final Granularity granularity = query.getGranularity();
          // this is used for quick calculation of prediction only
          final BoundedTimeseries[] numbers = makeReservoir(valueColumns.length, granularity);
          ((Query.ArrayOutputSupport) query).array(baseRunner.run(query, responseContext)).accumulate(
              null, new Accumulator<Object, Object[]>()
              {
                @Override
                public Object accumulate(Object accumulated, Object[] in)
                {
                  final long timestamp = ((Number) in[timeIdx]).longValue();
                  for (int i = 0; i < indices.length; i++) {
                    if (indices[i] >= 0 && in[indices[i]] instanceof Number) {
                      numbers[i].add(((Number) in[indices[i]]).doubleValue(), timestamp);
                    }
                  }
                  return null;
                }
              }
          );
          return Sequences.simple(Arrays.asList(makeArrayedPrediction(valueColumns, numbers)));

        } else {
          LOG.info("Not supported query type " + query.getType());
          return baseRunner.run(query, responseContext);
        }
      }
    };
  }

  @SuppressWarnings("unchecked")
  private BoundedTimeseries[] makeReservoir(int length, Granularity granularity)
  {
    final BoundedTimeseries[] numbers = (BoundedTimeseries[]) Array.newInstance(
        BoundedTimeseries.class,
        values.size()
    );
    for (int i = 0; i < length; i++) {
      numbers[i] = new BoundedTimeseries(useLastN, granularity);
    }
    return numbers;
  }

  private Object internIfString(Object value)
  {
    return value instanceof String ? ((String) value).intern() : value;
  }

  protected abstract Map<String, Object> makeArrayedPrediction(String[] columnNames, BoundedTimeseries[] numbers);

  @SuppressWarnings("unchecked")
  protected abstract List<Row> makeRowedPrediction(
      String[] metrics,
      String[] dimensions,
      Map<ObjectArray<Object>, BoundedTimeseries[]> numbersMap,
      long lastTimestamp,
      Granularity granularity,
      List<PostAggregator.Processor> postProcessors
  );

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<String> getDimensions()
  {
    return dimensions;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<String> getValues()
  {
    return values;
  }

  @JsonProperty
  public int getUseLastN()
  {
    return useLastN;
  }

  @JsonProperty
  public boolean isKeepValuesOnly()
  {
    return keepValuesOnly;
  }

  @JsonProperty
  public int getNumPrediction()
  {
    return numPrediction;
  }

  @JsonProperty
  public int getConfidence()
  {
    return confidence;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<PostAggregator> getPostAggregations()
  {
    return postAggregations;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getTimeExpression()
  {
    return timeExpression;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getTimeColumn()
  {
    return timeColumn;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getTimeLocale()
  {
    return timeLocale;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getTimeZone()
  {
    return timeZone;
  }

  @JsonProperty
  public Granularity getTimeGranularity()
  {
    return timeGranularity;
  }

  static class BoundedTimeseries
  {
    private final int limit;
    private double[] values;
    private int index;
    private boolean exceeded;

    private final Granularity granularity;
    private long prevTimestamp;
    private Long bucketEnd;

    BoundedTimeseries(int limit, Granularity granularity)
    {
      this.limit = Math.max(limit, 32);
      this.values = new double[32];
      this.granularity = granularity;
    }

    public void add(double value, long currentTime)
    {
      if (bucketEnd == null || bucketEnd == currentTime) {
        bucketEnd = granularity.bucketEnd(currentTime);
      } else if (currentTime > bucketEnd) {
        final int count = countEmptyTo(currentTime);
        if (count > 0) {
          double lastValue = lastValue();
          double delta = (value - lastValue) / count;
          for (int i = 0; i < count; i++) {
            _add(lastValue + delta * i);  // linear...
          }
        }
        bucketEnd = granularity.bucketEnd(currentTime);
      } else if (currentTime < prevTimestamp) {
        throw new ISE("Input data is not time-sorted");
      }
      prevTimestamp = currentTime;
      _add(value);
    }

    private int countEmptyTo(long currentTime)
    {
      final long newStart = granularity.bucketStart(currentTime);
      if (newStart == bucketEnd) {
        return 0;
      }
      return Iterables.size(granularity.getIterable(new Interval(bucketEnd, newStart)));
    }

    private double lastValue()
    {
      return index > 0 ? values[index - 1] : values[values.length - 1];
    }

    private void _add(double value)
    {
      if (values.length < limit && index == values.length - 1) {
        double[] extended = new double[Math.min(limit, values.length * 2)];
        System.arraycopy(values, 0, extended, 0, values.length);
        values = extended;
      }
      values[index++] = value;
      index = index % values.length;
      exceeded |= index == 0;
    }

    public int size()
    {
      return exceeded ? values.length : index;
    }

    public double[] asArray()
    {
      if (!exceeded) {
        return Arrays.copyOfRange(values, 0, index);
      }
      if (index == 0) {
        return values;
      }
      final double[] result = new double[values.length];
      System.arraycopy(values, index, result, 0, values.length - index);
      System.arraycopy(values, 0, result, values.length - index, index);
      return result;
    }
  }

  // just to deliver marking
  public static class PredictedRow extends MapBasedRow
  {
    public PredictedRow(DateTime timestamp, Map<String, Object> event)
    {
      super(timestamp, event);
    }
  }
}
