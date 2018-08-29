/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.data.input;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.parsers.Parser;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.ParseSpec;
import io.druid.jackson.FunctionInitializer;
import io.druid.query.BaseQuery;
import io.druid.query.Query;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.dimension.DimensionSpecs;
import io.druid.query.filter.DimFilter;
import io.druid.segment.filter.Filters;
import org.joda.time.DateTime;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@JsonTypeName("requestLog")
public class RequestLogParseSpec implements ParseSpec
{
  private static final String LOG4J_PREFIX = "RequestLogger - ";
  private static final int LOG4J_PREFIX_LEN = LOG4J_PREFIX.length();

  private final ObjectMapper mapper;

  @JsonCreator
  public RequestLogParseSpec(@JacksonInject ObjectMapper mapper) {
    this.mapper = mapper;
  }

  @Override
  public TimestampSpec getTimestampSpec()
  {
    return new RelayTimestampSpec(Row.TIME_COLUMN_NAME);
  }

  @Override
  public DimensionsSpec getDimensionsSpec()
  {
    return DimensionsSpec.ofStringDimensions(
        Arrays.asList(
            "ip",
            "queryType",
            "dataSource",
            "filterColumns",
            "dimensions",
            "aggregators",
            "postAggregators",
            "success"
        )
    );
  }

  @Override
  public Parser<String, Object> makeParser()
  {
    final Map<Class, String> aggrMap = FunctionInitializer.resolveSubtypesAsInverseMap(
        mapper, AggregatorFactory.class);
    final Map<Class, String> postAggrMap = FunctionInitializer.resolveSubtypesAsInverseMap(
        mapper, PostAggregator.class
    );
    return new Parser<String, Object>()
    {
      @Override
      public Map<String, Object> parse(String input)
      {
        int index = input.indexOf(LOG4J_PREFIX);
        if (index < 0) {
          return null;
        }
        String target = input.substring(index + LOG4J_PREFIX_LEN);
        String[] splits = target.split("\\t");
        Map<String, Object> event = Maps.newHashMap();
        event.put(Row.TIME_COLUMN_NAME, new DateTime(splits[0].trim()));
        event.put("ip", splits[1].trim());

        Query query;
        Map result;
        try {
          query = mapper.readValue(splits[2], Query.class);
          result = mapper.readValue(splits[3], Map.class);
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
        event.put("queryType", query.getType());
        event.put("dataSource", query.getDataSource().getNames());
        DimFilter filter = BaseQuery.getDimFilter(query);
        if (filter != null) {
          event.put("filterColumns", Lists.newArrayList(Filters.getDependents(filter)));
        }
        if (query instanceof Query.AggregationsSupport) {
          Query.AggregationsSupport<?> aggregationQuery = (Query.AggregationsSupport) query;
          event.put("dimensions", DimensionSpecs.toDescriptions(aggregationQuery.getDimensions()));
          List<String> aggregators = Lists.newArrayList();
          for (AggregatorFactory factory : aggregationQuery.getAggregatorSpecs()) {
            aggregators.add(aggrMap.get(factory.getClass()));
          }
          event.put("aggregators", aggregators);
          List<String> postAggregators = Lists.newArrayList();
          for (PostAggregator postAggregator : aggregationQuery.getPostAggregatorSpecs()) {
            postAggregators.add(postAggrMap.get(postAggregator.getClass()));
          }
          event.put("postAggregators", postAggregators);
        }
        boolean success = (boolean) result.get("success");
        event.put("success", String.valueOf(success));
        event.put("queryTime", result.get("query/time"));
        if (success) {
          event.put("queryBytes", result.get("query/bytes"));
        } else {
          event.put("exception", result.get("exception"));
        }
        return event;
      }

      @Override
      public void setFieldNames(Iterable<String> fieldNames)
      {
        throw new UnsupportedOperationException("setFieldNames");
      }

      @Override
      public List<String> getFieldNames()
      {
        throw new UnsupportedOperationException("getFieldNames");
      }
    };
  }

  @Override
  public ParseSpec withTimestampSpec(TimestampSpec spec)
  {
    throw new UnsupportedOperationException("withTimestampSpec");
  }

  @Override
  public ParseSpec withDimensionsSpec(DimensionsSpec spec)
  {
    return this;
  }
}
