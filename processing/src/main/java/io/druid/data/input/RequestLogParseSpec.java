/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 SK Telecom Group Inc.
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
import io.druid.java.util.common.parsers.Parser;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.ParseSpec;
import io.druid.jackson.FunctionInitializer;
import io.druid.query.BaseAggregationQuery;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@JsonTypeName("requestLog")
public class RequestLogParseSpec implements ParseSpec
{
  private static final String REQUEST_PREFIX = "RequestLogger - ";
  private static final int REQUEST_PREFIX_LEN = REQUEST_PREFIX.length();

  private static final String MANAGER_PREFIX = "QueryManager - ";
  private static final int MANAGER_PREFIX_LEN = MANAGER_PREFIX.length();

  private static final Matcher MATCHER = Pattern.compile(
      "(\\d+) item\\(s\\) averaging (\\d+) msec\\.\\. mostly from \\[(.+)]").matcher("");
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
            "success",
            "slowHosts"
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
      private final Map<String, String> managerLog = Maps.newHashMap();

      @Override
      public Map<String, Object> parse(String input)
      {
        int index = input.indexOf(MANAGER_PREFIX);
        if (index > 0) {
          managerLog.put(input.substring(0, index).split(" ")[2], input.substring(index + MANAGER_PREFIX_LEN));
          return null;
        }
        index = input.indexOf(REQUEST_PREFIX);
        if (index < 0) {
          return null;
        }
        String target = input.substring(index + REQUEST_PREFIX_LEN);
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
        if (query instanceof BaseAggregationQuery) {
          BaseAggregationQuery aggregationQuery = (BaseAggregationQuery) query;
          event.put("hasWindowing", GuavaUtils.isNullOrEmpty(aggregationQuery.getLimitSpec().getWindowingSpecs()));
        }
        boolean success = (boolean) result.get("success");
        event.put("success", String.valueOf(success));
        event.put("queryTime", result.get("query/time"));
        if (success) {
          event.put("queryRows", result.get("query/rows"));
          event.put("queryBytes", result.get("query/bytes"));
        } else {
          event.put("exception", result.get("exception"));
        }
        String manager = managerLog.remove(input.substring(0, index).split(" ")[2]);
        if (manager != null && MATCHER.reset(manager).find()) {
          List<String> slowHosts = Lists.newArrayList();
          List<Long> slowTimes = Lists.newArrayList();
          for (String hostTime : MATCHER.group(3).split(", ")) {
            int x = hostTime.indexOf('=');
            if (x < 0) {
              continue;
            }
            String host = hostTime.substring(0, x).trim();
            if (host.charAt(host.length() - 1) == ')' && host.indexOf('(') > 0) {
              host = host.substring(0, host.indexOf('('));
            }
            slowHosts.add(host);
            slowTimes.add(Long.valueOf(hostTime.substring(x + 1, hostTime.length() - 2).trim()));
            if (slowHosts.size() >= 5) {
              break;
            }
          }
          event.put("slowHosts", slowHosts);
          event.put("slowTimes", slowTimes);
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
