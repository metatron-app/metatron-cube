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

package io.druid.query.sketch;

import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import io.druid.query.CacheStrategy;
import io.druid.query.DefaultQueryRunnerFactoryConglomerate;
import io.druid.query.JoinQueryConfig;
import io.druid.query.Query;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.Result;
import io.druid.query.TableDataSource;
import io.druid.segment.TestHelper;
import io.druid.segment.TestIndex;
import io.druid.sql.calcite.util.TestQuerySegmentWalker;
import org.junit.Assert;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 */
public class SketchQueryRunnerTestHelper extends QueryRunnerTestHelper
{
  protected static final TestQuerySegmentWalker segmentWalker;

  protected static final ObjectMapper JSON_MAPPER;

  static {
    ObjectMapper mapper = TestHelper.JSON_MAPPER.copy();
    mapper.setInjectableValues(
        new InjectableValues()
        {
          @Override
          public Object findInjectableValue(
              Object valueId, DeserializationContext ctxt, BeanProperty forProperty, Object beanInstance
          )
          {
            if (valueId.equals(QuerySegmentWalker.class.getName())) {
              return segmentWalker;
            } else if (valueId.equals(ExecutorService.class.getName())) {
              return segmentWalker.getExecutor();
            } else if (valueId.equals(QueryToolChestWarehouse.class.getName())) {
              return segmentWalker;
            } else if (valueId.equals(JoinQueryConfig.class.getName())) {
              return segmentWalker.getQueryConfig().getJoin();
            }
            return null;
          }
        }
    );
    JSON_MAPPER = mapper;
  }

  static final SketchQueryQueryToolChest toolChest = new SketchQueryQueryToolChest(
      QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
  );

  static {
    QueryRunnerFactoryConglomerate conglomerate = TestIndex.segmentWalker.getQueryRunnerFactoryConglomerate();
    SketchQuery dummy = new SketchQuery(TableDataSource.of("dummy"), null, null, null, null, null, null, null, null);
    if (conglomerate.findFactory(dummy) == null) {
      Map<Class<? extends Query>, QueryRunnerFactory> factoryMap = Maps.newHashMap(
          ((DefaultQueryRunnerFactoryConglomerate) conglomerate).getFactories()
      );
      factoryMap.put(
          SketchQuery.class,
          new SketchQueryRunnerFactory(toolChest, QueryRunnerTestHelper.NOOP_QUERYWATCHER)
      );
      conglomerate = new DefaultQueryRunnerFactoryConglomerate(factoryMap);
    }
    segmentWalker = TestIndex.segmentWalker.withConglomerate(conglomerate).withObjectMapper(JSON_MAPPER);
  }

  @Override
  protected TestQuerySegmentWalker getSegmentWalker()
  {
    return segmentWalker;
  }

  public static Object[] array(Object... objects)
  {
    return objects;
  }

  public static List list(Object... objects)
  {
    return Arrays.asList(objects);
  }

  protected void assertCache(SketchQuery query, Result<Object[]> result)
  {
    CacheStrategy<Result<Object[]>, Object[], SketchQuery> strategy = toolChest.getCacheStrategyIfExists(query);
    Object[] cached = strategy.prepareForCache().apply(result);
    Assert.assertEquals(result.getTimestamp().getMillis(), cached[0]);
    Assert.assertArrayEquals(result.getValue(), Arrays.copyOfRange(cached, 1, cached.length));

    Result<Object[]> out = strategy.pullFromCache().apply(cached);
    Assert.assertEquals(result.getTimestamp(), out.getTimestamp());
    Assert.assertArrayEquals(result.getValue(), out.getValue());
  }
}
