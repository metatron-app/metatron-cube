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

package io.druid.query.egads;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.metamx.common.guava.Sequence;
import com.yahoo.egads.data.TimeSeries;
import com.yahoo.egads.utilities.FileUtils;
import io.druid.common.utils.Sequences;
import io.druid.data.Pair;
import io.druid.granularity.Granularities;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class AnomalyPostProcessorTest
{
  @Test
  public void test()
  {
    final QueryRunner runner = new QueryRunner()
    {
      @Override
      public Sequence run(Query query, Map responseContext)
      {
        Properties p = new Properties();
        return Sequences.simple(
            Iterables.transform(
                Iterables.getOnlyElement(FileUtils.createTimeSeries("src/test/resources/model_input.csv", p)).data,
                new Function<TimeSeries.Entry, Map<String, Object>>()
                {
                  @Override
                  public Map<String, Object> apply(TimeSeries.Entry input)
                  {
                    Map<String, Object> row = Maps.newHashMap();
                    row.put("t", input.time);
                    row.put("m", input.value);
                    return row;
                  }
                }
            )
        );
      }
    };
    Map<String, Object> parameters = Maps.newHashMap();
    parameters.put("MAX_ANOMALY_TIME_AGO", 999999999);
    parameters.put("NUM_WEEKS", 10);
    parameters.put("NUM_TO_DROP", 0);
    parameters.put("THRESHOLD", "mapee#100,mase#10");
    parameters.put("TIME_SHIFTS", new int[]{0, 1});
    parameters.put("BASE_WINDOWS", new int[]{24, 168});

    for (Pair<String, Integer> entry : Arrays.asList(
        Pair.of("ExtremeLowDensityModel", 547),
        Pair.of("DBScanModel", 3),
        Pair.of("SimpleThresholdModel", 39)
    )) {
      AnomalyPostProcessor processor = new AnomalyPostProcessor(
          "t", "m", "p", "a", "OlympicModel", entry.lhs, parameters, Granularities.HOUR, null
      );

      @SuppressWarnings("unchecked")
      List<Map<String, Object>> result = Sequences.toList(processor.postProcess(runner).run(null, null));
      Iterable<Map<String, Object>> anomalies = Iterables.filter(
          result, new Predicate<Map<String, Object>>()
          {
            @Override
            public boolean apply(Map<String, Object> input)
            {
              return input.containsKey("a");
            }
          }
      );
      Assert.assertEquals(entry.rhs.longValue(), Iterables.size(anomalies));
    }
  }
}