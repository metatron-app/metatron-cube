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

package io.druid.indexing.commin.task;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.granularity.QueryGranularities;
import io.druid.indexer.HadoopIOConfig;
import io.druid.indexer.HadoopIngestionSpec;
import io.druid.indexer.IndexingHadoopDruidModule;
import io.druid.indexing.common.TestUtils;
import io.druid.indexing.common.task.HadoopIndexTask;
import io.druid.indexing.common.task.Task;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

public class TaskSerdeTest
{
  private final ObjectMapper jsonMapper;

  public TaskSerdeTest()
  {
    TestUtils testUtils = new TestUtils();
    jsonMapper = testUtils.getTestObjectMapper();

    for (final Module jacksonModule : new IndexingHadoopDruidModule().getJacksonModules()) {
      jsonMapper.registerModule(jacksonModule);
    }
  }

  @Test
  public void testHadoopIndexTaskSerde() throws Exception
  {
    final HadoopIndexTask task = new HadoopIndexTask(
        null,
        new HadoopIngestionSpec(
            new DataSchema(
                "foo", null, new AggregatorFactory[0], new UniformGranularitySpec(
                QueryGranularities.DAY,
                null,
                ImmutableList.of(new Interval("2010-01-01/P1D"))
            ),
                jsonMapper
            ), new HadoopIOConfig(ImmutableMap.<String, Object>of("paths", "bar"), null, null), null
        ),
        null,
        null,
        "blah",
        jsonMapper,
        null
    );

    final String json = jsonMapper.writeValueAsString(task);

    final HadoopIndexTask task2 = (HadoopIndexTask) jsonMapper.readValue(json, Task.class);

    Assert.assertEquals("foo", task.getDataSource());

    Assert.assertEquals(task.getId(), task2.getId());
    Assert.assertEquals(task.getGroupId(), task2.getGroupId());
    Assert.assertEquals(task.getDataSource(), task2.getDataSource());
    Assert.assertEquals(
        task.getSpec().getTuningConfig().getJobProperties(),
        task2.getSpec().getTuningConfig().getJobProperties()
    );
    Assert.assertEquals("blah", task.getClasspathPrefix());
    Assert.assertEquals("blah", task2.getClasspathPrefix());
  }
}
