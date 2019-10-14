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

package io.druid.query.frequency;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.guava.Sequences;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.TableDataSource;
import io.druid.segment.TestIndex;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 */
@RunWith(Parameterized.class)
public class FrequencyQueryRunnerTest extends QueryRunnerTestHelper
{
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return cartesian(Arrays.asList(TestIndex.DS_NAMES));
  }

  private final String dataSource;

  public FrequencyQueryRunnerTest(String dataSource)
  {
    this.dataSource = dataSource;
  }

  @Test
  public void testBasic()
  {
    List<String> columns = Arrays.asList("market", "quality");
    FrequencyQuery query = new FrequencyQuery(
        TableDataSource.of(dataSource), null, null, null, columns, 65536, 4, 3, null, null);
    String[] columnNames = {"__time", "market", "quality", "index", "indexMin"};
    List<Object[]> expected = createExpected(
        new Object[]{93, "spot", "automotive"},
        new Object[]{93, "spot", "business"},
        new Object[]{93, "spot", "entertainment"}
    );

    List<Object[]> results = Sequences.toList(
        query.run(TestIndex.segmentWalker, Maps.<String, Object>newHashMap()),
        Lists.<Object[]>newArrayList()
    );
    validate(expected, results);
  }

  public static List<Object[]> createExpected(Object[]... values)
  {
    return Arrays.asList(values);
  }

  public static void validate(
      List<Object[]> expected,
      List<Object[]> result
  )
  {
    int max1 = Math.min(expected.size(), result.size());
    for (int i = 0; i < max1; i++) {
      Object[] e = expected.get(i);
      Object[] r = result.get(i);
      Assert.assertEquals(Arrays.toString(e), Arrays.toString(r));
    }
    if (expected.size() > result.size()) {
      Assert.fail("need more results");
    }
    if (expected.size() < result.size()) {
      Assert.fail("need less results");
    }
  }
}
