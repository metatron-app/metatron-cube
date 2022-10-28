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

package io.druid.query;

import io.druid.segment.TestHelper;
import io.druid.segment.TestIndex;
import io.druid.sql.calcite.CalciteQueryTestHelper;
import io.druid.sql.calcite.util.TestQuerySegmentWalker;
import io.druid.storage.hdfs.HdfsStorageHandler;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class TestSqlQuery extends CalciteQueryTestHelper
{
  static final TestQuerySegmentWalker SEGMENT_WALKER = TestIndex.segmentWalker.duplicate();

  public TestSqlQuery()
  {
    SEGMENT_WALKER.getForwardHandler().getHandlerMap().put(
        "file",
        new HdfsStorageHandler(new Configuration(), SEGMENT_WALKER.getMapper(), TestHelper.getTestIndexMergerV9())
    );
  }

  @Override
  protected TestQuerySegmentWalker walker()
  {
    return SEGMENT_WALKER;
  }

  @Test
  public void testTemporaryIndex() throws Exception
  {
    // todo: actually forward written index to test segment walker (it's simply ignored)
    testQuery(
        "CREATE TEMPORARY TABLE xx (__time, metric) as values\n"
        + " ('2021-04-01T09:00:00',1), ('2021-04-01T09:00:10',2), ('2021-04-01T09:00:20',1), ('2021-04-01T09:00:30',3),\n"
        + " ('2021-04-01T09:00:40',2), ('2021-04-01T09:00:50',1), ('2021-04-01T09:01:00',2), ('2021-04-01T09:01:10',3),\n"
        + " ('2021-04-01T09:01:20',2), ('2021-04-01T09:01:30',2), ('2021-04-01T09:01:40',4), ('2021-04-01T09:01:50',1),\n"
        + " ('2021-04-01T09:02:00',1), ('2021-04-01T09:02:10',2), ('2021-04-01T09:02:20',3), ('2021-04-01T09:02:30',1),\n"
        + " ('2021-04-01T09:02:40',2), ('2021-04-01T09:02:50',3), ('2021-04-01T09:03:00',3)",
        new Object[]{true, 19, MASKED, 1361L, MASKED, MASKED}
    );
  }
}
