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

package io.druid.sql.calcite;

import io.druid.segment.TestIndex;
import io.druid.sql.calcite.util.TestQuerySegmentWalker;

public class TpchTestHelper extends CalciteQueryTestHelper
{
  protected static final MiscQueryHook hook = new MiscQueryHook();
  protected static final TestQuerySegmentWalker walker;

  static {
    walker = TestIndex.segmentWalker.duplicate().withQueryHook(hook);
    walker.populate("lineitem");  // 30201
    walker.populate("orders");    // 7500
    walker.populate("partsupp");  // 4000
    walker.populate("customer");  // 750
    walker.populate("part");      // 1000
    walker.populate("supplier");  // 50
    walker.populate("nation");    // 25
    walker.populate("region");    // 5
  }

  @Override
  protected TestQuerySegmentWalker walker()
  {
    return walker;
  }
}
