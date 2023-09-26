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

package io.druid.sql.calcite.rule;

import io.druid.sql.calcite.rel.DruidQueryRel;
import io.druid.sql.calcite.rel.DruidRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;

public class DruidMetaQueryRule extends RelOptRule
{
  public static final DruidMetaQueryRule INSTANCE = new DruidMetaQueryRule();

  private DruidMetaQueryRule()
  {
    super(DruidRel.operand(DruidQueryRel.class, q -> !q.containsEstimates()));
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    DruidQueryRel query = call.rel(0);
    DruidQueryRel propagated = query.propagateEstimates(call.getMetadataQuery());
    if (propagated != null) {
      call.transformTo(propagated);
    }
  }
}
