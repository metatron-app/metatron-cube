/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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

import io.druid.sql.calcite.rel.DruidJoinRel;
import io.druid.sql.calcite.rel.DruidQueryRel;
import io.druid.sql.calcite.rel.DruidRel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;

public class DruidJoinRule extends RelOptRule
{
  private static final DruidJoinRule INSTANCE = new DruidJoinRule();

  public static DruidJoinRule instance()
  {
    return INSTANCE;
  }

  private DruidJoinRule()
  {
    super(
        operand(
            Join.class, some(operand(DruidQueryRel.class, any()), operand(DruidQueryRel.class, any()))
        )
    );
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    final Join join = call.rel(0);
    final DruidRel left = call.rel(1);
    final DruidRel right = call.rel(2);

    final JoinInfo joinInfo = join.analyzeCondition();
    if (joinInfo.isEqui()) {
      call.transformTo(DruidJoinRel.create(join, joinInfo, left, right));
    }
  }
}
