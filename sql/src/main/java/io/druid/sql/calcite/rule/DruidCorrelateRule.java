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

import com.google.common.base.Predicate;
import io.druid.sql.calcite.rel.DruidCorrelateRel;
import io.druid.sql.calcite.rel.DruidRel;
import io.druid.sql.calcite.rel.PartialDruidQuery;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Correlate;

public class DruidCorrelateRule extends RelOptRule
{
  private static final Predicate<PartialDruidQuery> PREDICATE = p -> p != null && p.isFilterScan();

  private static final DruidCorrelateRule INSTANCE = new DruidCorrelateRule();

  public static DruidCorrelateRule instance()
  {
    return INSTANCE;
  }

  private DruidCorrelateRule()
  {
    super(operand(Correlate.class, some(DruidRules.anyDruid(), DruidRules.anyDruid())));
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    final Correlate correlate = call.rel(0);
    final DruidRel left = call.rel(1);
    final DruidRel right = call.rel(2);
    call.transformTo(DruidCorrelateRel.of(correlate, left, right));
  }
}
