/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.groupby.orderby;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import io.druid.common.utils.StringUtils;
import io.druid.data.Pair;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.Parser;
import io.druid.query.RowResolver;
import io.druid.segment.StringArray;
import io.druid.segment.incremental.IncrementalIndex;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/**
 */
public class PivotContext
{
  private final PivotSpec pivotSpec;
  private final WindowContext context;
  private final Object[] partitionKey;

  public PivotContext(PivotSpec pivotSpec, WindowContext context, Object[] partitionKey)
  {
    this.pivotSpec = pivotSpec;
    this.context = context;
    this.partitionKey = partitionKey;
  }

  public Map<String, Object> evaluate(final Map<StringArray, Object> mapping)
  {
    List<String> partitionColumns = context.partitionColumns();
    List<String> pivotColumns = Lists.newArrayList();
    for (PivotColumnSpec columnSpec : pivotSpec.getPivotColumns()) {
      pivotColumns.add(columnSpec.getDimension());
    }
    final String separator = pivotSpec.getSeparator();
    final boolean appendingValue = pivotSpec.isAppendValueColumn();
    final List<String> valueColumns = pivotSpec.getValueColumns();

    // snapshot
    final Map<String, Object> event = Maps.newLinkedHashMap();
    for (int i = 0; i < partitionKey.length; i++) {
      event.put(partitionColumns.get(i), partitionKey[i]);
    }
    final Comparator<StringArray> comparator = pivotSpec.makeColumnOrdering();
    final List<Map.Entry<StringArray, Object>> entries = IncrementalIndex.sortOn(mapping, comparator, false);
    for (Map.Entry<StringArray, Object> entry : entries) {
      String newKey = StringUtils.concat(pivotSpec.getSeparator(), entry.getKey().array());
      Object newValue = entry.getValue();
      event.put(newKey, newValue);
      if (newValue != null) {
        context.addType(newKey, RowResolver.toValueType(newValue));
      }
    }
    if (pivotSpec.getRowExpressions().isEmpty()) {
      return event;
    }

    final Expr.NumericBinding binding = WindowingSpec.withMap(event);

    for (String expression : pivotSpec.getRowExpressions()) {
      List<Integer> groupIds = Lists.newArrayList();
      Pair<Expr, Expr> assign = Evals.splitAssign(expression);
      for (String required : Iterables.concat(
          Parser.findRequiredBindings(assign.lhs),
          Parser.findRequiredBindings(assign.rhs)
      )) {
        int groupId = extractGroupId(required, pivotColumns.size());
        if (groupId > 0 && !groupIds.contains(groupId)) {
          groupIds.add(groupId);
        }
      }
      if (groupIds.isEmpty()) {
        // no pivot group.. simple expression
        String key = Evals.toAssigneeEval(assign.lhs).asString();
        ExprEval value = assign.rhs.eval(binding);
        event.put(key, value.value());
        context.addType(key, value.type());
        continue;
      }
      // contains pivot grouping expression
      List<BitSet> bitSets = Lists.newArrayList();
      for (Integer groupId : groupIds) {
        BitSet bitSet = new BitSet();
        for (int i = 0; groupId > 0; groupId >>>= 1, i++) {
          if ((groupId & 0x01) != 0) {
            bitSet.set(i);
          }
        }
        bitSets.add(bitSet);
      }
      BitSet union = new BitSet();
      for (BitSet bitSet : bitSets) {
        union.or(bitSet);
      }

next:
      for (Map.Entry<StringArray, Object> entry : entries) {
        final StringArray key = entry.getKey();
        if (!isTarget(key, union, pivotColumns.size())) {
          continue;
        }
        Map<String, Object> assigneeOverrides = Maps.newHashMap();
        final Map<String, Object> assignedOverrides = Maps.newHashMap();
        for (int i = 0; i < valueColumns.size(); i++) {
          for (int j = 0; j < bitSets.size(); j++) {
            int groupId = groupIds.get(j);
            BitSet bitSet = bitSets.get(j);
            String[] keys = new String[pivotColumns.size() + (appendingValue ? 1 : 0)];
            for (int k = 0; k < keys.length; k++) {
              keys[k] = bitSet.get(k) ? key.get(k) : "";  // see PivotColumnSpec.toExtractor()
            }
            if (appendingValue) {
              keys[pivotColumns.size()] = valueColumns.get(i);
            }
            Object value = mapping.get(StringArray.of(keys));
            if (value == null) {
              continue next;
            }
            if (valueColumns.size() > 1 && !appendingValue) {
              value = ((List)value).get(i);
            }
            String groupKey = "$" + groupId;
            assigneeOverrides.put(groupKey, StringUtils.concat(separator, keys));
            assignedOverrides.put(groupKey, value);
          }
          ExprEval assignee = Evals.toAssigneeEval(assign.lhs, assigneeOverrides);
          final ExprEval assigned = Evals.eval(
              assign.rhs, new Expr.NumericBinding()
              {
                @Override
                public Collection<String> names()
                {
                  return Sets.newHashSet(Iterables.concat(event.keySet(), assignedOverrides.keySet()));
                }

                @Override
                public Object get(String name)
                {
                  Object value = assignedOverrides.get(name);
                  return value != null && assignedOverrides.containsKey(name) ? value : binding.get(name);
                }
              }
          );

          String newAssignee = assignee.stringValue();
          if (valueColumns.size() > 1 && !appendingValue) {
            final int x = i;
            event.compute(
                newAssignee, new BiFunction<String, Object, Object>()
                {
                  @Override
                  @SuppressWarnings("unchecked")
                  public Object apply(String s, Object o)
                  {
                    if (o == null) {
                      o = Arrays.asList(new Object[valueColumns.size()]);
                    }
                    ((List)o).set(x, assigned.value());
                    return o;
                  }
                }
            );
          } else {
            event.put(newAssignee, assigned.value());
            context.addType(newAssignee, assigned.type());
          }
        }
      }
    }
    return event;
  }

  private boolean isTarget(StringArray key, BitSet bitSet, int length)
  {
    for (int i = 0; i < length; i++) {
      if (!(bitSet.get(i) ^ StringUtils.isNullOrEmpty(key.get(i)))) {
        return false;
      }
    }
    return true;
  }

  private static int extractGroupId(String binding, int length)
  {
    if (StringUtils.isNullOrEmpty(binding) || binding.charAt(0) != '$') {
      return -1;
    }
    Long groupId = Longs.tryParse(binding.substring(1));
    if (groupId == null) {
      return -1;
    }
    final int max = (int) Math.pow(2, length);
    if (groupId >= max) {
      throw new IllegalArgumentException("invalid group Id " + groupId);
    }
    return groupId == 0 ? max - 1 : Ints.checkedCast(groupId);
  }
}
