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

package io.druid.query.filter;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.List;

public class ValueMatchers
{
  public static ValueMatcher and(ValueMatcher... matchers)
  {
    final List<ValueMatcher> list = ImmutableList.copyOf(
        Iterables.filter(Arrays.asList(matchers), m -> m != null && m != ValueMatcher.TRUE)
    );
    if (list.isEmpty()) {
      return ValueMatcher.TRUE;
    }
    if (list.size() == 1) {
      return list.get(0);
    }
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        for (ValueMatcher matcher : list) {
          if (!matcher.matches()) {
            return false;
          }
        }
        return true;
      }
    };
  }

  public static ValueMatcher or(ValueMatcher... matchers)
  {
    final List<ValueMatcher> list = Lists.newArrayList(Iterables.filter(Arrays.asList(matchers), Predicates.notNull()));
    if (list.isEmpty()) {
      return ValueMatcher.TRUE;
    }
    if (list.size() == 1) {
      return list.get(0);
    }
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        for (ValueMatcher matcher : list) {
          if (matcher.matches()) {
            return true;
          }
        }
        return false;
      }
    };
  }

  public static ValueMatcher not(ValueMatcher matcher)
  {
    return matcher == ValueMatcher.TRUE ? ValueMatcher.FALSE :
           matcher == ValueMatcher.FALSE ? ValueMatcher.TRUE :
           () -> !matcher.matches();
  }
}
