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

package io.druid.segment.filter;

import io.druid.query.filter.ValueMatcher;

/**
*/
public class BooleanValueMatcher implements ValueMatcher
{
  public static final ValueMatcher TRUE = new BooleanValueMatcher(true);
  public static final ValueMatcher FALSE = new BooleanValueMatcher(false);

  public static ValueMatcher of(boolean bool)
  {
    return bool ? TRUE : FALSE;
  }

  private final boolean matches;

  private BooleanValueMatcher(final boolean matches)
  {
    this.matches = matches;
  }

  @Override
  public boolean matches()
  {
    return matches;
  }
}
