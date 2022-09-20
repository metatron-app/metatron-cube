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

package io.druid.segment;

import io.druid.segment.filter.FilterContext;

import java.util.function.IntFunction;

/**
 * should not be used outside of lifecycle. don't do like this: Sequences.only(factory.makeCursors())
 */
public interface Cursor extends ColumnSelectorFactory
{
  default int size()
  {
    return -1;
  }

  long getStartTime();
  long getRowTimestamp();
  int offset();
  void advance();
  void advanceWithoutMatcher();
  default int advanceN(int n) {for (; n > 0 && !isDone(); n--) advance(); return n;}
  default int advanceNWithoutMatcher(int n) {for (; n > 0 && !isDone(); n--) advanceWithoutMatcher(); return n;}
  boolean isDone();
  void reset();

  default FilterContext getFilterContext() { return null;}
  default IntFunction getAttachment(String name) { return null;}

  abstract class ExprSupport extends ColumnSelectorFactory.ExprSupport implements Cursor { }
}
