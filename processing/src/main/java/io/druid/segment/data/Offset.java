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

package io.druid.segment.data;

/**
 * The "mutable" version of a ReadableOffset.  Introduces "increment()" and "withinBounds()" methods, which are
 * very similar to "next()" and "hasNext()" on the Iterator interface except increment() does not return a value.
 */
public interface Offset extends ReadableOffset
{
  boolean increment();

  default int incrementN(int n)
  {
    for (; n > 0 && increment(); n--) ;
    return n;
  }

  boolean withinBounds();

  Offset clone();

  Offset EMPTY = new Offset()
  {
    @Override
    public boolean increment()
    {
      return false;
    }

    @Override
    public boolean withinBounds()
    {
      return false;
    }

    @Override
    public Offset clone()
    {
      return this;
    }

    @Override
    public int getOffset()
    {
      return -1;
    }
  };
}
