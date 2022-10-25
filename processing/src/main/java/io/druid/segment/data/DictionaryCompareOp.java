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

// very confusing..
public enum DictionaryCompareOp
{
  EQ {
    @Override
    public DictionaryCompareOp flip()
    {
      return this;
    }

    @Override
    public int ix(int ix)
    {
      return ix;
    }

    @Override
    public boolean compare(int ix, int target)
    {
      return ix == target;
    }
  },
  NEQ {
    @Override
    public DictionaryCompareOp flip()
    {
      return this;
    }

    @Override
    public int ix(int ix)
    {
      return ix;
    }

    @Override
    public boolean compare(int ix, int target)
    {
      return ix != target;
    }
  },
  GT {
    @Override
    public DictionaryCompareOp flip()
    {
      return LT;
    }

    @Override
    public int ix(int ix)
    {
      return ix < 0 ? -ix - 1: ix;
    }

    @Override
    public boolean compare(int ix, int target)
    {
      return target < ix;
    }
  },
  GTE {
    @Override
    public DictionaryCompareOp flip()
    {
      return LTE;
    }

    @Override
    public int ix(int ix)
    {
      return ix < 0 ? -ix - 2 : ix;
    }

    @Override
    public boolean compare(int ix, int target)
    {
      return target <= ix;
    }
  },
  LT {
    @Override
    public DictionaryCompareOp flip()
    {
      return GT;
    }

    @Override
    public int ix(int ix)
    {
      return ix < 0 ? -ix - 2 : ix;
    }

    @Override
    public boolean compare(int ix, int target)
    {
      return target > ix;
    }
  },
  LTE {
    @Override
    public DictionaryCompareOp flip()
    {
      return GTE;
    }

    @Override
    public int ix(int ix)
    {
      return ix < 0 ? -ix - 1 : ix;
    }

    @Override
    public boolean compare(int ix, int target)
    {
      return target >= ix;
    }
  };

  public abstract DictionaryCompareOp flip();

  public abstract int ix(int ix);

  public abstract boolean compare(int ix, int target);
}
