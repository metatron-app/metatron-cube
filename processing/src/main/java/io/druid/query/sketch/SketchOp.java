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

package io.druid.query.sketch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 */
public enum SketchOp
{
  THETA {
    @Override
    public int defaultParam()
    {
      return 16384; // nomEntries
    }

    @Override
    public SketchHandler handler()
    {
      return new SketchHandler.Theta();
    }

    @Override
    public boolean isCardinalitySensitive()
    {
      return false;
    }
  },
  QUANTILE {
    @Override
    public int defaultParam()
    {
      // k Parameter that controls space usage of sketch and accuracy of estimates
      // Must be greater than 2 and less than 65536 and a power of 2
      return 1024;
    }

    @Override
    public SketchHandler handler()
    {
      return new SketchHandler.Quantile();
    }
  },
  FREQUENCY {
    @Override
    public int defaultParam()
    {
      return 2048;
    }

    @Override
    public SketchHandler handler()
    {
      // maxMapSize Determines the physical size of the internal hash map managed by this sketch
      // must be a power of 2
      return new SketchHandler.Frequency();
    }
  },
  SAMPLING {
    @Override
    public int defaultParam()
    {
      // Maximum size of sampling. Allocated size may be smaller until sampling fills.
      // Unlike many sketches in this package, this value does not need to be a power of 2
      return 64;
    }

    @Override
    public int normalize(int sketchParam)
    {
      return sketchParam;
    }

    @Override
    public SketchHandler handler()
    {
      return new SketchHandler.Sampling();
    }
  };

  public abstract int defaultParam();

  public int normalize(int sketchParam)
  {
    sketchParam = sketchParam - 1;
    sketchParam |= sketchParam >> 1;
    sketchParam |= sketchParam >> 2;
    sketchParam |= sketchParam >> 4;
    sketchParam |= sketchParam >> 8;
    sketchParam |= sketchParam >> 16;
    return sketchParam + 1;
  }

  public abstract SketchHandler handler();

  public boolean isCardinalitySensitive()
  {
    return true;
  }

  @JsonValue
  public String getName()
  {
    return name();
  }

  @JsonCreator
  public static SketchOp fromString(String name)
  {
    return name == null ? SketchOp.THETA : valueOf(name.toUpperCase());
  }
}
