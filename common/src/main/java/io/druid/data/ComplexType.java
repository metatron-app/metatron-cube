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
package io.druid.data;

/**
 */
public enum ComplexType
{
  UNKNOWN,
  OTHER,
  MAP,
  LIST,

  ARRAY {
    @Override
    public boolean isPrefixed() { return true; }
    @Override
    public String prefix() { return ValueDesc.ARRAY_PREFIX; }
  },
  // aka. IndexedInts.WithLookup
  // this is return type of object selector which simulates dimension selector (used for some filter optimization)
  INDEXED_ID {
    @Override
    public boolean isPrefixed() { return true; }
    @Override
    public String prefix() { return ValueDesc.INDEXED_ID_PREFIX; }
  },
  // this is return type of object selector which can return element type or array of element type,
  // which is trait of dimension
  MULTIVALUED {
    @Override
    public boolean isPrefixed() { return true; }
    @Override
    public String prefix() { return ValueDesc.MULTIVALUED_PREFIX; }
  },
  // prefix of dimension
  DIMENSION {
    @Override
    public boolean isPrefixed() { return true; }
    @Override
    public String prefix() { return ValueDesc.DIMENSION_PREFIX; }
  },
  // descriptive type
  DECIMAL {
    @Override
    public boolean isDescriptive() { return true; }
  };

  public boolean isPrefixed()
  {
    return false;
  }

  public String prefix()
  {
    throw new UnsupportedOperationException("prefix");
  }

  public boolean isDescriptive()
  {
    return false;
  }
}
