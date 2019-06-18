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

package io.druid.data.input.impl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class JSONPathSpec
{
  private final boolean useFieldDiscovery;
  private final List<JSONPathFieldSpec> fields;

  @JsonCreator
  public JSONPathSpec(
      @JsonProperty("useFieldDiscovery") Boolean useFieldDiscovery,
      @JsonProperty("fields") List<JSONPathFieldSpec> fields
  )
  {
    this.useFieldDiscovery = useFieldDiscovery == null ? true : useFieldDiscovery;
    this.fields = fields == null ? ImmutableList.<JSONPathFieldSpec>of() : fields;
  }

  @JsonProperty
  public boolean isUseFieldDiscovery()
  {
    return useFieldDiscovery;
  }

  @JsonProperty
  public List<JSONPathFieldSpec> getFields()
  {
    return fields;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    JSONPathSpec pathSpec = (JSONPathSpec) o;

    if (useFieldDiscovery != pathSpec.useFieldDiscovery) {
      return false;
    }
    if (!fields.equals(pathSpec.fields)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = (useFieldDiscovery ? 1 : 0);
    result = 31 * result + fields.hashCode();
    return result;
  }
}
