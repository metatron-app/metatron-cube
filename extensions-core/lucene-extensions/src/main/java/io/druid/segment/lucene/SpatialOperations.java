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

package io.druid.segment.lucene;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableMap;
import org.apache.lucene.spatial.query.SpatialOperation;

import java.util.Map;

/**
 */
public enum SpatialOperations
{
  // RecursivePrefixTreeStrategy supports below three operations
  COVERS {
    @Override
    public SpatialOperation op()
    {
      return SpatialOperation.Contains;
    }
  },
  INTERSECTS {
    @Override
    public SpatialOperation op()
    {
      return SpatialOperation.Intersects;
    }
  },
  COVEREDBY {
    @Override
    public SpatialOperation op()
    {
      return SpatialOperation.IsWithin;
    }
  },
  EQUALTO {
    @Override
    public SpatialOperation op()
    {
      return SpatialOperation.IsEqualTo;
    }

    @Override
    public boolean isLuceneNative() { return false;}
  },
  OVERLAPS {
    @Override
    public SpatialOperation op()
    {
      return SpatialOperation.Overlaps;
    }

    @Override
    public boolean isLuceneNative() { return false;}
  },
  BBOX_INTERSECTS {
    @Override
    public SpatialOperation op()
    {
      return SpatialOperation.BBoxIntersects;
    }

    @Override
    public boolean isLuceneNative() { return false;}
  },
  BBOX_WITHIN {
    @Override
    public SpatialOperation op()
    {
      return SpatialOperation.BBoxWithin;
    }

    @Override
    public boolean isLuceneNative() { return false;}
  };

  public abstract SpatialOperation op();

  public boolean isLuceneNative()
  {
    return true;
  }

  @JsonValue
  public String getName()
  {
    return name().toLowerCase();
  }

  @JsonCreator
  public static SpatialOperations fromString(String name)
  {
    if (name == null) {
      return null;
    }
    String normalized = name.toUpperCase().trim();
    if (ALIASES.containsKey(normalized)) {
      return ALIASES.get(normalized);
    }
    return valueOf(normalized);
  }

  // alias to real
  private static Map<String, SpatialOperations> ALIASES = ImmutableMap.<String, SpatialOperations>of(
      "CONTAINS", COVERS,
      "ISWITHIN", COVEREDBY,
      "WITHIN", COVEREDBY
  );
}
