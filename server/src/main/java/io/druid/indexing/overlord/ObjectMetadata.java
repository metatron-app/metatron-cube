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

package io.druid.indexing.overlord;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.metadata.TableDesc;

import java.util.Objects;

public final class ObjectMetadata extends BaseDataSourceMetadata
{
  private final Object theObject;

  @JsonCreator
  public ObjectMetadata(
      @JsonProperty("object") Object theObject,
      @JsonProperty("tableDesc") TableDesc tableDesc
  )
  {
    super(tableDesc);
    this.theObject = theObject;
  }

  public ObjectMetadata(Object theObject)
  {
    this(theObject, null);
  }

  @JsonProperty("object")
  public Object getObject()
  {
    return theObject;
  }

  @Override
  public boolean isValidStart()
  {
    return theObject == null;
  }

  @Override
  public boolean matches(DataSourceMetadata other)
  {
    return other instanceof ObjectMetadata && Objects.equals(theObject, ((ObjectMetadata)other).theObject);
  }

  @Override
  public DataSourceMetadata plus(DataSourceMetadata other)
  {
    return new ObjectMetadata(((ObjectMetadata)other).theObject, updateProperty(other.getTableDesc()));
  }

  @Override
  public DataSourceMetadata minus(DataSourceMetadata other)
  {
    return this;
  }

  @Override
  public boolean equals(Object o)
  {
    if (o instanceof ObjectMetadata) {
      final Object other = ((ObjectMetadata) o).getObject();
      return super.equals(o) && Objects.equals(theObject, other);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), theObject);
  }

  @Override
  public String toString()
  {
    return "ObjectMetadata{" +
           "theObject=" + theObject +
           "tableDesc=" + getTableDesc() +
           '}';
  }
}
