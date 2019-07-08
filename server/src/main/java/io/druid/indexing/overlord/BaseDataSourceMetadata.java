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

/**
 */
public class BaseDataSourceMetadata implements DataSourceMetadata
{
  private final TableDesc tableDesc;

  @JsonCreator
  public BaseDataSourceMetadata(@JsonProperty("tableDesc") TableDesc tableDesc)
  {
    this.tableDesc = tableDesc;
  }

  @JsonProperty
  public TableDesc getTableDesc()
  {
    return tableDesc;
  }

  @Override
  public boolean isValidStart()
  {
    return true;
  }

  @Override
  public boolean matches(DataSourceMetadata other)
  {
    return true;
  }

  @Override
  public DataSourceMetadata plus(DataSourceMetadata other)
  {
    return new BaseDataSourceMetadata(updateProperty(other.getTableDesc()));
  }

  @Override
  public DataSourceMetadata minus(DataSourceMetadata other)
  {
    return new BaseDataSourceMetadata(updateProperty(this.getTableDesc()));
  }

  protected final TableDesc updateProperty(TableDesc other)
  {
    if (tableDesc == null) {
      return other;
    }
    if (other == null) {
      return tableDesc;
    }
    return tableDesc.update(other);
  }

  @Override
  public boolean equals(Object o)
  {
    if (o instanceof DataSourceMetadata) {
      return Objects.equals(tableDesc, ((DataSourceMetadata) o).getTableDesc());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(tableDesc);
  }

  @Override
  public String toString()
  {
    return "BaseDataSourceMetadata{" +
           "tableDesc=" + tableDesc +
           '}';
  }
}
