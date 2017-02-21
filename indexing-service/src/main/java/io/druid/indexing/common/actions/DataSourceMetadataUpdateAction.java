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

package io.druid.indexing.common.actions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.DataSourceMetadata;

import java.io.IOException;

public class DataSourceMetadataUpdateAction implements TaskAction<Boolean>
{
  @JsonIgnore
  private final String dataSource;

  @JsonIgnore
  private final DataSourceMetadata metaData;

  @JsonCreator
  public DataSourceMetadataUpdateAction(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("metaData") DataSourceMetadata metaData
  )
  {
    this.dataSource = Preconditions.checkNotNull(dataSource);
    this.metaData = metaData;
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public DataSourceMetadata getMetaData()
  {
    return metaData;
  }

  public TypeReference<Boolean> getReturnTypeReference()
  {
    return new TypeReference<Boolean>() { };
  }

  @Override
  public Boolean perform(Task task, TaskActionToolbox toolbox) throws IOException
  {
    return toolbox.getIndexerMetadataStorageCoordinator().updateDataSourceMetadata(dataSource, metaData);
  }

  @Override
  public boolean isAudited()
  {
    return true;
  }

  @Override
  public String toString()
  {
    return "DataSourceMetadataUpdateAction{" +
           "dataSource=" + dataSource +
           "metaData=" + metaData +
           '}';
  }
}
