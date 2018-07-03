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

package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metamx.common.logger.Logger;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.DataSourceMetadataUpdateAction;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.overlord.BaseDataSourceMetadata;
import io.druid.metadata.TableDesc;
import org.joda.time.DateTime;

/**
 */
public class DataSourceMetadataUpdateTask extends AbstractTask
{
  private static final Logger log = new Logger(DataSourceMetadataUpdateTask.class);

  private final TableDesc tableDesc;

  @JsonCreator
  public DataSourceMetadataUpdateTask(
      @JsonProperty("id") String id,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("tableDesc") TableDesc tableDesc
  )
  {
    super(id == null ? joinId("ds_metadata_update", dataSource, new DateTime().toString()) : id, dataSource, null);
    this.tableDesc = tableDesc;
  }

  @Override
  public String getType()
  {
    return "ds_metadata_update";
  }

  @JsonProperty
  public TableDesc getTableDesc()
  {
    return tableDesc;
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    return true;
  }

  @Override
  public TaskStatus run(TaskToolbox toolbox) throws Exception
  {
    String dataSource = getDataSource();
    BaseDataSourceMetadata metaData = new BaseDataSourceMetadata(tableDesc);
    if (toolbox.getTaskActionClient()
               .submit(new DataSourceMetadataUpdateAction(dataSource, metaData))) {
      return TaskStatus.success(getId());
    }
    return TaskStatus.failure(getId(), "Failed to update datasource metadata");
  }
}
