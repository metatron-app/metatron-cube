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

package io.druid.metadata.storage.mysql;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Key;
import io.druid.guice.LazySingleton;
import io.druid.guice.PolyBind;
import io.druid.guice.SQLMetadataStorageDruidModule;
import io.druid.initialization.DruidModule;
import io.druid.metadata.MetadataStorageActionHandlerFactory;
import io.druid.metadata.MetadataStorageConnector;
import io.druid.metadata.MySQLMetadataStorageActionHandlerFactory;
import io.druid.metadata.SQLMetadataConnector;

import java.util.List;

public class MySQLMetadataStorageModule extends SQLMetadataStorageDruidModule implements DruidModule
{
  public static final String TYPE = "mysql";

  public MySQLMetadataStorageModule()
  {
    super(TYPE);
  }

  @Override
  public List<? extends com.fasterxml.jackson.databind.Module> getJacksonModules()
  {
    return ImmutableList.of();
  }

  @Override
  public void configure(Binder binder)
  {
    super.configure(binder);

    PolyBind.optionBinder(binder, Key.get(MetadataStorageConnector.class))
            .addBinding(TYPE)
            .to(MySQLConnector.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(SQLMetadataConnector.class))
            .addBinding(TYPE)
            .to(MySQLConnector.class)
            .in(LazySingleton.class);
    PolyBind.optionBinder(binder, Key.get(MetadataStorageActionHandlerFactory.class))
            .addBinding(TYPE)
            .to(MySQLMetadataStorageActionHandlerFactory.class)
            .in(LazySingleton.class);
  }
}
