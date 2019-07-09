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

package io.druid.server;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Key;
import io.druid.guice.JerseyModule;
import io.druid.server.http.AdminResource;

import java.util.List;

public class AdminModule implements JerseyModule
{
  private final Shutdown.Proc closeable;

  public AdminModule(Shutdown.Proc closeable) {this.closeable = closeable;}

  @Override
  public void configure(Binder binder)
  {
    binder.bind(Key.get(Shutdown.Proc.class, Shutdown.class)).toInstance(closeable);
  }

  @Override
  public List<Class> getResources()
  {
    return ImmutableList.<Class>of(AdminResource.class);
  }
}