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

import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import io.druid.initialization.DruidModule;

import java.util.List;

public class Lucene7ExtensionModule implements DruidModule
{
  @Override
  public List<? extends com.fasterxml.jackson.databind.Module> getJacksonModules()
  {
    return ImmutableList.of(
        new LuceneCommonExtensionModule().getModule(true),
        new SimpleModule("lucene7-extension")
            .registerSubtypes(Lucene7IndexingSpec.class)
            .registerSubtypes(Lucene7IndexingSpec.SerDe.class)
            .registerSubtypes(Lucene7FSTSerDe.class)
    );
  }

  @Override
  public void configure(Binder binder)
  {
    new LuceneCommonExtensionModule().configure(binder);
  }
}
