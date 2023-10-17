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

package io.druid.query.deeplearning;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import io.druid.common.utils.PropUtils;
import io.druid.common.utils.StringUtils;
import io.druid.initialization.DruidModule;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.logger.Logger;

import java.util.List;
import java.util.Properties;

public class DeepLearningExtensionModule implements DruidModule
{
  private static final Logger LOG = new Logger(DeepLearningExtensionModule.class);

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule("deeplearning-extension")
            .registerSubtypes(DeepLearningFunctions.class)
    );
  }

  @Override
  public void configure(Binder binder)
  {
    binder.requestStaticInjection(DeepLearningFunctions.class);
    ClassLoader loader = DeepLearningExtensionModule.class.getClassLoader();

    // ReflectionUtils uses context loader
    ClassLoader prev = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(loader);
    try {
      bindFunctions(loader, new DefaultObjectMapper());
    }
    finally {
      Thread.currentThread().setContextClassLoader(prev);
    }
  }

  static void bindFunctions(ClassLoader loader, ObjectMapper mapper)
  {
    for (Properties properties : PropUtils.loadProperties(loader, "dl4j.properties")) {
      for (String name : properties.stringPropertyNames()) {
        String property = properties.getProperty(name);
        if (StringUtils.isNullOrEmpty(property)) {
          continue;
        }
        try {
          Word2VecConf config = mapper.readValue(property, Word2VecConf.class);
          if (config != null) {
            config.build(name, property, loader);
          } else {
            LOG.info("> Not found function [%s] with config %s.. skip", name, property);
          }
        }
        catch (Exception e) {
          LOG.info("> Failed to register function [%s] with class %s by %s.. skip", name, property, e);
        }
      }
    }
  }
}
