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

package io.druid.hive;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.inject.Binder;
import com.google.inject.multibindings.Multibinder;
import hivemall.anomaly.HivemallFunctions;
import io.druid.common.utils.PropUtils;
import io.druid.common.utils.StringUtils;
import io.druid.initialization.DruidModule;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.aggregation.AggregatorFactory.SQLBundle;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 */
public class HiveUDFDruidModule implements DruidModule
{
  private static final Logger LOG = new Logger(HiveUDFDruidModule.class);

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule("HiveUDFDruidModule")
            .registerSubtypes(HivemallFunctions.class)
            .registerSubtypes(HiveFunctions.class)
            .registerSubtypes(HiveUDAFAggregatorFactory.class)
    );
  }

  @Override
  public void configure(Binder binder)
  {
    final ClassLoader loader = HiveUDFDruidModule.class.getClassLoader();

    // ReflectionUtils uses context loader
    final ClassLoader prev = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(loader);
    try {
      bindFunctions(binder, loader);
    }
    finally {
      Thread.currentThread().setContextClassLoader(prev);
    }
  }

  public void bindFunctions(Binder binder, ClassLoader loader)
  {
    final Set<String> userDefind = Sets.newHashSet();
    for (Properties properties : PropUtils.loadProperties(loader, "hive.function.properties")) {
      for (String name : properties.stringPropertyNames()) {
        String className = properties.getProperty(name);
        try {
          if (!StringUtils.isNullOrEmpty(className)) {
            FunctionInfo info = HiveFunctions.registerFunction(name, Class.forName(className, false, loader));
            if (info != null) {
              LOG.info("> '%s' is registered with class %s", name, className);
              userDefind.add(name);
            }
          }
        }
        catch (Exception e) {
          LOG.info("> Failed to register function [%s] with class %s by %s.. skip", name, className, e);
        }
      }
    }
    final Multibinder<SQLBundle> set = Multibinder.newSetBinder(binder, SQLBundle.class);
    for (String name : HiveFunctions.getFunctionNames()) {
      try {
        FunctionInfo info = HiveFunctions.getFunctionInfo(name);
        if (info != null && info.isGenericUDAF()) {
          String registered = userDefind.contains(name) || name.startsWith("hive") ? name : "hive_" + name;
          HiveUDAFAggregatorFactory udaf = new HiveUDAFAggregatorFactory("<name>", Arrays.<String>asList(), name);
          set.addBinding().toInstance(new SQLBundle(registered, udaf));
          LOG.info("> hive UDAF '%s' is registered as %s", name, registered);
        }
      }
      catch (SemanticException e) {
        // ignore
      }
    }
  }
}
