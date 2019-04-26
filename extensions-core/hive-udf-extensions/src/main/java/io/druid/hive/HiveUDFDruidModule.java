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

package io.druid.hive;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Binder;
import com.metamx.common.logger.Logger;
import hivemall.anomaly.HivemallFunctions;
import io.druid.common.utils.StringUtils;
import io.druid.initialization.DruidModule;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

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
    for (Properties properties : loadProperties(loader)) {
      for (String name : properties.stringPropertyNames()) {
        String className = properties.getProperty(name);
        try {
          if (!StringUtils.isNullOrEmpty(className)) {
            FunctionInfo info = HiveFunctions.registerFunction(name, Class.forName(className, false, loader));
            if (info != null) {
              LOG.info("> '%s' is registered with class %s", name, className);
            }
          }
        }
        catch (Exception e) {
          LOG.info("> Failed to register function [%s] with class %s by %s.. skip", name, className, e);
        }
      }
    }
  }

  private Iterable<Properties> loadProperties(ClassLoader loader)
  {
    final Enumeration<URL> resources;
    try {
      resources = loader.getResources("hive.function.properties");
    }
    catch (IOException e) {
      return Arrays.asList();
    }
    final List<Properties> loaded = Lists.newArrayList();
    while (resources.hasMoreElements()) {
      final URL element = resources.nextElement();
      try {
        loaded.add(load(element.openStream()));
      }
      catch (IOException e) {
        // ignore
      }
    }
    return loaded;
  }

  private Properties load(InputStream resource)
  {
    Properties properties = new Properties();
    try {
      properties.load(resource);
    }
    catch (IOException e) {
      LOG.warn(e, "Failed to load function resource.. ignoring");
      return null;
    }
    finally {
      IOUtils.closeQuietly(resource);
    }
    return properties;
  }
}
