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

package io.druid.guice;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.metamx.common.logger.Logger;
import io.druid.jackson.JacksonModule;

import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 */
public class GuiceInjectors
{
  private static final Logger log = new Logger(GuiceInjectors.class);

  public static Collection<Module> makeDefaultStartupModules(String... propertiesLocations)
  {
    return ImmutableList.<Module>of(
        new DruidGuiceExtensions(),
        new JacksonModule(),
        new PropertiesModule(Arrays.asList(propertiesLocations)),
        new ConfigModule(),
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bind(DruidSecondaryModule.class);
            JsonConfigProvider.bind(binder, "druid.extensions", ExtensionsConfig.class);
          }
        }
    );
  }

  public static Injector makeStartupInjector(String... propertiesLocations)
  {
    String[] version1 = findResources(
        "com/fasterxml/jackson/databind/deser/std/FromStringDeserializer.class",
        "jackson-databind"
    );
    String[] version2 = findResources(
        "com/fasterxml/jackson/datatype/guava/deser/HostAndPortDeserializer.class",
        "jackson-datatype-guava"
    );
    if (!Objects.equals(version1[1], version2[1])) {
      log.warn("jackson-databind is in '%s' and jackson-datatype-guava is in '%s'", version1[0], version1[1]);
      log.warn("Possible dependency conflicts.. consider using 'mapreduce.job.user.classpath.first=true'");
    }
    return Guice.createInjector(makeDefaultStartupModules(propertiesLocations));
  }

  // log message for xxxxing HostAndPortDeserializer problem
  private static String[] findResources(String resource, String library)
  {
    URL found = Guice.class.getClassLoader().getResource(resource);
    if (found != null && "jar".equals(found.getProtocol())) {
      String path = URI.create(found.getPath()).getPath();
      if (path.indexOf('!') > 0) {
        path = path.substring(0, path.indexOf('!'));
      }
      String regex = ".*" + library + "-(.+)\\.jar";
      Matcher matcher = Pattern.compile(regex).matcher(path);
      if (matcher.matches()) {
        return new String[] {path, matcher.group(1)};
      }
      return new String[] {path, null};
    }
    return new String[] {found == null ? "" : found.getPath(), null};
  }

  public static Injector makeStartupInjectorWithModules(Iterable<? extends Module> modules)
  {
    List<Module> theModules = Lists.newArrayList();
    theModules.addAll(makeDefaultStartupModules());
    for (Module theModule : modules) {
      theModules.add(theModule);
    }
    return Guice.createInjector(theModules);
  }
}
