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

package io.druid.cli.shell;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.curator.discovery.CuratorServiceUtils;
import io.druid.java.util.common.logger.Logger;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceInstance;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

/**
 */
public interface CommonShell
{
  void run(List<String> arguments) throws Exception;

  abstract class WithUtils implements CommonShell
  {
    protected static Logger LOG = new Logger(CommonShell.class);

    protected Properties loadNodeProperties(String nodeType) throws IOException
    {
      ClassLoader loader = getClass().getClassLoader();
      Properties properties = new Properties();
      URL resource = loader.getResource(String.format("%s/runtime.properties", nodeType));
      if (resource != null) {
        try (InputStream input = resource.openStream()) {
          properties.load(input);
        }
      }
      return properties;
    }

    protected URL discoverFromZK(CuratorFramework curator, String path)
    {
      try {
        return new URL(String.format("http://%s", new LeaderLatch(curator, path).getLeader().getId()));
      }
      catch (Exception e) {
        return null;
      }
    }

    protected List<URL> discoverFromNodeType(final ServiceDiscovery<String> discovery, final String nodeType)
    {
      try {
        Properties properties = loadNodeProperties(nodeType);
        String serviceName = Optional.ofNullable(properties.getProperty("druid.service")).orElse(nodeType);
        return discover(discovery, CuratorServiceUtils.makeCanonicalServiceName(serviceName));
      }
      catch (Exception e) {
        return null;
      }
    }

    private List<URL> discover(final ServiceDiscovery<String> discovery, final String service) throws Exception
    {
      Collection<ServiceInstance<String>> services = discovery.queryForInstances(service);
      if (services.isEmpty()) {
        services = Lists.newArrayList(
            Iterables.concat(Iterables.transform(Iterables.filter(discovery.queryForNames(), new Predicate<String>()
            {
              @Override
              public boolean apply(String input)
              {
                return input != null && input.contains(service);
              }
            }), new Function<String, Collection<ServiceInstance<String>>>()
            {
              @Override
              public Collection<ServiceInstance<String>> apply(String input)
              {
                try {
                  return discovery.queryForInstances(input);
                }
                catch (Exception e) {
                  return Collections.emptyList();
                }
              }
            }))
        );
        if (services.isEmpty()) {
          LOG.info("Failed to find service [%s] from discovery", service);
        }
      }
      return Lists.newArrayList(
          Iterables.transform(
              services,
              new Function<ServiceInstance<String>, URL>()
              {
                @Override
                public URL apply(ServiceInstance<String> input)
                {
                  try {
                    return new URL(String.format("http://%s:%d", input.getAddress(), input.getPort()));
                  }
                  catch (Exception e) {
                    throw Throwables.propagate(e);
                  }
                }
              }
          )
      );
    }
  }
}
