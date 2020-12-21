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

package io.druid.cli;

import com.google.common.base.Throwables;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.druid.initialization.Initialization;

import java.util.List;

/**
 */
@Command(name = "exec", description = "Runs main method of class.")
public class SimpleExec implements Runnable
{
  @Option(name = {"-e"}, title = "extension", description = "Name of extension")
  public String extension;

  @Arguments(description = "arguments")
  public List<String> args;

  @Override
  public void run()
  {
    if (args.isEmpty()) {
      return;
    }
    final String className = args.get(0);
    final String[] parameters = args.subList(1, args.size()).toArray(new String[0]);
    final ClassLoader prev = Thread.currentThread().getContextClassLoader();
    final ClassLoader loader = Initialization.getClassLoaderForExtension(extension, SimpleExec.class.getClassLoader());
    Thread.currentThread().setContextClassLoader(loader);
    try {
      loader.loadClass(className)
            .getMethod("main", String[].class)
            .invoke(null, new Object[]{parameters});
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
    finally {
      Thread.currentThread().setContextClassLoader(prev);
    }
  }
}
