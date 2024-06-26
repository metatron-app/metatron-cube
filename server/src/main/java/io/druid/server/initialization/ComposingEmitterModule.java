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

package io.druid.server.initialization;

import com.fasterxml.jackson.databind.Module;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.emitter.core.ComposingEmitter;
import io.druid.java.util.emitter.core.Emitter;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.ManageLifecycle;
import io.druid.initialization.DruidModule;

import java.util.Collections;
import java.util.List;

/**
 */
public class ComposingEmitterModule implements DruidModule
{
  private static Logger log = new Logger(ComposingEmitterModule.class);

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.emitter.composing", ComposingEmitterConfig.class);
    JsonConfigProvider.bind(
        binder,
        "druid.event.emitter.composing",
        ComposingEmitterConfig.class,
        Names.named("event.composing")
    );
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.EMPTY_LIST;
  }

  @Provides
  @ManageLifecycle
  @Named("composing")
  public Emitter getEmitter(ComposingEmitterConfig config, final Injector injector)
  {
    return createEmitter(config, injector);
  }

  @Provides
  @ManageLifecycle
  @Named("event.composing")
  public Emitter getEventEmitter(@Named("event.composing") ComposingEmitterConfig config, final Injector injector)
  {
    return createEmitter(config, injector);
  }

  private Emitter createEmitter(ComposingEmitterConfig config, final Injector injector)
  {
    log.info("Creating Composing Emitter with %s", config.getEmitters());

    List<Emitter> emitters = Lists.transform(
        config.getEmitters(),
        new Function<String, Emitter>()
        {
          @Override
          public Emitter apply(String s)
          {
            return injector.getInstance(Key.get(Emitter.class, Names.named(s)));
          }
        }
    );

    return new ComposingEmitter(emitters);
  }
}

