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

package io.druid.server.initialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.google.inject.util.Providers;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.core.Emitter;
import com.metamx.emitter.core.HttpEmitterConfig;
import com.metamx.emitter.core.HttpPostEmitter;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.ManageLifecycle;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.JdkSslContext;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;

import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import java.io.Closeable;
import java.security.NoSuchAlgorithmException;

/**
 */
public class HttpEmitterModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.emitter.http", HttpEmitterConfig.class);
    JsonConfigProvider.bind(binder, "druid.event.emitter.http", HttpEmitterConfig.class, Names.named("event.http"));

    final SSLContext context;
    try {
      context = SSLContext.getDefault();
    } catch (NoSuchAlgorithmException e) {
      throw Throwables.propagate(e);
    }

    binder.bind(SSLContext.class).toProvider(Providers.of(context)).in(LazySingleton.class);
  }

  @Provides
  @ManageLifecycle
  @Named("http")
  public Emitter getEmitter(
      Supplier<HttpEmitterConfig> config,
      @Nullable SSLContext sslContext,
      Lifecycle lifecycle,
      ObjectMapper jsonMapper
  )
  {
    final DefaultAsyncHttpClientConfig.Builder builder = new DefaultAsyncHttpClientConfig.Builder();
    if (sslContext != null) {
      builder.setSslContext(new JdkSslContext(sslContext, true, ClientAuth.NONE));
    }
    final AsyncHttpClient client = new DefaultAsyncHttpClient(builder.build());
    lifecycle.addHandler(new CloseableHandler(client));

    return new HttpPostEmitter(config.get(), client, jsonMapper);
  }

  private static class CloseableHandler implements Lifecycle.Handler
  {
    private static final Logger log = new Logger(CloseableHandler.class);
    private final Closeable o;

    private CloseableHandler(Closeable o)
    {
      this.o = o;
    }

    @Override
    public void start() throws Exception
    {
      // do nothing
    }

    @Override
    public void stop()
    {
      log.info("Closing object[%s]", o);
      try {
        o.close();
      }
      catch (Exception e) {
        log.error(e, "Exception when closing object [%s]", o);
      }
    }
  }
}
