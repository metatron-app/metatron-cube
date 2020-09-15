/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.java.util.emitter.service;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.java.util.emitter.core.Emitter;
import io.druid.java.util.emitter.core.Event;

import java.io.IOException;

public class ServiceEmitter implements Emitter
{
  private final ImmutableMap<String, String> serviceDimensions;
  private final Emitter emitter;

  @VisibleForTesting
  public ServiceEmitter(String service, String host, Emitter emitter)
  {
    this(service, host, null, emitter, ImmutableMap.<String, String>of());
  }

  public ServiceEmitter(
      String service,
      String host,
      String type,
      Emitter emitter,
      ImmutableMap<String, String> otherServiceDimensions
  )
  {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.<String, String>builder();
    builder.put("service", Preconditions.checkNotNull(service))
           .put("host", Preconditions.checkNotNull(host));
    if (type != null) {
      builder.put("servicetype", type);   // 'type' conflicts with query type
    }
    builder.putAll(otherServiceDimensions);
    this.serviceDimensions = builder.build();
    this.emitter = emitter;
  }

  public String getService()
  {
    return serviceDimensions.get("service");
  }

  public String getHost()
  {
    return serviceDimensions.get("host");
  }

  public String getType()
  {
    return serviceDimensions.get("servicetype");
  }

  @LifecycleStart
  public void start()
  {
    emitter.start();
  }

  public void emit(Event event)
  {
    emitter.emit(event);
  }

  public void emit(ServiceEventBuilder builder)
  {
    emit(builder.build(serviceDimensions));
  }

  public void flush() throws IOException
  {
    emitter.flush();
  }

  @LifecycleStop
  public void close() throws IOException
  {
    emitter.close();
  }

  @Override
  public String toString()
  {
    return "ServiceEmitter{" +
           "serviceDimensions=" + serviceDimensions +
           ", emitter=" + emitter +
           '}';
  }
}
