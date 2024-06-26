/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. SK Telecom licenses this file
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

package io.druid.java.util.metrics;

import io.druid.java.util.emitter.core.Event;
import io.druid.java.util.emitter.service.ServiceEmitter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class StubServiceEmitter extends ServiceEmitter
{
  private List<Event> events = new ArrayList<>();

  public StubServiceEmitter(String service, String host)
  {
    super(service, host, null);
  }

  @Override
  public void emit(Event event)
  {
    events.add(event);
  }

  public List<Event> getEvents()
  {
    return events;
  }

  @Override
  public void start()
  {
  }

  @Override
  public void flush() throws IOException
  {
  }

  @Override
  public void close() throws IOException
  {
  }
}
