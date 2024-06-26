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

package io.druid.java.util.http.client.response;

import org.jboss.netty.handler.codec.http.HttpResponseStatus;

/**
 */
public class StatusResponseHolder
{
  private final HttpResponseStatus status;
  private final StringBuilder builder;

  public StatusResponseHolder(
      HttpResponseStatus status,
      StringBuilder builder
  )
  {
    this.status = status;
    this.builder = builder;
  }

  public HttpResponseStatus getStatus()
  {
    return status;
  }

  public StringBuilder getBuilder()
  {
    return builder;
  }

  public String getContent()
  {
    return builder.toString();
  }
}
