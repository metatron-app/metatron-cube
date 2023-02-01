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

package io.druid.java.util.http.client;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ForwardingListenableFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.druid.java.util.common.logger.Logger;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ChannelFuture;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;

public class ChannelResources
{
  private static final Logger LOG = new Logger(ChannelResource.class);

  public static final ChannelResource CLOSED = new ForwardingResource<>(
      Futures.immediateFailedFuture(new ClosedChannelException())
  );

  public static <T> ChannelResource<T> immediateFailed(ChannelFuture channelFuture)
  {
    Preconditions.checkArgument(!channelFuture.isSuccess());
    return new ForwardingResource<T>(Futures.<T>immediateFailedFuture(
        new ChannelException("Faulty channel in resource pool", channelFuture.getCause())
    ));
  }

  public static <T> ChannelResource<T> wrap(T result)
  {
    return wrap(Futures.immediateFuture(result));
  }

  public static <T> ChannelResource<T> wrap(ListenableFuture<T> future)
  {
    return new ForwardingResource<T>(future);
  }

  public static <T> ChannelResource<T> wrap(ListenableFuture<T> future, Closeable resource)
  {
    return new ForwardingResource<T>(future)
    {
      @Override
      public void close() throws IOException
      {
        resource.close();
      }
    };
  }

  public static <T> ChannelResource<T> wrap(SettableFuture<T> retVal, Channel channel)
  {
    return new ForwardingResource<T>(retVal)
    {
      @Override
      public void close() throws IOException
      {
        LOG.debug(new Exception(), "Closing.. %s", channel);
        retVal.cancel(true);
        channel.close();
      }
    };
  }

  private static class ForwardingResource<T> extends ForwardingListenableFuture<T> implements ChannelResource<T>
  {
    private final ListenableFuture<T> future;

    protected ForwardingResource(ListenableFuture<T> future) {this.future = future;}

    @Override
    protected ListenableFuture<T> delegate()
    {
      return future;
    }

    @Override
    public void close() throws IOException
    {
    }
  }
}
