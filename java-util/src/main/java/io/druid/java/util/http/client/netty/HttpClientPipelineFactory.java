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

package io.druid.java.util.http.client.netty;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.DefaultChannelPipeline;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.HttpClientCodec;
import org.jboss.netty.handler.codec.http.HttpContentDecompressor;

import java.net.ConnectException;

/**
 */
public class HttpClientPipelineFactory implements ChannelPipelineFactory
{
  @Override
  public ChannelPipeline getPipeline() throws Exception
  {
    ChannelPipeline pipeline = new DefaultChannelPipeline();

    pipeline.addLast("codec", new HttpClientCodec());
    pipeline.addLast("inflater", new HttpContentDecompressor());
    pipeline.addLast("connectionErrorHandler", new SimpleChannelUpstreamHandler()
    {
      @Override
      public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
      {
        Channel channel = ctx.getChannel();
        if (channel != null && channel.isOpen()) {
          channel.close();
        }
        if (e.getCause() instanceof ConnectException) {
          Thread.yield();   // some backoff?
        }
      }
    });

    return pipeline;
  }
}
