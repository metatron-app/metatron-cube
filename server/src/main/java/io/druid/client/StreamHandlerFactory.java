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

package io.druid.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import com.metamx.http.client.response.ClientResponse;
import io.druid.query.Query;
import io.druid.query.QueryInterruptedException;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class StreamHandlerFactory
{
  protected static final Logger debug = new Logger(StreamHandlerFactory.class);

  private static final TypeReference<Map<String, Object>> CONTEXT_TYPE = new TypeReference<Map<String, Object>>()
  {
  };

  protected final Logger log;
  protected final ObjectMapper mapper;

  public StreamHandlerFactory(Logger log, ObjectMapper mapper)
  {
    this.log = log;
    this.mapper = mapper;
  }

  public StreamHandler create(final String queryId, final URL url, final int queueSize)
  {
    return new BaseHandler(queryId, url, queueSize);
  }

  private class BaseHandler implements StreamHandler
  {
    private final String queryId;
    private final URL url;

    private final long requestStartTime = System.currentTimeMillis();
    private long responseStartTime;

    private final BlockingDeque<InputStream> queue;
    private final AtomicLong byteCount = new AtomicLong(0);
    private final AtomicBoolean done = new AtomicBoolean(false);

    private BaseHandler(String queryId, URL url, int queueSize)
    {
      this.queryId = queryId;
      this.url = url;
      this.queue = new LinkedBlockingDeque<>(queueSize <= 0 ? Integer.MAX_VALUE : queueSize);
    }

    @Override
    public void response(long requestStartTime, long responseStartTime) { }

    @Override
    public void finished(long requestStartTime, long stopTime, long byteCount) { }

    @Override
    public void handleHeader(HttpHeaders headers) throws IOException { }

    @Override
    public ClientResponse<InputStream> handleResponse(HttpResponse response)
    {
      response(requestStartTime, responseStartTime = System.currentTimeMillis());

      HttpResponseStatus status = response.getStatus();
      log.debug(
          "Initial response from url[%s] for queryId[%s] with status[%s] in %,d msec",
          url, queryId, status, responseStartTime - requestStartTime
      );

      if (status.getCode() >= HttpResponseStatus.INTERNAL_SERVER_ERROR.getCode()) {
        final ByteBuffer contents = response.getContent().toByteBuffer();
        final byte[] binary = new byte[contents.remaining()];
        contents.get(binary);

        return ClientResponse.<InputStream>finished(
            new InputStream()
            {
              @Override
              public int read() throws IOException
              {
                throw toException(binary);
              }

              @Override
              public int available() throws IOException
              {
                throw toException(binary);
              }
            }
        );
      }

      try {
        handleHeader(response.headers());
        queue.put(new ChannelBufferInputStream(response.getContent()));
      }
      catch (final IOException e) {
        log.error(e, "Error parsing response context from url [%s]", url);
        return ClientResponse.<InputStream>finished(
            new InputStream()
            {
              @Override
              public int read() throws IOException
              {
                throw e;
              }
            }
        );
      }
      catch (InterruptedException e) {
        log.error(e, "Queue appending interrupted");
        Thread.currentThread().interrupt();
        throw Throwables.propagate(e);
      }
      byteCount.addAndGet(response.getContent().readableBytes());
      return ClientResponse.<InputStream>finished(
          new SequenceInputStream(
              new Enumeration<InputStream>()
              {
                @Override
                public boolean hasMoreElements()
                {
                  // Done is always true until the last stream has be put in the queue.
                  // Then the stream should be spouting good InputStreams.
                  synchronized (done) {
                    return !done.get() || !queue.isEmpty();
                  }
                }

                @Override
                public InputStream nextElement()
                {
                  try {
                    return queue.take();
                  }
                  catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw Throwables.propagate(e);
                  }
                }
              }
          )
          {
            @Override
            public void close() throws IOException
            {
              queue.clear();  // nothing to close in ChannelBufferInputStream
            }
          }
      );
    }

    private IOException toException(byte[] contents) throws IOException
    {
      try {
        throw mapper.readValue(contents, QueryInterruptedException.class);
      }
      catch (IOException e) {
        // ignore
      }
      throw new IOException(new String(contents, Charsets.ISO_8859_1));
    }

    @Override
    public ClientResponse<InputStream> handleChunk(ClientResponse<InputStream> clientResponse, HttpChunk chunk)
    {
      final ChannelBuffer channelBuffer = chunk.getContent();
      final int bytes = channelBuffer.readableBytes();
      if (bytes > 0) {
        debug.debug("Chunk arrived from url[%s] for queryId[%s] with length[%d]", url, queryId, bytes);
        try {
          queue.put(new ChannelBufferInputStream(channelBuffer));
        }
        catch (InterruptedException e) {
          log.error(e, "Unable to put finalizing input stream into Sequence queue for url [%s]", url);
          Thread.currentThread().interrupt();
          throw Throwables.propagate(e);
        }
        byteCount.addAndGet(bytes);
      }
      return clientResponse;
    }

    @Override
    public ClientResponse<InputStream> done(ClientResponse<InputStream> clientResponse)
    {
      long stopTime = System.currentTimeMillis();
      log.debug(
          "Completed queryId[%s] request to url[%s] with %,d bytes returned in %,d millis [%,f b/s].",
          queryId,
          url,
          byteCount.get(),
          stopTime - responseStartTime,
          byteCount.get() / (0.0001 * (stopTime - responseStartTime))
      );
      finished(responseStartTime, stopTime, byteCount.get());
      synchronized (done) {
        done.set(true);
        // An empty byte array is put at the end to give the SequenceInputStream.close() as something to close out
        // after done is set to true, regardless of the rest of the stream's state.
        queue.offer(new ByteArrayInputStream(new byte[0]));
      }
      return ClientResponse.<InputStream>finished(clientResponse.getObj());
    }

    @Override
    public void exceptionCaught(final ClientResponse<InputStream> clientResponse, final Throwable e)
    {
      final InputStream thrower = new InputStream()
      {
        @Override
        public int read() throws IOException { throw new IOException(e); }
      };
      // Don't wait for lock in case the lock had something to do with the error
      synchronized (done) {
        done.set(true);
        queue.offer(thrower);
      }
    }

    @Override
    public void close() throws IOException
    {
      synchronized (done) {
        done.set(true);
        queue.clear();  // blocked worker in handleChunk prevents shutdown process. I don't know why
      }
    }
  }

  public static class WithEmitter extends StreamHandlerFactory
  {
    private final ServiceEmitter emitter;

    public WithEmitter(Logger log, ServiceEmitter emitter, ObjectMapper mapper)
    {
      super(log, mapper);
      this.emitter = emitter;
    }

    public StreamHandler create(
        final Query query,
        final URL url,
        final int queueSize,
        final ServiceMetricEvent.Builder builder,
        final Map<String, Object> context
    )
    {
      return new BaseHandler(query.getId(), url, queueSize)
      {
        @Override
        public void handleHeader(HttpHeaders headers) throws IOException
        {
          // context may be null in case of error or query timeout
          String responseContext = headers.get("X-Druid-Response-Context");
          if (responseContext != null) {
            context.putAll(mapper.<Map<String, Object>>readValue(responseContext, CONTEXT_TYPE));
          }
        }

        @Override
        public void response(long requestStartTime, long responseStartTime)
        {
          emitter.emit(builder.build("query/node/ttfb", responseStartTime - requestStartTime));
        }

        @Override
        public void finished(long requestStartTime, long stopTime, long byteCount)
        {
          emitter.emit(builder.build("query/node/time", stopTime - requestStartTime));
          emitter.emit(builder.build("query/node/bytes", byteCount));
        }
      };
    }
  }
}
