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

package io.druid.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import io.druid.common.utils.StringUtils;
import io.druid.jackson.ObjectMappers;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.java.util.http.client.response.ClientResponse;
import io.druid.query.Query;
import io.druid.query.QueryInterruptedException;
import io.druid.query.QueryMetrics;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class StreamHandlerFactory
{
  private static final InputStream EOF = new ByteArrayInputStream(new byte[0]);

  protected final Logger log;
  protected final ObjectMapper mapper;

  public StreamHandlerFactory(Logger log, ObjectMapper mapper)
  {
    this.log = log;
    this.mapper = mapper;
  }

  public StreamHandler create(final Query query, final URL url, final int queueSize)
  {
    return new BaseHandler(query, String.format("%s:%s", url.getHost(), url.getPort()), queueSize);
  }

  private class BaseHandler implements StreamHandler
  {
    private final Query query;
    private final boolean disableLog;
    private final String host;

    private final long requestStartTimeNs = System.nanoTime();
    private long responseStartTimeNs;

    private final BlockingDeque<InputStream> queue;
    private final AtomicLong byteCount = new AtomicLong(0);
    private final AtomicBoolean done = new AtomicBoolean(false);

    private BaseHandler(Query query, String host, int queueSize)
    {
      this.query = query;
      this.disableLog = query.getContextBoolean(Query.DISABLE_LOG, false);
      this.host = host;
      this.queue = new LinkedBlockingDeque<InputStream>(queueSize <= 0 ? Integer.MAX_VALUE : queueSize);
    }

    @Override
    public void response(long requestStartTimeNs, long responseStartTimeNs) { }

    @Override
    public void finished(long requestStartTimeNs, long stopTimeNs, long byteCount) { }

    @Override
    public void handleHeader(HttpHeaders headers) throws IOException { }

    @Override
    public ClientResponse<InputStream> handleResponse(HttpResponse response)
    {
      response(requestStartTimeNs, responseStartTimeNs = System.nanoTime());

      HttpResponseStatus status = response.getStatus();
      if (!disableLog && log.isDebugEnabled()) {
        log.debug(
            "Initial response from url[%s] for [%s][%s:%s] with status[%s] in %,d msec",
            host, query.getId(), query.getType(), query.getDataSource(),
            status, TimeUnit.NANOSECONDS.toMillis(responseStartTimeNs - requestStartTimeNs)
        );
      }

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
        if (!done.get()) {
          queue.put(new ChannelBufferInputStream(response.getContent()));
        }
      }
      catch (final IOException e) {
        log.error(e, "Error parsing response context from url [%s]", host);
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
                  return !done.get() || !queue.isEmpty();
                }

                @Override
                public InputStream nextElement()
                {
                  try {
                    return done.get() && queue.isEmpty() ? EOF : queue.take();
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
      catch (Exception e) {
        // ignore
      }
      throw new IOException(new String(contents, Charsets.ISO_8859_1));
    }

    @Override
    public ClientResponse<InputStream> handleChunk(ClientResponse<InputStream> clientResponse, HttpChunk chunk)
    {
      final ChannelBuffer channelBuffer = chunk.getContent();
      final int bytes = channelBuffer.readableBytes();
      if (bytes > 0 && !done.get()) {
        try {
          queue.put(new ChannelBufferInputStream(channelBuffer));
        }
        catch (InterruptedException e) {
          log.error(e, "Unable to put finalizing input stream into Sequence queue for url [%s]", host);
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
      long stopTimeNs = System.nanoTime();
      long nodeTimeNs = stopTimeNs - requestStartTimeNs;
      if (!disableLog && log.isDebugEnabled()) {
        log.debug(
            "Completed [%s][%s:%s] request to url[%s] with %,d bytes in %,d msec [%s/s].",
            query.getId(), query.getType(), query.getDataSource(),
            host,
            byteCount.get(),
            TimeUnit.NANOSECONDS.toMillis(nodeTimeNs),
            StringUtils.toKMGT(byteCount.get() * 1000 / Math.max(1, TimeUnit.NANOSECONDS.toMillis(nodeTimeNs)))
        );
      }
      finished(responseStartTimeNs, stopTimeNs, byteCount.get());
      close();
      return ClientResponse.<InputStream>finished(clientResponse.getObj());
    }

    @Override
    public void exceptionCaught(final ClientResponse<InputStream> clientResponse, final Throwable e)
    {
      if (done.compareAndSet(false, true)) {
        queue.clear();
        queue.offer(new InputStream()
        {
          private final IOException ex = e instanceof IOException ? (IOException) e : new IOException(e);

          @Override
          public int read() throws IOException { throw ex; }

          @Override
          public int available() throws IOException { throw ex; }
        });
      }
    }

    @Override
    public void close()
    {
      if (done.compareAndSet(false, true) && queue.isEmpty()) {
        // An empty byte array is put at the end to give the SequenceInputStream.close() as something to close out
        // after done is set to true, regardless of the rest of the stream's state.
        queue.add(EOF);
      }
    }
  }

  private static final int WRITE_DELAY_LOG_THRESHOLD = 100;

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
        final int content,
        final String host,
        final int queueSize,
        final QueryMetrics queryMetrics,
        final Map<String, Object> context
    )
    {
      return new BaseHandler(query, host, queueSize)
      {
        @Override
        public void writeCompleted(long writeStart)
        {
          final long elapsed = System.currentTimeMillis() - writeStart;
          if (elapsed > WRITE_DELAY_LOG_THRESHOLD) {
            log.info(
                "Took %,d msec to write query[%s:%s] (%d bytes) to [%s]",
                elapsed, query.getType(), query.getId(), content, host
            );
          }
        }

        @Override
        public void handleHeader(HttpHeaders headers) throws IOException
        {
          // context may be null in case of error or query timeout
          String responseContext = headers.get("X-Druid-Response-Context");
          if (responseContext != null) {
            context.putAll(mapper.<Map<String, Object>>readValue(responseContext, ObjectMappers.MAP_REF));
          }
        }

        @Override
        public void response(long requestStartTimeNs, long responseStartTimeNs)
        {
          queryMetrics.reportNodeTimeToFirstByte(responseStartTimeNs - requestStartTimeNs);
        }

        @Override
        public void finished(long requestStartTimeNs, long stopTimeNs, long byteCount)
        {
          queryMetrics.reportNodeTime(stopTimeNs - requestStartTimeNs);
          queryMetrics.reportNodeBytes(byteCount);
          queryMetrics.emit(emitter);
        }
      };
    }
  }
}
