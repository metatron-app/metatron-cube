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

package io.druid.server.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Throwables;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.StringUtils;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Self;
import io.druid.java.util.common.IAE;
import io.druid.query.jmx.JMXQueryRunnerFactory;
import io.druid.server.DruidNode;
import io.druid.server.Shutdown;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.filter.AbstractFilter;
import org.apache.logging.log4j.core.layout.AbstractStringLayout;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.eclipse.jetty.io.EofException;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

@Path("/druid/admin")
public class AdminResource
{
  private final DruidNode node;
  private final Shutdown.Proc shutdown;
  private final ObjectMapper objectMapper;

  @Inject
  public AdminResource(
      @Self DruidNode node,
      @Shutdown Shutdown.Proc shutdown,
      @Json ObjectMapper objectMapper
  )
  {
    this.node = node;
    this.shutdown = shutdown;
    this.objectMapper = objectMapper;
  }

  @GET
  @Path("/ping")
  @Produces(MediaType.APPLICATION_JSON)
  public Response ping()
  {
    return Response.ok().build();
  }

  @GET
  @Path("/jmx")
  @Produces(MediaType.APPLICATION_JSON)
  public Response jmx(@QueryParam("pretty") String pretty)
  {
    final Object result = JMXQueryRunnerFactory.queryJMX(node, null, false).get(node.getHostAndPort());
    if (pretty != null) {
      final ObjectWriter writer = objectMapper.writerWithDefaultPrettyPrinter();
      try {
        return Response.ok(writer.writeValueAsString(result)).build();
      }
      catch (JsonProcessingException e) {
      }
    }
    return Response.ok(result).build();
  }

  @GET
  @Path("/stack")
  @Produces(MediaType.TEXT_PLAIN)
  public Response stack(@QueryParam("longest") String longest)
  {
    if (longest != null) {
      return Response.ok(GuavaUtils.dump(JMXQueryRunnerFactory.findLongestStack()), MediaType.TEXT_PLAIN).build();
    }
    final StreamingOutput output = new StreamingOutput()
    {
      @Override
      public void write(OutputStream output) throws IOException, WebApplicationException
      {
        final OutputStreamWriter writer = new OutputStreamWriter(output, StringUtils.UTF8_STRING);
        final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        for (long threadId : threadMXBean.getAllThreadIds()) {
          writer.write(GuavaUtils.dump(threadMXBean.getThreadInfo(threadId, Integer.MAX_VALUE)));
        }
        writer.flush();
      }
    };
    return Response.ok(output, MediaType.TEXT_PLAIN).build();
  }

  @GET
  @Path("/shutdown")
  @Produces(MediaType.APPLICATION_JSON)
  public Response shutdown(@QueryParam("timeout") Long timeout) throws IOException
  {
    if (timeout == null || timeout < 0) {
      shutdown.shutdown();
    } else {
      shutdown.shutdown(timeout);
    }
    return Response.ok().build();
  }

  @GET
  @Path("/logLevel/{name}")
  @Produces(MediaType.TEXT_PLAIN)
  public Response getLogLevel(@PathParam("name") String name)
  {
    return handleLogLevel(name, null);
  }

  @GET
  @Path("/logLevel/{name}/{level}")
  @Produces(MediaType.TEXT_PLAIN)
  public Response setLogLevel(@PathParam("name") String name, @PathParam("level") String levelString)
  {
    Level level = Level.toLevel(levelString, null);
    if (level == null) {
      throw new IAE("Invalid level [%s]", levelString);
    }
    return handleLogLevel(name, level);
  }

  private Response handleLogLevel(String name, Level level)
  {
    LoggerContext context = (LoggerContext) LogManager.getContext(false);
    Logger logger = "root".equalsIgnoreCase(name) ? context.getRootLogger() : context.getLogger(name);
    if (level != null) {
      logger.setLevel(level);
    }
    return Response.ok(String.format("%s --> %s", name, logger.getLevel())).build();
  }

  private static final int DEFAULT_LOG_QUEUE_SIZE = 1024;
  private static final int DEFAULT_LOG_POLLING_SEC = 300;   // 5 min

  @GET
  @Path("/log")
  @Produces(MediaType.TEXT_PLAIN)
  public Response log(
      @QueryParam("level") String level,
      @QueryParam("timeout") Integer timeout
  )
  {
    return Response.ok(streamLog(null, level, timeout), MediaType.TEXT_PLAIN).build();
  }

  @GET
  @Path("/log/{name}")
  @Produces(MediaType.TEXT_PLAIN)
  public Response log(
      @PathParam("name") String name,
      @QueryParam("level") String level,
      @QueryParam("timeout") Integer timeout
  )
  {
    return Response.ok(streamLog(name, level, timeout), MediaType.TEXT_PLAIN).build();
  }

  private StreamingOutput streamLog(final String name, final String level, final Integer timeout)
  {
    final LoggerContext context = (LoggerContext) LogManager.getContext(false);
    final Logger logger = StringUtils.isNullOrEmpty(name) ? context.getRootLogger() : context.getLogger(name);

    final Filter filter = toLevelFilter(level);
    final long polling = TimeUnit.SECONDS.toMillis(timeout == null || timeout <= 0 ? DEFAULT_LOG_POLLING_SEC : timeout);

    return new StreamingOutput()
    {
      @Override
      public void write(final OutputStream output) throws IOException, WebApplicationException
      {
        final BlockingQueue<LogEvent> queue = new LinkedBlockingDeque<>(DEFAULT_LOG_QUEUE_SIZE);
        final Appender appender = new AbstractAppender(UUID.randomUUID().toString(), filter, null)
        {
          @Override
          public void append(LogEvent event)
          {
            queue.offer(event);   // can leak
          }
        };
        logger.addAppender(appender);
        appender.start();

        final Layout<String> layout = getLayout(logger);
        final long limit = System.currentTimeMillis() + polling;
        try {
          for (long remaining = polling; remaining > 0; remaining = limit - System.currentTimeMillis()) {
            final LogEvent event = queue.poll(remaining, TimeUnit.MILLISECONDS);
            if (event == null) {
              break;
            }
            output.write(layout.toByteArray(event));
            if (queue.isEmpty()) {
              output.flush();
            }
          }
        }
        catch (Throwable t) {
          if (!(t instanceof EofException)) {
            throw Throwables.propagate(t);
          }
        }
        finally {
          logger.removeAppender(appender);
          appender.stop();
        }
      }
    };
  }

  private Filter toLevelFilter(String levelString)
  {
    final Level level = Level.toLevel(levelString, null);
    if (level == null) {
      return null;
    }
    return new AbstractFilter()
    {
      @Override
      public Result filter(LogEvent event)
      {
        return level.isLessSpecificThan(event.getLevel()) ? Result.ACCEPT : Result.DENY;
      }
    };
  }

  private Layout<String> getLayout(Logger rootLogger)
  {
    for (Appender appender : rootLogger.getAppenders().values()) {
      if (appender.getLayout() instanceof AbstractStringLayout) {
        return (AbstractStringLayout) appender.getLayout();
      }
    }
    return PatternLayout.createDefaultLayout();
  }
}
