/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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

package io.druid.sql;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import io.druid.client.BrokerServerView;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.emitter.service.QueryEvent;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.java.util.emitter.service.ServiceMetricEvent;
import io.druid.server.QueryLifecycle;
import io.druid.server.QueryStats;
import io.druid.server.RequestLogLine;
import io.druid.server.log.RequestLogger;
import io.druid.server.security.Access;
import io.druid.server.security.AuthenticationResult;
import io.druid.server.security.AuthorizationUtils;
import io.druid.server.security.AuthorizerMapper;
import io.druid.sql.calcite.planner.DruidPlanner;
import io.druid.sql.calcite.planner.PlannerContext;
import io.druid.sql.calcite.planner.PlannerResult;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Similar to {@link io.druid.server.QueryLifecycle}, this class manages the lifecycle of a SQL query.
 * It ensures that a SQL query goes through the following stages, in the proper order:
 *
 * <ol>
 * <li>Initialization ({@link #initialize(String, Map)})</li>
 * <li>Planning ({@link #plan(HttpServletRequest)} or {@link #plan(AuthenticationResult)})</li>
 * <li>Authorization ({@link #authorize()})</li>
 * <li>Execution ({@link #execute()})</li>
 * <li>Logging ({@link #emitLogsAndMetrics(Throwable, String, long)})</li>
 * </ol>
 *
 * <p>Unlike QueryLifecycle, this class is designed to be <b>thread safe</b> so that it can be used in multi-threaded
 * scenario (JDBC) without external synchronization.
 */
public class SqlLifecycle
{
  enum State
  {
    INITIALIZED,
    PLANNED,
    AUTHORIZING,
    AUTHORIZED,
    EXECUTING,
    UNAUTHORIZED,
    DONE
  }

  private static final Logger log = new Logger(SqlLifecycle.class);

  private final String sql;
  private final BrokerServerView brokerServerView;
  private final ServiceEmitter emitter;
  private final RequestLogger requestLogger;
  private final DruidPlanner planner;
  private final PlannerContext plannerContext;
  private final AuthorizerMapper authorizerMapper;
  private final long startMs;
  private final long startNs;

  private final Object lock = new Object();
  private State state = State.INITIALIZED;
  private PlannerResult plannerResult;

  public SqlLifecycle(
      String sql,
      BrokerServerView brokerServerView,
      ServiceEmitter emitter,
      RequestLogger requestLogger,
      DruidPlanner planner,
      AuthorizerMapper authorizerMapper,
      long startMs,
      long startNs
  )
  {
    this.sql = Preconditions.checkNotNull(sql);
    this.brokerServerView = brokerServerView;
    this.emitter = emitter;
    this.requestLogger = requestLogger;
    this.planner = planner;
    this.plannerContext = planner.getPlannerContext();
    this.authorizerMapper = authorizerMapper;
    this.startMs = startMs;
    this.startNs = startNs;
  }

  public String getSQL()
  {
    return sql;
  }

  public PlannerContext getPlannerContext()
  {
    return plannerContext;
  }

  public String getQueryId()
  {
    return plannerContext.getQueryId();
  }

  public PlannerResult plan() throws ValidationException, RelConversionException, SqlParseException
  {
    synchronized (lock) {
      transition(State.INITIALIZED, State.PLANNED);
      try (DruidPlanner planner = this.planner) {
        return plannerResult = planner.plan(sql, brokerServerView);
      }
    }
  }

  public Access authorize()
  {
    synchronized (lock) {
      transition(State.PLANNED, State.AUTHORIZING);
      return doAuthorize(AuthorizationUtils.authorizeAllResourceActions(
          plannerContext.getAuthenticationResult(),
          Iterables.transform(plannerResult.datasourceNames(), AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR),
          authorizerMapper
      ));
    }
  }

  // for sql resource
  public Access authorize(HttpServletRequest req)
      throws SqlParseException, RelConversionException, ValidationException
  {
    synchronized (lock) {
      transition(State.PLANNED, State.AUTHORIZING);
      return doAuthorize(AuthorizationUtils.authorizeAllResourceActions(
          req,
          Iterables.transform(plannerResult.datasourceNames(), AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR),
          authorizerMapper
      ));
    }
  }

  private Access doAuthorize(final Access authorizationResult)
  {
    transition(State.AUTHORIZING, authorizationResult.isAllowed() ? State.AUTHORIZED : State.UNAUTHORIZED);
    return authorizationResult;
  }

  public Sequence<Object[]> execute()
  {
    synchronized (lock) {
      transition(State.AUTHORIZED, State.EXECUTING);
      return plannerResult.run();
    }
  }

  /**
   * Emit logs and metrics for this query.
   * @param e             exception that occurred while processing this query
   * @param remoteAddress remote address, for logging; or null if unknown
   * @param bytesWritten  number of bytes written; will become a query/bytes metric if >= 0
   */
  public void emitLogsAndMetrics(
      @Nullable final Throwable e,
      @Nullable final String remoteAddress,
      final long bytesWritten,
      final int rows
  )
  {
    synchronized (lock) {

      if (state == State.DONE) {
        log.warn("Tried to emit logs and metrics twice for query[%s]!", getQueryId());
      }

      state = State.DONE;

      final boolean success = e == null || plannerContext.isCanceled();
      final boolean interrupted = QueryLifecycle.isInterrupted(e);

      final long queryTimeNs = System.nanoTime() - startNs;
      try {
        ServiceMetricEvent.Builder metricBuilder = ServiceMetricEvent.builder();
        metricBuilder.setDimension("id", getQueryId());
        if (plannerResult != null) {
          metricBuilder.setDimension("dataSource", plannerResult.datasourceNames().toString());
        }
        metricBuilder.setDimension("remoteAddress", Strings.nullToEmpty(remoteAddress));
        metricBuilder.setDimension("success", String.valueOf(success));

        emitter.emit(metricBuilder.build("query/time", TimeUnit.NANOSECONDS.toMillis(queryTimeNs)));
        if (rows >= 0) {
          emitter.emit(metricBuilder.build("query/rows", rows));
        }
        if (bytesWritten >= 0) {
          emitter.emit(metricBuilder.build("query/bytes", bytesWritten));
        }

        final Map<String, Object> statsMap = new LinkedHashMap<>();
        statsMap.put("query/time", TimeUnit.NANOSECONDS.toMillis(queryTimeNs));
        statsMap.put("query/bytes", bytesWritten);
        statsMap.put("query/rows", rows);
        statsMap.put("success", success);
        if (e != null) {
          statsMap.put("exception", e.toString());
          statsMap.put("interrupted", interrupted);
        }

        requestLogger.log(
            new RequestLogLine(
                DateTimes.utc(startMs),
                Strings.nullToEmpty(remoteAddress),
                sql,
                new QueryStats(statsMap)
            )
        );
        if ("druid/broker".equals(emitter.getService())) {
          emitter.emit(
              new QueryEvent(
                  DateTimes.utc(startMs),
                  getQueryId(),
                  Strings.nullToEmpty(remoteAddress),
                  sql,
                  String.valueOf(success)
              ));
        }
      }
      catch (Throwable ex) {
        log.error(ex, "Unable to log sql [%s]!", sql);
      }
    }
  }

  private void transition(final State from, final State to)
  {
    if (state != from) {
      throw new ISE("Cannot transition from[%s] to[%s] because current state[%s] is not [%s].", from, to, state, from);
    }
    state = to;
  }
}
