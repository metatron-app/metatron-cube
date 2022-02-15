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

package io.druid.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import io.druid.common.guava.Sequence;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.emitter.service.AlertBuilder;
import io.druid.java.util.emitter.service.QueryEvent;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.query.DruidMetrics;
import io.druid.query.GenericQueryMetricsFactory;
import io.druid.query.Query;
import io.druid.query.QueryException;
import io.druid.query.QueryMetrics;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.QueryUtils;
import io.druid.server.log.RequestLogger;
import io.druid.server.security.Access;
import io.druid.server.security.AuthenticationResult;
import io.druid.server.security.AuthorizationInfo;
import io.druid.server.security.AuthorizationUtils;
import io.druid.server.security.AuthorizerMapper;
import io.druid.server.security.ForbiddenException;
import org.eclipse.jetty.io.EofException;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import java.io.EOFException;
import java.io.InterruptedIOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Class that helps a Druid server (broker, historical, etc) manage the lifecycle of a query that it is handling. It
 * ensures that a query goes through the following stages, in the proper order:
 *
 * <ol>
 * <li>Initialization ({@link #initialize(Query)})</li>
 * <li>Authorization ({@link #authorize(AuthorizationInfo)}</li>
 * <li>Execution ({@link #execute(Map)}</li>
 * <li>Logging ({@link #emitLogsAndMetrics(Query, Throwable, String, long, int)}</li>
 * </ol>
 *
 * This object is not thread-safe.
 */
public class  QueryLifecycle
{
  public static enum State
  {
    INITIALIZED,
    AUTHORIZING,
    AUTHORIZED,
    EXECUTING,
    UNAUTHORIZED,
    DONE
  }

  private static final Logger log = new Logger(QueryLifecycle.class);

  private final Query<?> query;
  private final QueryManager queryManager;
  private final QueryToolChest toolChest;
  private final QuerySegmentWalker segmentWalker;
  private final GenericQueryMetricsFactory metricsFactory;
  private final ServiceEmitter emitter;
  private final RequestLogger requestLogger;
  private final ObjectMapper jsonMapper;
  private final AuthorizerMapper authorizerMapper;
  private final long startMs;
  private final long startNs;

  private State state = State.INITIALIZED;

  public QueryLifecycle(
      final Query<?> query,
      final QueryManager queryManager,
      final QueryToolChestWarehouse warehouse,
      final QuerySegmentWalker segmentWalker,
      final GenericQueryMetricsFactory metricsFactory,
      final ServiceEmitter emitter,
      final RequestLogger requestLogger,
      final ObjectMapper jsonMapper,
      final AuthorizerMapper authorizerMapper,
      final long startMs,
      final long startNs
  )
  {
    this.query = query;
    this.queryManager = queryManager;
    this.toolChest = warehouse.getToolChest(query);
    this.segmentWalker = segmentWalker;
    this.metricsFactory = metricsFactory;
    this.emitter = emitter;
    this.requestLogger = requestLogger;
    this.jsonMapper = jsonMapper;
    this.authorizerMapper = authorizerMapper;
    this.startMs = startMs;
    this.startNs = startNs;
  }

  /**
   * For callers where simplicity is desired over flexibility. This method does it all in one call. If the request
   * is unauthorized, an IllegalStateException will be thrown. Logs and metrics are emitted when the Sequence is
   * either fully iterated or throws an exception.
   *
   * @param query             the query
   * @param authorizationInfo authorization info from the request; or null if none is present. This must be non-null
   *                          if security is enabled, or the request will be considered unauthorized.
   * @return results
   */
  @SuppressWarnings("unchecked")
  public <T> Sequence<T> runSimple(@Nullable final AuthenticationResult authenticationResult)
  {
    try {
      final Access access = authorize(query, authenticationResult);
      if (!access.isAllowed()) {
        throw new ForbiddenException(access.toString());
      }
      return execute(query, Maps.newHashMap());
    }
    catch (Throwable e) {
      emitLogsAndMetrics(QueryUtils.forLog(query), e, null, -1, -1);
      throw e;
    }
  }

  /**
   * Authorize the query. Will return an Access object denoting whether the query is authorized or not.
   *
   * @param authenticationResult authentication result indicating the identity of the requester
   *
   * @return authorization result
   */
  public Access authorize(final Query<?> query, final AuthenticationResult authenticationResult)
  {
    transition(State.INITIALIZED, State.AUTHORIZING);
    return doAuthorize(
        AuthorizationUtils.authorizeAllResourceActions(
            authenticationResult,
            Iterables.transform(
                query.getDataSource().getNames(),
                AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR
            ),
            authorizerMapper
        )
    );
  }

  /**
   * Authorize the query. Will return an Access object denoting whether the query is authorized or not.
   *
   * @param req HTTP request object of the request. If provided, the auth-related fields in the HTTP request
   *            will be automatically set.
   *
   * @return authorization result
   */
  public Access authorize(final Query<?> query, HttpServletRequest req)
  {
    transition(State.INITIALIZED, State.AUTHORIZING);
    return doAuthorize(
        AuthorizationUtils.authorizeAllResourceActions(
            req,
            Iterables.transform(
                QueryUtils.getAllDataSources(query),
                AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR
            ),
            authorizerMapper
        )
    );
  }

  private Access doAuthorize(final Access authorizationResult)
  {
    transition(State.AUTHORIZING, authorizationResult.isAllowed() ? State.AUTHORIZED : State.UNAUTHORIZED);
    return authorizationResult;
  }

  /**
   * Execute the query. Can only be called if the query has been authorized. Note that query logs and metrics will
   * not be emitted automatically when the Sequence is fully iterated. It is the caller's responsibility to call
   * {@link #emitLogsAndMetrics(Query, Throwable, String, long, int)} to emit logs and metrics.
   *
   * @return result sequence and response context
   */
  public Sequence execute(Query<?> prepared, Map<String, Object> responseContext)
  {
    transition(State.AUTHORIZED, State.EXECUTING);
    return prepared.run(segmentWalker, responseContext);
  }

  /**
   * Emit logs and metrics for this query.
   *
   * @param e             exception that occurred while processing this query
   * @param remoteAddress remote address, for logging; or null if unknown
   * @param bytesWritten  number of bytes written; will become a query/bytes metric if >= 0
   * @param rows          number of rows; will become a query/bytes metric if >= 0
   */
  @SuppressWarnings("unchecked")
  public void emitLogsAndMetrics(
      final Query forLog,
      @Nullable final Throwable e,
      @Nullable final String remoteAddress,
      final long bytesWritten,
      final int rows
  )
  {
    if (state == State.DONE) {
      log.warn("Tried to emit logs and metrics twice for query[%s]!", query.getId());
    }
    state = State.DONE;

    final boolean success = e == null || queryManager.isCancelled(query.getId());
    final boolean interrupted = !success && (isInterrupted(e) || queryManager.isTimedOut(query.getId()));

    if (success) {
      log.debug("[%s] success", query.getId());
    } else if (interrupted) {
      log.info("[%s] interrupted[%s]", query.getId(), e.toString());
    } else {
      QueryException.warn(log, e, "Exception occurred on request: %s", forLog);
      emitter.emit(
          AlertBuilder.create(e, "Exception occurred on request [%s]", forLog)
                      .addData("exception", e.toString())
                      .addData("query", forLog)
                      .addData("peer", remoteAddress)
      );
    }

    final long queryTimeNs = System.nanoTime() - startNs;
    try {
      QueryMetrics metrics = DruidMetrics.makeRequestMetrics(
          metricsFactory,
          toolChest,
          query,
          Strings.nullToEmpty(remoteAddress)
      );
      metrics.success(success);
      metrics.reportQueryTime(queryTimeNs);

      if (rows >= 0 ) {
        metrics.reportQueryRows(rows);
      }
      if (bytesWritten >= 0) {
        metrics.reportQueryBytes(bytesWritten);
      }

      metrics.emit(emitter);

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
              forLog,
              new QueryStats(statsMap)
          )
      );
      if (Objects.equals(ServiceTypes.BROKER, emitter.getType())) {
        queryManager.finished(query.getId());
        emitter.emit(
            new QueryEvent(
                DateTimes.utc(startMs),
                Strings.nullToEmpty(remoteAddress),
                forLog.getId(),
                jsonMapper.writeValueAsString(forLog),
                String.valueOf(success),
                Long.valueOf(TimeUnit.NANOSECONDS.toMillis(queryTimeNs)),
                Long.valueOf(bytesWritten),
                Integer.valueOf(rows),
                Optional.ofNullable(e).map(Throwable::toString).orElse(""),
                Optional.ofNullable(e).map(throwable -> String.valueOf(interrupted)).orElse("false")
            ));
      }
    }
    catch (Exception ex) {
      log.error(ex, "Unable to log query [%s]!", forLog);
    }
  }

  public static boolean isInterrupted(@Nullable Throwable e)
  {
    for (; e != null; e = e.getCause()) {
      if (e instanceof InterruptedIOException ||
          e instanceof InterruptedException ||
          e instanceof TimeoutException ||
          e instanceof org.jboss.netty.handler.timeout.TimeoutException ||
          e instanceof CancellationException) {
        return true;
      }
    }
    return false;
  }

  public static boolean isEOF(@Nullable Throwable e)
  {
    for (; e != null; e = e.getCause()) {
      if (e instanceof EOFException || e instanceof EofException) {
        return true;
      }
    }
    return false;
  }

  private void transition(final State from, final State to)
  {
    if (state != from) {
      throw new ISE("Cannot transition from[%s] to[%s].", from, to);
    }
    state = to;
  }
}
