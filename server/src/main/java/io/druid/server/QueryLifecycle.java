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
import com.google.common.collect.Maps;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.java.util.emitter.service.QueryEvent;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.query.DruidMetrics;
import io.druid.query.GenericQueryMetricsFactory;
import io.druid.query.Query;
import io.druid.query.QueryInterruptedException;
import io.druid.query.QueryMetrics;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.server.log.RequestLogger;
import io.druid.server.security.Access;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.AuthorizationInfo;
import io.druid.server.security.AuthorizationUtils;
import org.eclipse.jetty.io.EofException;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

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
public class QueryLifecycle
{
  public static enum State
  {
    NEW,
    INITIALIZED,
    AUTHORIZING,
    AUTHORIZED,
    EXECUTING,
    DONE
  }

  private static final EmittingLogger log = new EmittingLogger(QueryLifecycle.class);

  private final QueryManager queryManager;
  private final QueryToolChestWarehouse warehouse;
  private final QuerySegmentWalker texasRanger;
  private final GenericQueryMetricsFactory queryMetricsFactory;
  private final ServiceEmitter emitter;
  private final RequestLogger requestLogger;
  private final ObjectMapper jsonMapper;
  private final AuthConfig authConfig;
  private final long startMs;
  private final long startNs;

  private State state = State.NEW;
  private QueryToolChest toolChest;
  private Query query;

  public QueryLifecycle(
      final QueryManager queryManager,
      final QueryToolChestWarehouse warehouse,
      final QuerySegmentWalker texasRanger,
      final GenericQueryMetricsFactory queryMetricsFactory,
      final ServiceEmitter emitter,
      final RequestLogger requestLogger,
      final ObjectMapper jsonMapper,
      final AuthConfig authConfig,
      final long startMs,
      final long startNs
  )
  {
    this.queryManager = queryManager;
    this.warehouse = warehouse;
    this.texasRanger = texasRanger;
    this.queryMetricsFactory = queryMetricsFactory;
    this.emitter = emitter;
    this.requestLogger = requestLogger;
    this.jsonMapper = jsonMapper;
    this.authConfig = authConfig;
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
   * @param remoteAddress     remote address, for logging; or null if unknown
   *
   * @return results
   */
  @SuppressWarnings("unchecked")
  public <T> Sequence<T> runSimple(
      final Query<T> query,
      @Nullable final AuthorizationInfo authorizationInfo,
      @Nullable final String remoteAddress
  )
  {
    initialize(query);

    try {
      final Access access = authorize(authorizationInfo);
      if (!access.isAllowed()) {
        throw new ISE("Unauthorized");
      }
      return execute(Maps.newHashMap());
    }
    catch (Throwable e) {
      emitLogsAndMetrics(query, e, remoteAddress, -1, -1);
      throw e;
    }
  }

  /**
   * Initializes this object to execute a specific query. Does not actually execute the query.
   *
   * @param query the query
   */
  @SuppressWarnings("unchecked")
  public Query initialize(final Query query)
  {
    transition(State.NEW, State.INITIALIZED);
    this.query = query;
    this.toolChest = warehouse.getToolChest(query);
    return query;
  }

  /**
   * Authorize the query. Will return an Access object denoting whether the query is authorized or not.
   *
   * @param authorizationInfo authorization info from the request; or null if none is present. This must be non-null
   *                          if security is enabled, or the request will be considered unauthorized.
   *
   * @return authorization result
   *
   * @throws IllegalStateException if security is enabled and authorizationInfo is null
   */
  public Access authorize(@Nullable final AuthorizationInfo authorizationInfo)
  {
    transition(State.INITIALIZED, State.AUTHORIZING);

    if (authConfig.isEnabled()) {
      Access accessAuth = AuthorizationUtils.authorize(authorizationInfo, query.getDataSource().getNames());
      if (!accessAuth.isAllowed()) {
        transition(State.AUTHORIZING, State.DONE);
      } else {
        transition(State.AUTHORIZING, State.AUTHORIZED);
      }
      return accessAuth;
    } else {
      transition(State.AUTHORIZING, State.AUTHORIZED);
      return new Access(true);
    }
  }

  /**
   * Execute the query. Can only be called if the query has been authorized. Note that query logs and metrics will
   * not be emitted automatically when the Sequence is fully iterated. It is the caller's responsibility to call
   * {@link #emitLogsAndMetrics(Query, Throwable, String, long, int)} to emit logs and metrics.
   *
   * @return result sequence and response context
   */
  public Sequence execute(Map<String, Object> responseContext)
  {
    transition(State.AUTHORIZED, State.EXECUTING);
    return query.run(texasRanger, responseContext);
  }

  /**
   * Emit logs and metrics for this query.
   *
   * @param e             exception that occurred while processing this query
   * @param remoteAddress remote address, for logging; or null if unknown
   * @param bytesWritten  number of bytes written; will become a query/bytes metric if >= 0
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
    if (state == State.NEW) {
      return;   // nothing to emit
    }
    if (state == State.DONE) {
      log.warn("Tried to emit logs and metrics twice for query[%s]!", query.getId());
    }
    state = State.DONE;

    final boolean success = e == null || queryManager.isCanceled(query);
    final boolean interrupted = e instanceof QueryInterruptedException || e instanceof EofException;

    if (success) {
      log.debug("[%s] success", query.getId());
    } else if (interrupted) {
      log.info("[%s] interrupted[%s]", query.getId(), e.toString());
    } else {
      log.warn(e, "Exception occurred on request [%s]", forLog);
      // FIXME duplicated logging
      log.makeAlert(e, "Exception handling request")
         .addData("exception", e.toString())
         .addData("query", query)
         .addData("peer", remoteAddress)
         .emit();
    }

    try {
      final long queryTimeNs = System.nanoTime() - startNs;
      QueryMetrics metrics = DruidMetrics.makeRequestMetrics(
          queryMetricsFactory,
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

        if (interrupted) {
          statsMap.put("interrupted", true);
          statsMap.put("reason", e.toString());
        }
      }

      requestLogger.log(
          new RequestLogLine(
              new DateTime(startMs),
              Strings.nullToEmpty(remoteAddress),
              forLog,
              new QueryStats(statsMap)
          )
      );
      String queryStr = jsonMapper.writeValueAsString(forLog);
      if ("druid/broker".equals(emitter.getService())) {
        emitter.emit(
            new QueryEvent(
                DateTimes.utc(startMs),
                forLog.getId(),
                forLog.getSqlQueryId(),
                Strings.nullToEmpty(remoteAddress),
                queryStr,
                String.valueOf(success)
            ));
      }
    }
    catch (Exception ex) {
      log.error(ex, "Unable to log query [%s]!", forLog);
    }
  }

  private void transition(final State from, final State to)
  {
    if (state != from) {
      throw new ISE("Cannot transition from[%s] to[%s].", from, to);
    }
    state = to;
  }
}
