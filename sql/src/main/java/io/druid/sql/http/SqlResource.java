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

package io.druid.sql.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CountingOutputStream;
import com.google.inject.Inject;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Yielder;
import io.druid.java.util.common.logger.Logger;
import io.druid.common.Yielders;
import io.druid.guice.annotations.Json;
import io.druid.query.QueryInterruptedException;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.AuthorizationInfo;
import io.druid.server.security.ForbiddenException;
import io.druid.sql.SqlLifecycle;
import io.druid.sql.SqlLifecycleFactory;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.planner.PlannerContext;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang.mutable.MutableInt;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Path("/druid/v2/sql/")
public class SqlResource
{
  private static final Logger log = new Logger(SqlResource.class);

  private final ObjectMapper jsonMapper;
  private final SqlLifecycleFactory sqlLifecycleFactory;

  @Inject
  public SqlResource(
      @Json ObjectMapper jsonMapper,
      SqlLifecycleFactory sqlLifecycleFactory
  )
  {
    this.jsonMapper = Preconditions.checkNotNull(jsonMapper, "jsonMapper");
    this.sqlLifecycleFactory = Preconditions.checkNotNull(sqlLifecycleFactory, "sqlLifecycleFactory");
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.TEXT_PLAIN)
  public Response doPost(
      String sqlQuery,
      @Context HttpServletRequest req
  ) throws SQLException, IOException
  {
    return execute(new SqlQuery(sqlQuery, null, false, null), ImmutableMap.of(), req);
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response doPost(
      SqlQuery sqlQuery,
      @Context HttpServletRequest req
  ) throws SQLException, IOException
  {
    return execute(sqlQuery, sqlQuery.getContext(), req);
  }

  private Response execute(
      final SqlQuery sqlQuery,
      final Map<String, Object> context,
      final HttpServletRequest req
  ) throws SQLException, IOException
  {
    final DateTimeZone timeZone;

    final SqlLifecycle lifecycle = sqlLifecycleFactory.factorize();
    final String sqlQueryId = lifecycle.initialize(sqlQuery.getQuery(), sqlQuery.getContext());
    final String remoteAddr = req.getRemoteAddr();

    final Thread currThread = Thread.currentThread();
    final String currThreadName = resetThreadName(currThread);

    final String query = sqlQuery.getQuery();
    try {
      currThread.setName(String.format("%s[sql_%s]", currThreadName, sqlQueryId));

      final PlannerContext plannerContext =
          lifecycle.planAndAuthorize((AuthorizationInfo) req.getAttribute(AuthConfig.DRUID_AUTH_TOKEN));
      timeZone = plannerContext.getTimeZone();

      // Remember which columns are time-typed, so we can emit ISO8601 instead of millis values.
      // Also store list of all column names, for X-Druid-Sql-Columns header.
      final List<RelDataTypeField> fieldList = lifecycle.rowType().getFieldList();
      final boolean[] timeColumns = new boolean[fieldList.size()];
      final boolean[] dateColumns = new boolean[fieldList.size()];
      final String[] columnNames = new String[fieldList.size()];

      for (int i = 0; i < fieldList.size(); i++) {
        final SqlTypeName sqlTypeName = fieldList.get(i).getType().getSqlTypeName();
        timeColumns[i] = sqlTypeName == SqlTypeName.TIMESTAMP;
        dateColumns[i] = sqlTypeName == SqlTypeName.DATE;
        columnNames[i] = fieldList.get(i).getName();
      }

      final Yielder<Object[]> yielder0 = Yielders.each(lifecycle.execute());

      try {
        return Response.ok(
            new StreamingOutput()
            {
              @Override
              public void write(final OutputStream outputStream) throws IOException, WebApplicationException
              {
                Exception e = null;
                Yielder<Object[]> yielder = yielder0;

                final MutableInt counter = new MutableInt();
                final CountingOutputStream os = new CountingOutputStream(outputStream);
                try (final ResultFormat.Writer writer = sqlQuery.getResultFormat()
                                                                .createFormatter(os, jsonMapper)) {
                  writer.writeResponseStart();

                  if (sqlQuery.includeHeader()) {
                    writer.writeHeader(Arrays.asList(columnNames));
                  }

                  while (!yielder.isDone()) {
                    final Object[] row = yielder.get();
                    writer.writeRowStart();
                    for (int i = 0; i < fieldList.size(); i++) {
                      final Object value;

                      if (timeColumns[i]) {
                        value = ISODateTimeFormat.dateTime().print(
                            Calcites.calciteTimestampToJoda((long) row[i], timeZone)
                        );
                      } else if (dateColumns[i]) {
                        value = ISODateTimeFormat.dateTime().print(
                            Calcites.calciteDateToJoda((int) row[i], timeZone)
                        );
                      } else {
                        value = row[i];
                      }

                      writer.writeRowField(fieldList.get(i).getName(), value);
                    }
                    writer.writeRowEnd();
                    yielder = yielder.next(null);
                    counter.increment();
                  }

                  writer.writeResponseEnd();
                }
                catch (Exception ex) {
                  e = ex;
                  log.error(ex, "Unable to send sql response [%s]", sqlQueryId);
                  throw Throwables.propagate(ex);
                }
                finally {
                  yielder.close();
                }
                lifecycle.emitLogsAndMetrics(query, e, remoteAddr, os.getCount(), counter.intValue());
                currThread.setName(currThreadName);
              }
            }
        ).build();
      }
      catch (Throwable e) {
        // make sure to close yielder if anything happened before starting to serialize the response.
        yielder0.close();
        throw Throwables.propagate(e);
      }
    }
    catch (ForbiddenException e) {
      throw e; // let ForbiddenExceptionMapper handle this
    }
    catch (Exception e) {
      log.warn(e, "Failed to handle query: %s %s", query, context);
      lifecycle.emitLogsAndMetrics(query, e, remoteAddr, -1, -1);
      currThread.setName(currThreadName);
      final Exception exceptionToReport;

      if (e instanceof RelOptPlanner.CannotPlanException) {
        exceptionToReport = new ISE("Cannot build plan for query: %s", query);
      } else {
        exceptionToReport = e;
      }

      return Response.serverError()
                     .type(MediaType.APPLICATION_JSON_TYPE)
                     .entity(jsonMapper.writeValueAsBytes(QueryInterruptedException.wrapIfNeeded(exceptionToReport)))
                     .build();
    }
  }

  // clear previous query name if exists (should not)
  private String resetThreadName(Thread thread)
  {
    String currThreadName = thread.getName();
    int index = currThreadName.indexOf('[');
    if (index > 0 && currThreadName.endsWith("]")) {
      thread.setName(currThreadName = currThreadName.substring(0, index));
    }
    return currThreadName;
  }
}
