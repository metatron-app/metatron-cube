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
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.CountingOutputStream;
import com.google.inject.Inject;
import io.druid.common.Yielders;
import io.druid.common.utils.Sequences;
import io.druid.data.ValueDesc;
import io.druid.data.output.OutputDecorator;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Self;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Yielder;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.QueryInterruptedException;
import io.druid.server.DruidNode;
import io.druid.server.QueryResource;
import io.druid.server.security.Access;
import io.druid.server.security.AuthorizationUtils;
import io.druid.server.security.ForbiddenException;
import io.druid.sql.SqlLifecycle;
import io.druid.sql.SqlLifecycleFactory;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.planner.PlannerResult;
import io.druid.sql.calcite.table.RowSignature;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang.mutable.MutableInt;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
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
import java.util.Enumeration;
import java.util.List;
import java.util.Map;

@Path("/druid/v2/sql/")
public class SqlResource
{
  private static final Logger log = new Logger(SqlResource.class);

  private final ObjectMapper jsonMapper;
  private final SqlLifecycleFactory lifecycleFactory;
  private final DruidNode node;

  @Inject
  public SqlResource(
      @Json ObjectMapper jsonMapper,
      SqlLifecycleFactory lifecycleFactory,
      @Self DruidNode node
  )
  {
    this.jsonMapper = Preconditions.checkNotNull(jsonMapper, "jsonMapper");
    this.lifecycleFactory = Preconditions.checkNotNull(lifecycleFactory, "sqlLifecycleFactory");
    this.node = node;
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.TEXT_PLAIN)
  @Path("/explain")
  public Response explain(
      String sqlQuery,
      @Context HttpServletRequest req
  ) throws SQLException, IOException
  {
    final String explain = String.format("EXPLAIN PLAN WITH IMPLEMENTATION FOR %s", sqlQuery);
    return execute(new SqlQuery(explain, null, false, contextFromParam(req)), req);
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.TEXT_PLAIN)
  @Path("/geojson")
  public Response getFeature(
      String sqlQuery,
      @Context HttpServletRequest req
  ) throws SQLException, IOException
  {
    req.setAttribute(QueryResource.GET_FEATURE, true);
    return execute(new SqlQuery(sqlQuery, null, false, contextFromParam(req)), req);
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.TEXT_PLAIN)
  public Response doPost(
      String sqlQuery,
      @Context HttpServletRequest req
  ) throws SQLException, IOException
  {
    return execute(new SqlQuery(sqlQuery, null, false, contextFromParam(req)), req);
  }

  @POST
  @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
  @Consumes(MediaType.APPLICATION_JSON)
  public Response doPost(
      SqlQuery sqlQuery,
      @Context HttpServletRequest req
  ) throws SQLException, IOException
  {
    return execute(sqlQuery, req);
  }

  private Response execute(
      final SqlQuery sqlQuery,
      final HttpServletRequest req
  ) throws SQLException, IOException
  {
    final String remoteAddr = req.getRemoteAddr();

    final SqlLifecycle lifecycle = lifecycleFactory.factorize(
        sqlQuery.getQuery(), sqlQuery.getContext(), AuthorizationUtils.authenticationResultFromRequest(req)
    );
    final DateTimeZone timeZone = lifecycle.getPlannerContext().getTimeZone();

    final Thread currThread = Thread.currentThread();
    final String currThreadName = resetThreadName(currThread);

    final String query = sqlQuery.getQuery();
    try {
      currThread.setName(String.format("%s[sql_%s]", currThreadName, lifecycle.getQueryId()));

      PlannerResult result = lifecycle.plan();
      Access access = lifecycle.authorize(req);
      if (!access.isAllowed()) {
        throw new ForbiddenException(access.toString());
      }
      // Remember which columns are time-typed, so we can emit ISO8601 instead of millis values.
      // Also store list of all column names, for X-Druid-Sql-Columns header.
      final List<RelDataTypeField> fieldList = result.rowType().getFieldList();
      final boolean[] timeColumns = new boolean[fieldList.size()];
      final boolean[] dateColumns = new boolean[fieldList.size()];
      final String[] columnNames = new String[fieldList.size()];

      boolean needTypeConversion = false;
      for (int i = 0; i < fieldList.size(); i++) {
        final SqlTypeName sqlTypeName = fieldList.get(i).getType().getSqlTypeName();
        timeColumns[i] = sqlTypeName == SqlTypeName.TIMESTAMP;
        dateColumns[i] = sqlTypeName == SqlTypeName.DATE;
        columnNames[i] = fieldList.get(i).getName();
        needTypeConversion |= timeColumns[i] || dateColumns[i];
      }

      Sequence<Object[]> sequence = lifecycle.execute();
      if (needTypeConversion) {
        sequence = Sequences.map(sequence, new Function<Object[], Object[]>()
        {
          private final DateTimeFormatter formatter = ISODateTimeFormat.dateTime();

          @Override
          public Object[] apply(Object[] input)
          {
            for (int i = 0; i < fieldList.size(); i++) {
              if (timeColumns[i]) {
                input[i] = formatter.print(Calcites.calciteTimestampToJoda((long) input[i], timeZone));
              } else if (dateColumns[i]) {
                input[i] = formatter.print(Calcites.calciteDateToJoda((int) input[i], timeZone));
              }
            }
            return input;
          }
        });
      }

      final Yielder<Object[]> yielder0 = Yielders.each(sequence);

      try {
        return Response.ok(
            new StreamingOutput()
            {
              @Override
              @SuppressWarnings("unchecked")
              public void write(final OutputStream outputStream) throws IOException, WebApplicationException
              {
                Exception e = null;
                Yielder<Object[]> yielder = yielder0;

                final MutableInt counter = new MutableInt();
                final CountingOutputStream os = new CountingOutputStream(outputStream);

                final ResultFormat.Writer formatter;
                if (req.getAttribute(QueryResource.GET_FEATURE) == null) {
                  formatter = sqlQuery.getResultFormat().createFormatter(os, jsonMapper);
                } else {
                  RowSignature signature = RowSignature.from(result.rowType());
                  int geomIndex = signature.getColumnTypes().indexOf(ValueDesc.GEOMETRY);
                  OutputDecorator<Object[]> decorator = jsonMapper.convertValue(
                      ImmutableMap.of(
                          "format", "geojson",
                          "geomIndex", geomIndex,
                          "columnNames", signature.getColumnNames()
                      ),
                      OutputDecorator.class
                  );
                  formatter = new DelegatedWriter(jsonMapper.getFactory().createGenerator(os), decorator);
                }

                try (final ResultFormat.Writer writer = formatter) {
                  writer.start();
                  if (sqlQuery.includeHeader()) {
                    writer.writeHeader(columnNames);
                  }
                  for (;!yielder.isDone(); yielder = yielder.next(null)) {
                    writer.writeRow(columnNames, yielder.get());
                    counter.increment();
                  }
                  writer.end();
                }
                catch (Exception ex) {
                  log.error(ex, "Unable to send sql response [%s]", lifecycle.getQueryId());
                  throw Throwables.propagate(ex);
                }
                finally {
                  yielder.close();
                }
                lifecycle.emitLogsAndMetrics(e, remoteAddr, os.getCount(), counter.intValue());
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
    catch (Throwable e) {
      log.warn(e, "Failed to handle query: %s %s", query, sqlQuery.getContext());
      lifecycle.emitLogsAndMetrics(e, remoteAddr, -1, -1);
      currThread.setName(currThreadName);

      final Throwable exceptionToReport;
      if (e instanceof RelOptPlanner.CannotPlanException) {
        // cause this can be huge
        exceptionToReport = new ISE("Cannot build plan by %s for query: %s", e, query);
      } else {
        exceptionToReport = e;
      }

      return Response.serverError()
                     .type(MediaType.APPLICATION_JSON_TYPE)
                     .entity(jsonMapper.writeValueAsBytes(
                         QueryInterruptedException.wrapIfNeeded(exceptionToReport, node))
                     )
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

  private Map<String, Object> contextFromParam(HttpServletRequest req)
  {
    Map<String, Object> context = Maps.newHashMap();
    Enumeration<String> names = req.getParameterNames();
    while (names.hasMoreElements()) {
      String name = names.nextElement();
      context.put(name, req.getParameter(name));
    }
    return context;
  }
}
