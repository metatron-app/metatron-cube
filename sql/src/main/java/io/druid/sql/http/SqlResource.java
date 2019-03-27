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

package io.druid.sql.http;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CountingOutputStream;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.guava.Yielder;
import com.metamx.common.logger.Logger;
import io.druid.client.BrokerServerView;
import io.druid.common.Yielders;
import io.druid.guice.annotations.Json;
import io.druid.query.QueryInterruptedException;
import io.druid.server.QueryStats;
import io.druid.server.RequestLogLine;
import io.druid.server.log.RequestLogger;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.planner.DruidPlanner;
import io.druid.sql.calcite.planner.PlannerFactory;
import io.druid.sql.calcite.planner.PlannerResult;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.joda.time.DateTime;
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
import java.util.List;
import java.util.Map;

@Path("/druid/v2/sql/")
public class SqlResource
{
  private static final Logger log = new Logger(SqlResource.class);

  private final ObjectMapper jsonMapper;
  private final PlannerFactory plannerFactory;
  private final BrokerServerView brokerServerView;
  private final RequestLogger requestLogger;

  @Inject
  public SqlResource(
      @Json ObjectMapper jsonMapper,
      PlannerFactory plannerFactory,
      BrokerServerView brokerServerView,
      RequestLogger requestLogger
  )
  {
    this.jsonMapper = Preconditions.checkNotNull(jsonMapper, "jsonMapper");
    this.plannerFactory = Preconditions.checkNotNull(plannerFactory, "connection");
    this.brokerServerView = brokerServerView;
    this.requestLogger = requestLogger;
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.TEXT_PLAIN)
  public Response doPost(
      String sqlQuery,
      @Context HttpServletRequest req
  ) throws SQLException, IOException
  {
    return execute(sqlQuery, ImmutableMap.of(), req);
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response doPost(
      SqlQuery sqlQuery,
      @Context HttpServletRequest req
  ) throws SQLException, IOException
  {
    return execute(sqlQuery.getQuery(), sqlQuery.getContext(), req);
  }

  private Response execute(
      final String query,
      final Map<String, Object> context,
      final HttpServletRequest req
  ) throws SQLException, IOException
  {
    final PlannerResult plannerResult;
    final DateTimeZone timeZone;

    final long start = System.currentTimeMillis();
    try (final DruidPlanner planner = plannerFactory.createPlanner(context)) {
      plannerResult = planner.plan(query, brokerServerView, req);
      timeZone = planner.getPlannerContext().getTimeZone();

      // Remember which columns are time-typed, so we can emit ISO8601 instead of millis values.
      final List<RelDataTypeField> fieldList = plannerResult.rowType().getFieldList();
      final boolean[] timeColumns = new boolean[fieldList.size()];
      final boolean[] dateColumns = new boolean[fieldList.size()];
      for (int i = 0; i < fieldList.size(); i++) {
        final SqlTypeName sqlTypeName = fieldList.get(i).getType().getSqlTypeName();
        timeColumns[i] = sqlTypeName == SqlTypeName.TIMESTAMP;
        dateColumns[i] = sqlTypeName == SqlTypeName.DATE;
      }

      final Yielder<Object[]> yielder0 = Yielders.each(plannerResult.run());

      try {
        return Response.ok(
            new StreamingOutput()
            {
              @Override
              public void write(final OutputStream outputStream) throws IOException, WebApplicationException
              {
                Yielder<Object[]> yielder = yielder0;

                CountingOutputStream os = new CountingOutputStream(outputStream);
                try (final JsonGenerator jsonGenerator = jsonMapper.getFactory().createGenerator(os)) {
                  jsonGenerator.writeStartArray();

                  while (!yielder.isDone()) {
                    final Object[] row = yielder.get();
                    jsonGenerator.writeStartObject();
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

                      jsonGenerator.writeObjectField(fieldList.get(i).getName(), value);
                    }
                    jsonGenerator.writeEndObject();
                    yielder = yielder.next(null);
                  }

                  jsonGenerator.writeEndArray();
                  jsonGenerator.flush();

                  // End with CRLF
                  os.write('\r');
                  os.write('\n');

                  os.flush(); // Some types of OutputStream suppress flush errors in the .close() method.
                }
                finally {
                  yielder.close();
                }
                final long queryTime = System.currentTimeMillis() - start;
                requestLogger.log(
                    new RequestLogLine(
                        new DateTime(start),
                        req.getRemoteAddr(),
                        query,
                        new QueryStats(
                            ImmutableMap.<String, Object>of(
                                "query/time", queryTime,
                                "query/bytes", os.getCount(),
                                "success", true
                            )
                        )
                    )
                );
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
    catch (Exception e) {
      log.warn(e, "Failed to handle query: %s %s", query, context);

      final Exception exceptionToReport;

      if (e instanceof RelOptPlanner.CannotPlanException) {
        exceptionToReport = new ISE("Cannot build plan for query: %s", query);
      } else {
        exceptionToReport = e;
      }

      final long queryTime = System.currentTimeMillis() - start;
      requestLogger.log(
          new RequestLogLine(
              new DateTime(start),
              req.getRemoteAddr(),
              query,
              new QueryStats(
                  ImmutableMap.<String, Object>of(
                      "query/time", queryTime,
                      "success", false,
                      "interrupted", true,
                      "reason", e.toString()
                  )
              )
          )
      );
      return Response.serverError()
                     .type(MediaType.APPLICATION_JSON_TYPE)
                     .entity(jsonMapper.writeValueAsBytes(QueryInterruptedException.wrapIfNeeded(exceptionToReport)))
                     .build();
    }
  }
}
