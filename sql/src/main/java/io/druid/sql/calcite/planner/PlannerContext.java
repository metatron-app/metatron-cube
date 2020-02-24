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

package io.druid.sql.calcite.planner;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.druid.query.BaseAggregationQuery;
import io.druid.server.QueryManager;
import io.druid.server.security.AuthenticationResult;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Like {@link PlannerConfig}, but that has static configuration and this class contains dynamic, per-query
 * configuration.
 */
public class PlannerContext
{
  // query context keys
  public static final String CTX_SQL_QUERY_ID = "sqlQueryId";
  public static final String CTX_SQL_CURRENT_TIMESTAMP = "sqlCurrentTimestamp";
  public static final String CTX_SQL_TIME_ZONE = "sqlTimeZone";

  private final QueryManager queryManager;
  private final DruidOperatorTable operatorTable;
  private final PlannerConfig plannerConfig;
  private final DateTime localNow;
  private final long queryStartTimeMillis;
  private final Map<String, Object> queryContext;
  private final AuthenticationResult authenticationResult;
  private final String sqlQueryId;
  private final List<String> nativeQueryIds = new CopyOnWriteArrayList<>();

  private PlannerContext(
      final QueryManager queryManager,
      final DruidOperatorTable operatorTable,
      final PlannerConfig plannerConfig,
      final DateTime localNow,
      final Map<String, Object> queryContext,
      final AuthenticationResult authenticationResult
  )
  {
    this.queryManager = queryManager;
    this.operatorTable = operatorTable;
    this.plannerConfig = Preconditions.checkNotNull(plannerConfig, "plannerConfig");
    this.queryContext = queryContext != null ? Maps.newHashMap(queryContext) : Maps.newHashMap();
    this.localNow = Preconditions.checkNotNull(localNow, "localNow");
    this.queryStartTimeMillis = System.currentTimeMillis();
    this.authenticationResult = authenticationResult;
    this.queryContext.put(BaseAggregationQuery.SORT_ON_TIME, false);

    String sqlQueryId = (String) this.queryContext.get(CTX_SQL_QUERY_ID);
    // special handling for DruidViewMacro, normal client will allocate sqlid in SqlLifecyle
    if (Strings.isNullOrEmpty(sqlQueryId)) {
      sqlQueryId = UUID.randomUUID().toString();
    }
    this.sqlQueryId = sqlQueryId;
  }

  public static PlannerContext create(
      final QueryManager queryManager,
      final DruidOperatorTable operatorTable,
      final PlannerConfig plannerConfig,
      final Map<String, Object> queryContext,
      final AuthenticationResult authenticationResult
  )
  {
    final DateTimeZone timeZone;
    final DateTime utcNow;

    if (queryContext != null) {
      final Object tzParam = queryContext.get(CTX_SQL_TIME_ZONE);
      if (tzParam != null) {
        timeZone = DateTimeZone.forID(String.valueOf(tzParam));
      } else {
        timeZone = DateTimeZone.UTC;
      }

      final Object tsParam = queryContext.get(CTX_SQL_CURRENT_TIMESTAMP);
      if (tsParam != null) {
        utcNow = new DateTime(tsParam, timeZone);
      } else {
        utcNow = new DateTime(timeZone);
      }
    } else {
      timeZone = DateTimeZone.UTC;
      utcNow = new DateTime(timeZone);
    }

    return new PlannerContext(
        queryManager,
        operatorTable,
        plannerConfig.withOverrides(queryContext),
        utcNow.withZone(timeZone),
        queryContext,
        authenticationResult
    );
  }

  public QueryManager getQueryManager()
  {
    return queryManager;
  }

  public DruidOperatorTable getOperatorTable()
  {
    return operatorTable;
  }

  public PlannerConfig getPlannerConfig()
  {
    return plannerConfig;
  }

  public DateTime getLocalNow()
  {
    return localNow;
  }

  public DateTimeZone getTimeZone()
  {
    return localNow.getZone();
  }

  public Map<String, Object> getQueryContext()
  {
    return queryContext;
  }

  public AuthenticationResult getAuthenticationResult()
  {
    return authenticationResult;
  }

  public String getSqlQueryId()
  {
    return sqlQueryId;
  }

  public List<String> getNativeQueryIds()
  {
    return nativeQueryIds;
  }

  public void addNativeQueryId(String queryId)
  {
    this.nativeQueryIds.add(queryId);
  }

  public long getQueryStartTimeMillis()
  {
    return queryStartTimeMillis;
  }

  public DataContext createDataContext(final JavaTypeFactory typeFactory)
  {
    class DruidDataContext implements DataContext
    {
      private final Map<String, Object> context = ImmutableMap.<String, Object>of(
          DataContext.Variable.UTC_TIMESTAMP.camelName, localNow.getMillis(),
          DataContext.Variable.CURRENT_TIMESTAMP.camelName, localNow.getMillis(),
          DataContext.Variable.LOCAL_TIMESTAMP.camelName, new Interval(
              new DateTime("1970-01-01T00:00:00.000", localNow.getZone()),
              localNow
          ).toDurationMillis(),
          DataContext.Variable.TIME_ZONE.camelName, localNow.getZone().toTimeZone().clone()
      );

      @Override
      public SchemaPlus getRootSchema()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public JavaTypeFactory getTypeFactory()
      {
        return typeFactory;
      }

      @Override
      public QueryProvider getQueryProvider()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public Object get(final String name)
      {
        return context.get(name);
      }
    }

    return new DruidDataContext();
  }
}
