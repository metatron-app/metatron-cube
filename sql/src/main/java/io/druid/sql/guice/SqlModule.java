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

package io.druid.sql.guice;

import com.google.common.base.Preconditions;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.multibindings.Multibinder;
import io.druid.guice.Jerseys;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.server.initialization.jetty.JettyBindings;
import io.druid.server.metrics.MetricsModule;
import io.druid.sql.avatica.AvaticaMonitor;
import io.druid.sql.avatica.AvaticaServerConfig;
import io.druid.sql.avatica.DruidAvaticaHandler;
import io.druid.sql.calcite.aggregation.SqlAggregator;
import io.druid.sql.calcite.expression.builtin.LookupOperatorConversion;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.planner.DruidOperatorTable;
import io.druid.sql.calcite.planner.PlannerConfig;
import io.druid.sql.calcite.schema.DruidSchema;
import io.druid.sql.calcite.view.NoopViewManager;
import io.druid.sql.calcite.view.ViewManager;
import io.druid.sql.http.SqlResource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Properties;

public class SqlModule implements Module
{
  private static final String PROPERTY_SQL_ENABLE = "druid.sql.enable";
  private static final String PROPERTY_SQL_ENABLE_JSON_OVER_HTTP = "druid.sql.http.enable";
  private static final String PROPERTY_SQL_ENABLE_AVATICA = "druid.sql.avatica.enable";

  @Inject
  private Properties props;

  public SqlModule()
  {
  }

  @Override
  public void configure(Binder binder)
  {
    if (isEnabled()) {
      Calcites.setSystemProperties();

      JsonConfigProvider.bind(binder, "druid.sql.planner", PlannerConfig.class);
      JsonConfigProvider.bind(binder, "druid.sql.avatica", AvaticaServerConfig.class);
      LifecycleModule.register(binder, DruidSchema.class);
      binder.bind(ViewManager.class).to(NoopViewManager.class).in(LazySingleton.class);

      // Add empty SqlAggregator binder.
      Multibinder.newSetBinder(binder, SqlAggregator.class);

      // Add empty AggregatorFactory binder.
      Multibinder.newSetBinder(binder, AggregatorFactory.WithName.class);

      // LookupOperatorConversion isn't in DruidOperatorTable since it needs a LookupReferencesManager injected.
      SqlBindings.addOperatorConversion(binder, LookupOperatorConversion.class);

      binder.bind(DruidOperatorTable.class).in(LazySingleton.class);

      if (isJsonOverHttpEnabled()) {
        Jerseys.addResource(binder, SqlResource.class);
      }

      if (isAvaticaEnabled()) {
        binder.bind(AvaticaMonitor.class).in(LazySingleton.class);
        JettyBindings.addHandler(binder, DruidAvaticaHandler.class);
        MetricsModule.register(binder, AvaticaMonitor.class);
      }
    }
  }

  private boolean isEnabled()
  {
    Preconditions.checkNotNull(props, "props");
    return Boolean.valueOf(props.getProperty(PROPERTY_SQL_ENABLE, "false"));
  }

  private boolean isJsonOverHttpEnabled()
  {
    Preconditions.checkNotNull(props, "props");
    return Boolean.valueOf(props.getProperty(PROPERTY_SQL_ENABLE_JSON_OVER_HTTP, "true"));
  }

  private boolean isAvaticaEnabled()
  {
    Preconditions.checkNotNull(props, "props");
    return Boolean.valueOf(props.getProperty(PROPERTY_SQL_ENABLE_AVATICA, "false"));
  }

  public static void main(String[] args) throws Exception
  {
    String url = "jdbc:avatica:remote:url=http://localhost:8082/druid/v2/sql/avatica/";

    // Set any connection context parameters you need here (see "Connection context" below).
    // Or leave empty for default behavior.
    Properties connectionProperties = new Properties();

    try (Connection connection = DriverManager.getConnection(url, connectionProperties)) {
      try (
          final Statement statement = connection.createStatement();
          final ResultSet resultSet = statement.executeQuery("select * from lineitem")
      ) {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int count = metaData.getColumnCount();
        StringBuilder b = new StringBuilder();
        int x = 0;
        while (resultSet.next() && x++ < 100) {
          b.setLength(0);
          for (int i = 1; i <= count; i++) {
            if (i > 1) {
              b.append(',');
            }
            b.append(resultSet.getObject(i));
          }
//          System.out.println(b.toString());
          // Do something
        }
        resultSet.close();
        statement.close();
      }
    }
  }
}
