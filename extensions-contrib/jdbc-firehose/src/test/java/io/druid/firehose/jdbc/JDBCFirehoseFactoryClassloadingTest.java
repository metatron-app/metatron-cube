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
package io.druid.firehose.jdbc;

import com.google.common.collect.Lists;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.JSONParseSpec;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.DefaultTimestampSpec;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;
import org.skife.jdbi.v2.exceptions.UnableToObtainConnectionException;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@RunWith(JDBCFirehoseFactoryClassloadingTest.SeparateClassloaderTestRunner.class)
public class JDBCFirehoseFactoryClassloadingTest
{
  private static final String tableName = "JDBCFSTest";
  private static final String tsName = "COL1";
  private static final List<String> columns = Lists.newArrayList("COL1", "COL2", "COL3");
  private static List<String> dbColumns = Lists.newArrayListWithCapacity(columns.size() + 1);

  static {
    dbColumns = Lists.newArrayListWithCapacity(columns.size() + 1);
    dbColumns.add(tsName);
    dbColumns.addAll(columns);
  }

  private static final MapInputRowParser parser = new MapInputRowParser(
      new JSONParseSpec(
          new DefaultTimestampSpec(
              "COL1",
              "YYYY-MM-DD HH:mm:ss.s",
              null
          ),
          new DimensionsSpec(
              DimensionsSpec.getDefaultSchemas(columns),
              null,
              null
          ),
          null,
          null
      )
  );

  public static class SeparateClassloaderTestRunner extends BlockJUnit4ClassRunner
  {

    public SeparateClassloaderTestRunner(Class<?> clazz) throws InitializationError
    {
      super(getFromTestClassloader(clazz));
    }

    private static Class<?> getFromTestClassloader(Class<?> clazz) throws InitializationError {
      try {
        ClassLoader testClassLoader = new TestClassLoader();
        return Class.forName(clazz.getName(), true, testClassLoader);
      } catch (ClassNotFoundException e) {
        throw new InitializationError(e);
      }
    }

    private static URL[] getClasspathURLs()
    {
      URL[] systemUrls = ((URLClassLoader)JDBCFirehoseFactoryClassloadingTest.class.getClassLoader()).getURLs();
      List<URL> urlList = new ArrayList<URL>(Arrays.asList(systemUrls));
      try {
        urlList.add(JDBCFirehoseFactoryClassloadingTest.class.getClassLoader().getResource("postgresql-9.4.1208.jre7.jar").toURI().toURL());
      }
      catch (MalformedURLException e) {
        Assert.assertTrue(false);
      }
      catch (URISyntaxException e) {
        Assert.assertTrue(false);
      }

      return urlList.toArray(new URL[]{});
    }

    public static class TestClassLoader extends URLClassLoader {
      public TestClassLoader() {
        super(getClasspathURLs());
      }

      @Override
      public Class<?> loadClass(String name) throws ClassNotFoundException {
        if (name.startsWith("io.druid.firehose.jdbc")) {
          return super.findClass(name);
        }
        return super.loadClass(name);
      }
    }
  }

  @Test
  public void testWithNoDriverLoading() throws IOException
  {
    JDBCFirehoseFactory factory = new JDBCFirehoseFactory(new JDBCConnectorConfig(
        "localhost",
        1000,
        "jdbc:postgresql://localhost:1000/test",
        null,
        null,
        null
    ), tableName, null, null, null, null, dbColumns);

    boolean seenEx = false;
    try {
      factory.connect(parser);
    } catch (UnableToObtainConnectionException e) {
      if (e.getCause() instanceof SQLException) {
        SQLException sqlEx = (SQLException)e.getCause();
        if (sqlEx.getMessage().startsWith("Cannot create JDBC driver of class")) {
          seenEx = true;
        }
      }
    }
    Assert.assertTrue(seenEx);
  }

  @Test
  public void testWithDriverLoading()
  {
    JDBCFirehoseFactory factory = new JDBCFirehoseFactory(new JDBCConnectorConfig(
              "localhost",
              1000,
              "jdbc:postgresql://localhost:1000/test",
              null,
              null,
              "org.postgresql.Driver"
          ),
          tableName, null, null, null, null, dbColumns);

    boolean seenEx = false;
    try {
      factory.connect(parser);
    }
    catch (UnableToObtainConnectionException e) {
      e.printStackTrace();
      if (e.getCause() instanceof SQLException) {
        SQLException sqlEx = (SQLException)e.getCause();
        if (sqlEx.getMessage().startsWith("Cannot create PoolableConnectionFactory")) {
          seenEx = true;
        }
      }
    }
    catch (IOException e) {
    }
    Assert.assertTrue(seenEx);
  }
}


