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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import com.metamx.common.logger.Logger;
import io.druid.concurrent.Execs;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.*;
import io.druid.guice.GuiceInjectors;
import io.druid.initialization.Initialization;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.metadata.TestDerbyConnector;
import org.apache.commons.lang.StringUtils;
import org.junit.*;
import org.skife.jdbi.v2.Handle;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class JDBCFirehoseFactoryTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();
  private static final Logger log = new Logger(JDBCFirehoseFactoryTest.class);
  private ObjectMapper mapper;
  private Injector injector;
  private static final String tableName = "JDBCFSTest";
  private static final String tsName = "TSCOLUMN";
  private static final List<String> columns = Lists.newArrayList("COL1", "COL2", "COL3");
  private static final List<Object[]> data = ImmutableList.of(
      new Object[] {"foo", "bar", "1.1"},
      new Object[] {"bad", "bar", "1.9"},
      new Object[] {"fuu", "baz", null},
      new Object[] {"poo", "bar", "5.9"}
      );
  private static final String joinTable = "JDBCFSJoin";
  private static final List<String> joinColumns = Lists.newArrayList("J1");
  private static final List<Object[]> joinData = ImmutableList.of(
      new Object[] {"bad"},
      new Object[] {"foo"}
  );

  private final Closer closer = Closer.create();
  private final ListeningExecutorService setupTeardownService =
      MoreExecutors.listeningDecorator(Execs.multiThreaded(2, "JDBCFirehoseFactoryTeardown--%s"));
  private Handle handleRef = null;

  @Before
  public void setUpModule() throws Exception
  {
    System.setProperty("druid.extensions.searchCurrentClassloader", "false");

    injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjectorWithModules(
            ImmutableList.<Module>of()
        ),
        ImmutableList.of(
            new Module() {
              @Override
              public void configure(Binder binder) {
                binder.bindConstant().annotatedWith(Names.named("serviceName")).to("test");
                binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
              }
            },
            new JDBCFirehoseDruidModule()
        )
    );
    mapper = injector.getInstance(ObjectMapper.class);
  }

  @Before
  public void setupDB() throws Exception
  {
    final ListenableFuture<Handle> setupFuture = setupTeardownService.submit(
        new Callable<Handle>()
        {
          @Override
          public Handle call()
          {
            final Handle handle = derbyConnectorRule.getConnector().getDBI().open();
            Assert.assertEquals(
                0,
                handle.createStatement(makeCreateQuery())
                    .setQueryTimeout(1).execute()
            );
            handle.createStatement(String.format("TRUNCATE TABLE %s", tableName)).setQueryTimeout(1).execute();
            for (Object[] entry: data) {
              try {
                insertValues(handle, entry, "2015-01-01 00:00:00");
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
              }
            }

            Assert.assertEquals(
                0,
                handle.createStatement(makeCreateJoinTableQuery())
                    .setQueryTimeout(1).execute()
            );
            handle.createStatement(String.format("TRUNCATE TABLE %s", joinTable)).setQueryTimeout(1).execute();
            for (Object[] entry: joinData) {
              try {
                insertJoinValues(handle, entry);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
              }
            }

            handle.commit();
            closer.register(new Closeable()
            {
              @Override
              public void close() throws IOException
              {
                handle.createStatement("DROP TABLE " + tableName).setQueryTimeout(1).execute();
                final ListenableFuture future = setupTeardownService.submit(new Runnable()
                {
                  @Override
                  public void run()
                  {
                    handle.close();
                  }
                });
                try (Closeable closeable = new Closeable()
                {
                  @Override
                  public void close() throws IOException
                  {
                    future.cancel(true);
                  }
                }) {
                  future.get(10, TimeUnit.SECONDS);
                }
                catch (InterruptedException | ExecutionException | TimeoutException e) {
                  throw new IOException("Error closing handle", e);
                }
              }
            });

            return handle;
          }
        }
    );

    try (final Closeable closeable =
             new Closeable()
             {
               @Override
               public void close() throws IOException
               {
                 if (!setupFuture.isDone() && !setupFuture.cancel(true) && !setupFuture.isDone()) {
                   throw new IOException("Unable to stop future");
                 }
               }
             }) {
      handleRef = setupFuture.get(10, TimeUnit.SECONDS);
    }
    Assert.assertNotNull(handleRef);
  }

  @After
  public void tearDownDB() throws InterruptedException, ExecutionException, TimeoutException, IOException
  {
    final ListenableFuture<?> tearDownFuture = setupTeardownService.submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            try {
              closer.close();
            }
            catch (IOException e) {
              throw Throwables.propagate(e);
            }
          }
        }
    );
    try (final Closeable closeable = new Closeable()
    {
      @Override
      public void close() throws IOException
      {
        setupTeardownService.shutdownNow();
        try {
          if (!setupTeardownService.awaitTermination(60, TimeUnit.SECONDS)) {
            log.error("Tear down service didn't finish");
          }
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted", e);
        }
      }
    }) {
      tearDownFuture.get(60, TimeUnit.SECONDS);
    }
    finally {
      if (Thread.interrupted()) {
        log.info("Thread was interrupted. Clearing interrupt and continuing.");
      }
    }
  }

  @Test
  public void testSimple() throws IOException
  {
    List<String> dbColumns = Lists.newArrayListWithCapacity(columns.size() + 1);
    dbColumns.add(tsName);
    dbColumns.addAll(columns);
    JDBCFirehoseFactory factory = new JDBCFirehoseFactory(
        toJDBCConnectorConfig(derbyConnectorRule.getMetadataConnectorConfig()),
        tableName,
        null,
        null,
        null,
        null,
        dbColumns
    );

    MapInputRowParser parser = new MapInputRowParser(
        new JSONParseSpec(
            new DefaultTimestampSpec(
                tsName,
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
    Firehose firehose = factory.connect(parser);

    List<InputRow> rows = Lists.newLinkedList();

    while(firehose.hasMore())
    {
      rows.add(firehose.nextRow());
    }

    Assert.assertEquals(4, rows.size());
  }

  @Test
  public void testComplex() throws IOException
  {
    List<String> dbColumns = Lists.newArrayListWithCapacity(columns.size() + 1);
    dbColumns.add(tsName);
    dbColumns.addAll(columns);
    String complexJoin = String.format("(select %s from %s A join %s B on A.COL1 = B.J1) x",
        StringUtils.join(dbColumns, ','), tableName, joinTable);
    JDBCFirehoseFactory factory = new JDBCFirehoseFactory(
        toJDBCConnectorConfig(derbyConnectorRule.getMetadataConnectorConfig()),
        complexJoin,
        null,
        null,
        null,
        null,
        null
    );

    MapInputRowParser parser = new MapInputRowParser(
        new JSONParseSpec(
            new DefaultTimestampSpec(
                tsName,
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
    Firehose firehose = factory.connect(parser);

    List<InputRow> rows = Lists.newLinkedList();

    while(firehose.hasMore())
    {
      rows.add(firehose.nextRow());
    }

    Assert.assertEquals(2, rows.size());
  }

  @Test
  public void testSimpleQuery() throws IOException
  {
    List<String> dbColumns = Lists.newArrayListWithCapacity(columns.size() + 1);
    String newTs = "ts";
    dbColumns.add(newTs);
    List<String> newCols = Lists.transform(columns, new Function<String, String>() {
      @Nullable
      @Override
      public String apply(@Nullable String input) {
        return input + "Mod";
      }
    });
    dbColumns.addAll(newCols);
    String query = "select * from " + tableName;
    JDBCFirehoseFactory factory = new JDBCFirehoseFactory(
        toJDBCConnectorConfig(derbyConnectorRule.getMetadataConnectorConfig()),
        null,
        query,
        null,
        null,
        null,
        dbColumns
    );

    MapInputRowParser parser = new MapInputRowParser(
        new JSONParseSpec(
            new DefaultTimestampSpec(
                newTs,
                "YYYY-MM-DD HH:mm:ss.s",
                null
            ),
            new DimensionsSpec(
                DimensionsSpec.getDefaultSchemas(newCols),
                null,
                null
            ),
            null,
            null
        )
    );
    Firehose firehose = factory.connect(parser);

    List<InputRow> rows = Lists.newLinkedList();

    while(firehose.hasMore())
    {
      rows.add(firehose.nextRow());
    }

    Assert.assertEquals(4, rows.size());
  }

  @Test
  public void testComplexQuery() throws IOException
  {
    List<String> dbColumns = Lists.newArrayListWithCapacity(columns.size() + 1);
    String newTs = "ts";
    dbColumns.add(newTs);
    List<String> newCols = Lists.transform(columns, new Function<String, String>() {
      @Nullable
      @Override
      public String apply(@Nullable String input) {
        return input + "Mod";
      }
    });
    dbColumns.addAll(newCols);
    String query = String.format("select * from %s A join %s B on A.COL1 = B.J1", tableName, joinTable);
    JDBCFirehoseFactory factory = new JDBCFirehoseFactory(
        toJDBCConnectorConfig(derbyConnectorRule.getMetadataConnectorConfig()),
        null,
        query,
        null,
        null,
        null,
        dbColumns
    );

    MapInputRowParser parser = new MapInputRowParser(
        new JSONParseSpec(
            new DefaultTimestampSpec(
                newTs,
                "YYYY-MM-DD HH:mm:ss.s",
                null
            ),
            new DimensionsSpec(
                DimensionsSpec.getDefaultSchemas(newCols),
                null,
                null
            ),
            null,
            null
        )
    );
    Firehose firehose = factory.connect(parser);

    List<InputRow> rows = Lists.newLinkedList();

    while(firehose.hasMore())
    {
      rows.add(firehose.nextRow());
    }

    Assert.assertEquals(2, rows.size());
  }

  @Test
  public void testSerde() throws Exception{
    List<String> dbColumns = Lists.newArrayListWithCapacity(columns.size() + 1);
    dbColumns.add(tsName);
    dbColumns.addAll(columns);
    JDBCFirehoseFactory factory = new JDBCFirehoseFactory(
        toJDBCConnectorConfig(derbyConnectorRule.getMetadataConnectorConfig()),
        tableName,
        null,
        null,
        null,
        null,
        null
    );

    byte[] bytes = mapper.writeValueAsBytes(factory);
    JDBCFirehoseFactory factory2 = mapper.readValue(bytes, JDBCFirehoseFactory.class);
    byte[] bytes2 = mapper.writeValueAsBytes(factory2);

    Assert.assertArrayEquals(bytes, bytes2);
  }

  @Test
  public void testExplicitJson() throws IOException
  {
    String json = String.format("{\"type\":\"jdbc\", \"connectorConfig\":{\"connectURI\":\"jdbc:mysql://localhost:3306/druid\",\"user\":\"druid\",\"password\":\"diurd\"}, \"table\": \"%s\", \"columns\": [\"key\",\"value\"]}",
        tableName);
    JDBCFirehoseFactory factory = (JDBCFirehoseFactory)mapper.readValue(
        json,
        FirehoseFactory.class
    );

    Assert.assertEquals(tableName, factory.getTable());
    List<String> expected = ImmutableList.of("key", "value");
    Assert.assertEquals(expected, factory.getColumns());
  }

  @Test
  public void testExplicitJson2() throws IOException
  {
    String query = "select * from test";
    String json = String.format("{\"type\":\"jdbc\", \"connectorConfig\":{\"connectURI\":\"jdbc:mysql://localhost:3306/druid\",\"user\":\"druid\",\"password\":\"diurd\"}, \"query\": \"%s\", \"columns\": [\"key\",\"value\"]}",
        query);
    JDBCFirehoseFactory factory = (JDBCFirehoseFactory)mapper.readValue(
        json,
        FirehoseFactory.class
    );

    Assert.assertEquals(query, factory.getQuery());
    List<String> expected = ImmutableList.of("key", "value");
    Assert.assertEquals(expected, factory.getColumns());
  }

  private void insertValues(final Handle handle, final Object[] data, final String updateTs)
      throws InterruptedException
  {
    final String query = makeInsertQuery(updateTs, data);
    Assert.assertEquals(1, handle.createStatement(query).setQueryTimeout(1).execute());
    handle.commit();
    Thread.sleep(2);
  }

  private void insertJoinValues(final Handle handle, final Object[] data)
    throws InterruptedException
  {
    final String query = makeInsertJoinTableQuery(data);
    Assert.assertEquals(1, handle.createStatement(query).setQueryTimeout(1).execute());
    handle.commit();
    Thread.sleep(2);
  }

  private String makeCreateQuery()
  {
    return new StringBuilder("CREATE TABLE ")
        .append(tableName).append(" (")
        .append(tsName).append(" TIMESTAMP, ")
        .append(StringUtils.join(columns, " VARCHAR(64),")).append(" VARCHAR(64))").toString();
  }

  private String makeCreateJoinTableQuery()
  {
    return new StringBuilder("CREATE TABLE ")
        .append(joinTable).append(" (")
        .append(StringUtils.join(joinColumns, " VARCHAR(64),")).append(" VARCHAR(64))").toString();
  }

  private String makeInsertQuery(String ts, Object[] data)
  {
    StringBuilder builder = new StringBuilder("INSERT INTO ")
        .append(tableName).append(" (")
        .append(tsName).append(", ")
        .append(StringUtils.join(columns, ", ")).append(") VALUES (");
    // add timestamp value
    builder.append("'").append(ts).append("'");
    // add other columns
    for (Object value : data)
    {
      builder.append(", ");
      if (value instanceof Float) {
        builder.append(value);
      } else if (value == null) {
        builder.append(value);
      } else {
        builder.append("'").append(value).append("'");
      }
    }
    builder.append(")");

    return builder.toString();
  }

  private String makeInsertJoinTableQuery(Object[] data)
  {
    StringBuilder builder = new StringBuilder("INSERT INTO ")
        .append(joinTable).append(" (")
        .append(StringUtils.join(joinColumns, ", ")).append(") VALUES (");
    boolean first = true;
    // add other columns
    for (Object value : data)
    {
      if (first) {
        first = false;
      } else {
        builder.append(", ");
      }
      if (value instanceof Float) {
        builder.append(value);
      } else {
        builder.append("'").append(value).append("'");
      }
    }
    builder.append(")");

    return builder.toString();
  }

  private JDBCConnectorConfig toJDBCConnectorConfig(MetadataStorageConnectorConfig config) {
    return new JDBCConnectorConfig(
        config.getHost(),
        config.getPort(),
        config.getConnectURI(),
        config.getUser(),
        config.getPassword(),
        null);
  }

}
