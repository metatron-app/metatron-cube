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

package io.druid.sql.avatica;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.security.AuthenticationResult;
import io.druid.sql.SqlLifecycleFactory;
import org.apache.calcite.avatica.Meta.StatementHandle;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Connection tracking for {@link DruidMeta}. Thread-safe.
 */
public class DruidConnection
{
  private static final Logger log = new Logger(DruidConnection.class);
  private static final Set<String> SENSITIVE_CONTEXT_FIELDS = Sets.newHashSet(
      "user", "password"
  );

  private final String connectionId;
  private final int maxStatements;
  private final ImmutableMap<String, Object> context;
  private final AuthenticationResult authenticationResult;

  private final AtomicInteger statementCounter = new AtomicInteger();
  private final AtomicReference<Future<?>> timeoutFuture = new AtomicReference<>();

  @GuardedBy("statements")
  private final Map<Integer, AtomicReference<DruidStatement>> statements = new HashMap<>();

  @GuardedBy("statements")
  private boolean open = true;

  public DruidConnection(
      String connectionId,
      int maxStatements,
      Map<String, Object> context,
      AuthenticationResult authenticationResult
  )
  {
    this.connectionId = Preconditions.checkNotNull(connectionId);
    this.maxStatements = maxStatements;
    this.context = ImmutableMap.copyOf(context);
    this.authenticationResult = authenticationResult;
  }

  public DruidStatement createStatement(
      StatementHandle statementHandle,
      String sql,
      Map<String, Object> context,
      SqlLifecycleFactory lifecycleFactory,
      long maxRowCount
  )
  {
    synchronized (statements) {
      AtomicReference<DruidStatement> reference = Preconditions.checkNotNull(
          statements.get(statementHandle.id), "closed?"
      );
      if (reference.get() != null) {
        reference.get().close();
      }
      Preconditions.checkArgument(reference != null && reference.get() == null, "invalid state");
      final DruidStatement statement = _createStatement(
          sql,
          context,
          lifecycleFactory,
          statementHandle,
          maxRowCount
      );
      reference.set(statement);

      statements.put(statementHandle.id, new AtomicReference<>(statement));
      log.debug("Connection[%s] opened statement[%s].", connectionId, statementHandle.id);
      return statement;
    }
  }

  public DruidStatement createStatement(
      String sql,
      Map<String, Object> context,
      SqlLifecycleFactory lifecycleFactory,
      long maxRowCount
  )
  {
    final StatementHandle statementHandle = createStatementHandle();
    synchronized (statements) {
      final DruidStatement statement = _createStatement(
          sql,
          context,
          lifecycleFactory,
          statementHandle,
          maxRowCount
      );

      statements.put(statementHandle.id, new AtomicReference<>(statement));
      log.debug("Connection[%s] opened statement[%s].", connectionId, statementHandle.id);
      return statement;
    }
  }

  private DruidStatement _createStatement(
      String sql,
      Map<String, Object> context,
      SqlLifecycleFactory lifecycleFactory,
      StatementHandle statementHandle,
      long maxRowCount
  )
  {
    if (statements.size() >= maxStatements) {
      throw new ISE("Too many open statements, limit is[%,d]", maxStatements);
    }

    // remove sensitive fields from the context, only the connection's context needs to have authentication
    // credentials
    Map<String, Object> sanitized = Maps.filterEntries(
        context,
        new Predicate<Map.Entry<String, Object>>()
        {
          @Override
          public boolean apply(@Nullable Map.Entry<String, Object> input)
          {
            return !SENSITIVE_CONTEXT_FIELDS.contains(input.getKey());
          }
        }
    );

    return new DruidStatement(
        this,
        statementHandle,
        lifecycleFactory.factorize(sql, sanitized, authenticationResult),
        () -> {
          // onClose function for the statement
          synchronized (statements) {
            log.debug("Connection[%s] closed statement[%s].", connectionId, statementHandle.id);
            statements.remove(statementHandle.id);
          }
        },
        maxRowCount
    );
  }

  public StatementHandle createStatementHandle()
  {
    StatementHandle handle = new StatementHandle(connectionId, statementCounter.incrementAndGet(), null);
    synchronized (statements) {
      statements.put(handle.id, new AtomicReference<DruidStatement>());
    }
    return handle;
  }

  public DruidStatement getStatement(final StatementHandle handle)
  {
    synchronized (statements) {
      AtomicReference<DruidStatement> statement = statements.get(handle.id);
      return statement == null ? null : statement.get();
    }
  }

  /**
   * Closes this connection if it has no statements.
   *
   * @return true if closed
   */
  public boolean closeIfEmpty()
  {
    synchronized (statements) {
      if (statements.isEmpty()) {
        close();
        return true;
      } else {
        return false;
      }
    }
  }

  public void close()
  {
    synchronized (statements) {
      // Copy statements before iterating because statement.close() modifies it.
      for (AtomicReference<DruidStatement> reference : ImmutableList.copyOf(statements.values())) {
        DruidStatement statement = reference.get();
        if (statement != null) {
          try {
            statement.close();
          }
          catch (Exception e) {
            log.warn("Connection[%s] failed to close statement[%s]!", connectionId, statement.getStatementHandle());
          }
        }
      }

      log.debug("Connection[%s] closed.", connectionId);
      open = false;
    }
  }

  public boolean isOpen()
  {
    synchronized (statements) {
      return open;
    }
  }

  public DruidConnection sync(final Future<?> newTimeoutFuture)
  {
    final Future<?> oldFuture = timeoutFuture.getAndSet(newTimeoutFuture);
    if (oldFuture != null) {
      oldFuture.cancel(false);
    }
    return this;
  }

  public Map<String, Object> context()
  {
    return context;
  }

  public String id()
  {
    return connectionId;
  }
}
