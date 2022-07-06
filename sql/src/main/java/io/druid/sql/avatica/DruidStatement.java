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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import io.druid.common.Yielders;
import io.druid.common.guava.Sequence;
import io.druid.common.guava.Yielder;
import io.druid.common.utils.Sequences;
import io.druid.common.utils.StringUtils;
import io.druid.concurrent.Execs;
import io.druid.java.util.common.ISE;
import io.druid.server.security.Access;
import io.druid.server.security.ForbiddenException;
import io.druid.sql.SqlLifecycle;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.planner.PlannerResult;
import org.apache.calcite.avatica.AvaticaParameter;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.ColumnMetaData.AvaticaType;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

import javax.annotation.concurrent.GuardedBy;
import java.io.Closeable;
import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Statement handle for {@link DruidMeta}. Thread-safe.
 */
public class DruidStatement implements Closeable
{
  public static final long START_OFFSET = 0;

  enum State
  {
    NEW,
    PREPARED,
    RUNNING,
    DONE
  }

  private final StatementHandle handle;
  private final SqlLifecycle lifecycle;
  private final Runnable onClose;
  private final long maxRowCount;

  private final Object lock = new Object();

  /**
   * Query metrics can only be used within a single thread. Because results can be paginated into multiple
   * JDBC frames (each frame being processed by a potentially different thread), the thread that closes the yielder
   * (resulting in a QueryMetrics emit() call) may not be the same thread that created the yielder (which initializes
   * DefaultQueryMetrics with the current thread as the owner). Create and close the yielder with this
   * single-thread executor to prevent this from happening.
   *
   * The thread owner check in DefaultQueryMetrics is more aggressive than needed for this specific JDBC case, since
   * the JDBC frames are processed sequentially. If the thread owner check is changed/loosened to permit this use case,
   * we would not need to use this executor.
   *
   * See discussion at:
   * https://github.com/druid-io/druid/pull/4288
   * https://github.com/druid-io/druid/pull/4415
   */
  private final ExecutorService yielderOpenCloseExecutor;

  private State state = State.NEW;
  private Meta.Signature signature;
  private Yielder<Object[]> yielder;
  private int offset = 0;
  private Throwable throwable;

  public DruidStatement(
      final DruidConnection connection,
      final StatementHandle handle,
      final SqlLifecycle lifecycle,
      final Runnable onClose,
      final long maxRowCount
  )
  {
    this.handle = handle;
    this.lifecycle = Preconditions.checkNotNull(lifecycle, "sqlLifecycle");
    this.onClose = Preconditions.checkNotNull(onClose, "onClose");
    this.yielderOpenCloseExecutor = Execs.singleThreaded(
        StringUtils.format("JDBCYielderOpenCloseExecutor-connection-%s-statement-%d", connection.id(), handle.id)
    );
    this.maxRowCount = maxRowCount;
  }

  @VisibleForTesting
  DruidStatement(final String connectionId, final StatementHandle handle, final SqlLifecycle lifecycle)
  {
    this(new DruidConnection(connectionId, 2, ImmutableMap.of(), null), handle, lifecycle, () -> { }, -1);
  }

  private static List<ColumnMetaData> createColumnMetaData(final RelDataType rowType)
  {
    final List<ColumnMetaData> columns = new ArrayList<>();
    final List<RelDataTypeField> fieldList = rowType.getFieldList();

    for (int i = 0; i < fieldList.size(); i++) {
      final RelDataTypeField field = fieldList.get(i);
      final RelDataType type = field.getType();
      final AvaticaType columnType = toAvaticaType(type);
      columns.add(
          new ColumnMetaData(
              i, // ordinal
              false, // auto increment
              true, // case sensitive
              false, // searchable
              false, // currency
              type.isNullable()
              ? DatabaseMetaData.columnNullable
              : DatabaseMetaData.columnNoNulls, // nullable
              true, // signed
              type.getPrecision(), // display size
              field.getName(), // label
              null, // column name
              null, // schema name
              type.getPrecision(), // precision
              type.getScale(), // scale
              null, // table name
              null, // catalog name
              columnType, // avatica type
              true, // read only
              false, // writable
              false, // definitely writable
              columnType.columnClassName() // column class name
          )
      );
    }

    return columns;
  }

  private static AvaticaType toAvaticaType(RelDataType type)
  {
    final SqlTypeName sqlTypeName = Calcites.getTypeName(type);
    if (sqlTypeName == SqlTypeName.ARRAY) {
      return ColumnMetaData.array(
          toAvaticaType(type.getComponentType()), sqlTypeName.getName(), ColumnMetaData.Rep.ARRAY
      );
    } else if (sqlTypeName == SqlTypeName.STRUCTURED) {
      return ColumnMetaData.struct(createColumnMetaData(type));
    } else {
      return ColumnMetaData.scalar(sqlTypeName.getJdbcOrdinal(), sqlTypeName.getName(), rep(sqlTypeName));
    }
  }

  private AvaticaParameter toAvaticaParameter(String name, RelDataType type)
  {
    // signed is always false because no way to extract from RelDataType, and the only usage of this AvaticaParameter
    // constructor I can find, in CalcitePrepareImpl, does it this way with hard coded false
    return new AvaticaParameter(
        false,
        type.getPrecision(),
        type.getScale(),
        type.getSqlTypeName().getJdbcOrdinal(),
        type.getSqlTypeName().getName(),
        Calcites.sqlTypeNameJdbcToJavaClass(type.getSqlTypeName()).getName(),
        name
    );
  }

  private static ColumnMetaData.Rep rep(final SqlTypeName sqlType)
  {
    if (SqlTypeName.CHAR_TYPES.contains(sqlType)) {
      return ColumnMetaData.Rep.STRING;
    } else if (sqlType == SqlTypeName.TIMESTAMP) {
      return ColumnMetaData.Rep.LONG;
    } else if (sqlType == SqlTypeName.DATE) {
      return ColumnMetaData.Rep.INTEGER;
    } else if (sqlType == SqlTypeName.INTEGER) {
      return ColumnMetaData.Rep.INTEGER;
    } else if (sqlType == SqlTypeName.BIGINT) {
      return ColumnMetaData.Rep.LONG;
    } else if (sqlType == SqlTypeName.FLOAT) {
      return ColumnMetaData.Rep.FLOAT;
    } else if (sqlType == SqlTypeName.DOUBLE) {
      return ColumnMetaData.Rep.DOUBLE;
    } else if (sqlType == SqlTypeName.BOOLEAN) {
      return ColumnMetaData.Rep.BOOLEAN;
    } else {
      return ColumnMetaData.Rep.OBJECT;
    }
  }

  public DruidStatement prepare()
  {
    synchronized (lock) {
      try {
        ensure(State.NEW);
        PlannerResult result = lifecycle.plan(maxRowCount);
        Access access = lifecycle.authorize();
        if (!access.isAllowed()) {
          throw new ForbiddenException(access.toString());
        }
        int i = 0;
        List<AvaticaParameter> parameters = new ArrayList<>();
        for (RelDataType type : result.parametersType()) {
          parameters.add(toAvaticaParameter("?" + i++, type));
        }
        signature = Meta.Signature.create(
            createColumnMetaData(result.rowType()),
            lifecycle.getSQL(),
            parameters,
            Meta.CursorFactory.ARRAY,
            Meta.StatementType.SELECT // We only support SELECT
        );
        this.state = State.PREPARED;
      }
      catch (Throwable t) {
        this.throwable = t;
        try {
          close();
        }
        catch (Throwable t1) {
          t.addSuppressed(t1);
        }
        throw Throwables.propagate(t);
      }

      return this;
    }
  }

  public DruidStatement execute(List<Object> parameters)
  {
    synchronized (lock) {
      ensure(State.PREPARED);

      try {
        Sequence<Object[]> sequence = yielderOpenCloseExecutor.submit(() -> lifecycle.execute(parameters)).get();

        // We can't apply limits greater than Integer.MAX_VALUE, ignore them.
        if (maxRowCount >= 0 && maxRowCount <= Integer.MAX_VALUE) {
          sequence = Sequences.limit(sequence, (int) maxRowCount);
        }

        yielder = Yielders.each(sequence);
        state = State.RUNNING;
      }
      catch (Throwable t) {
        this.throwable = t;
        try {
          close();
        }
        catch (Throwable t1) {
          t.addSuppressed(t1);
        }
        throw Throwables.propagate(t);
      }

      return this;
    }
  }

  public StatementHandle getStatementHandle()
  {
    return handle;
  }

  public Meta.Signature getSignature()
  {
    synchronized (lock) {
      ensure(State.PREPARED, State.RUNNING, State.DONE);
      return signature;
    }
  }

  public long getCurrentOffset()
  {
    synchronized (lock) {
      ensure(State.RUNNING, State.DONE);
      return offset;
    }
  }

  public boolean isDone()
  {
    synchronized (lock) {
      return state == State.DONE;
    }
  }

  public Meta.Frame nextFrame(final long fetchOffset, final int fetchMaxRowCount)
  {
    synchronized (lock) {
      ensure(State.RUNNING);
      Preconditions.checkState(fetchOffset == offset, "fetchOffset[%,d] != offset[%,d]", fetchOffset, offset);

      try {
        final List<Object> rows = new ArrayList<>();
        while (!yielder.isDone() && (fetchMaxRowCount < 0 || offset < fetchOffset + fetchMaxRowCount)) {
          rows.add(yielder.get());
          yielder = yielder.next(null);
          offset++;
        }

        final boolean done = yielder.isDone();
        if (done) {
          close();
        }

        return new Meta.Frame(fetchOffset, done, rows);
      }
      catch (Throwable t) {
        this.throwable = t;
        try {
          close();
        }
        catch (Throwable t1) {
          t.addSuppressed(t1);
        }
        throw t;
      }
    }
  }

  @Override
  public void close()
  {
    synchronized (lock) {
      if (state == State.DONE) {
        return;
      }
      final State oldState = state;
      state = State.DONE;

      try {
        if (yielder != null) {
          Yielder<Object[]> theYielder = this.yielder;
          this.yielder = null;

          // Put the close last, so any exceptions it throws are after we did the other cleanup above.
          yielderOpenCloseExecutor.submit(
              () -> {
                theYielder.close();
                // makes this a Callable instead of Runnable so we don't need to catch exceptions inside the lambda
                return null;
              }
          ).get();

          yielderOpenCloseExecutor.shutdownNow();
        }
        if (!(throwable instanceof ForbiddenException)) {
          lifecycle.emitLogsAndMetrics(throwable, null, -1, offset);
        }
      }
      catch (Throwable t) {
        try {
          lifecycle.emitLogsAndMetrics(t, null, -1, offset);
        }
        catch (Throwable t1) {
          t.addSuppressed(t1);
        }
        throw Throwables.propagate(t);
      }
      finally {
        onClose.run();
      }
    }
  }

  @GuardedBy("lock")
  private void ensure(final State... desiredStates)
  {
    for (State desiredState : desiredStates) {
      if (state == desiredState) {
        return;
      }
    }
    throw new ISE("Invalid action for state[%s]", state);
  }
}
