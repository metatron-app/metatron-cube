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

package io.druid.metadata;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.java.util.common.Pair;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.common.DateTimes;
import io.druid.common.utils.StringUtils;
import io.druid.indexer.TaskInfo;
import org.joda.time.DateTime;
import org.skife.jdbi.v2.FoldController;
import org.skife.jdbi.v2.Folder3;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.exceptions.CallbackFailedException;
import org.skife.jdbi.v2.exceptions.StatementException;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.skife.jdbi.v2.util.ByteArrayMapper;

import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public abstract class SQLMetadataStorageActionHandler<EntryType, StatusType, LogType, LockType>
    implements MetadataStorageActionHandler<EntryType, StatusType, LogType, LockType>
{
  private static final EmittingLogger log = new EmittingLogger(SQLMetadataStorageActionHandler.class);

  private final SQLMetadataConnector connector;
  private final ObjectMapper jsonMapper;
  private final TypeReference entryType;
  private final TypeReference statusType;
  private final TypeReference logType;
  private final TypeReference lockType;

  private final String entryTypeName;
  private final String entryTable;
  private final String logTable;
  private final String lockTable;

  public SQLMetadataStorageActionHandler(
      final SQLMetadataConnector connector,
      final ObjectMapper jsonMapper,
      final MetadataStorageActionHandlerTypes<EntryType, StatusType, LogType, LockType> types,
      final String entryTypeName,
      final String entryTable,
      final String logTable,
      final String lockTable
  )
  {
    this.connector = connector;
    this.jsonMapper = jsonMapper;
    this.entryType = types.getEntryType();
    this.statusType = types.getStatusType();
    this.logType = types.getLogType();
    this.lockType = types.getLockType();
    this.entryTypeName = entryTypeName;
    this.entryTable = entryTable;
    this.logTable = logTable;
    this.lockTable = lockTable;
  }

  protected SQLMetadataConnector getConnector()
  {
    return connector;
  }

  protected ObjectMapper getJsonMapper()
  {
    return jsonMapper;
  }

  protected TypeReference getStatusType()
  {
    return statusType;
  }

  protected String getEntryTable()
  {
    return entryTable;
  }

  protected String getLogTable()
  {
    return logTable;
  }

  protected String getEntryTypeName()
  {
    return entryTypeName;
  }

  public TypeReference getEntryType()
  {
    return entryType;
  }

  @Override
  public void insert(
      final String id,
      final DateTime timestamp,
      final String dataSource,
      final EntryType entry,
      final boolean active,
      final StatusType status
  ) throws EntryExistsException
  {
    try {
      connector.retryWithHandle(
          new HandleCallback<Void>()
          {
            @Override
            public Void withHandle(Handle handle) throws Exception
            {
              handle.createStatement(
                  String.format(
                      "INSERT INTO %s (id, created_date, datasource, payload, active, status_payload) VALUES (:id, :created_date, :datasource, :payload, :active, :status_payload)",
                      entryTable
                  )
              )
                    .bind("id", id)
                    .bind("created_date", timestamp.toString())
                    .bind("datasource", dataSource)
                    .bind("payload", jsonMapper.writeValueAsBytes(entry))
                    .bind("active", active)
                    .bind("status_payload", jsonMapper.writeValueAsBytes(status))
                    .execute();
              return null;
            }
          },
          new Predicate<Throwable>()
          {
            @Override
            public boolean apply(Throwable e)
            {
              final boolean isStatementException = e instanceof StatementException ||
                                                   (e instanceof CallbackFailedException
                                                    && e.getCause() instanceof StatementException);
              return connector.isTransientException(e) && !(isStatementException && getEntry(id).isPresent());
            }
          }
      );
    }
    catch (Exception e) {
      final boolean isStatementException = e instanceof StatementException ||
                                           (e instanceof CallbackFailedException
                                            && e.getCause() instanceof StatementException);
      if (isStatementException && getEntry(id).isPresent()) {
        throw new EntryExistsException(id, e);
      } else {
        throw Throwables.propagate(e);
      }
    }
  }

  @Override
  public boolean setStatus(final String entryId, final boolean active, final StatusType status)
  {
    return connector.retryWithHandle(
        new HandleCallback<Boolean>()
        {
          @Override
          public Boolean withHandle(Handle handle) throws Exception
          {
            return handle.createStatement(
                String.format(
                    "UPDATE %s SET active = :active, status_payload = :status_payload WHERE id = :id AND active = TRUE",
                    entryTable
                )
            )
                         .bind("id", entryId)
                         .bind("active", active)
                         .bind("status_payload", jsonMapper.writeValueAsBytes(status))
                         .execute() == 1;
          }
        }
    );
  }

  @Override
  public Optional<EntryType> getEntry(final String entryId)
  {
    return connector.retryWithHandle(
        new HandleCallback<Optional<EntryType>>()
        {
          @Override
          public Optional<EntryType> withHandle(Handle handle) throws Exception
          {
            byte[] res = handle.createQuery(
                String.format("SELECT payload FROM %s WHERE id = :id", entryTable)
            )
                               .bind("id", entryId)
                               .map(ByteArrayMapper.FIRST)
                               .first();

            return Optional.fromNullable(
                res == null ? null : jsonMapper.<EntryType>readValue(res, entryType)
            );
          }
        }
    );

  }

  @Override
  public Optional<StatusType> getStatus(final String entryId)
  {
    return connector.retryWithHandle(
        new HandleCallback<Optional<StatusType>>()
        {
          @Override
          public Optional<StatusType> withHandle(Handle handle) throws Exception
          {
            byte[] res = handle.createQuery(
                String.format("SELECT status_payload FROM %s WHERE id = :id", entryTable)
            )
                               .bind("id", entryId)
                               .map(ByteArrayMapper.FIRST)
                               .first();

            return Optional.fromNullable(
                res == null ? null : jsonMapper.<StatusType>readValue(res, statusType)
            );
          }
        }
    );
  }

  @Override
  public List<TaskInfo<EntryType, StatusType>> getCompletedTaskInfo(
      final DateTime timestamp,
      final @Nullable Integer maxNumStatuses,
      final @Nullable String dataSource
  )
  {
    return getConnector().retryWithHandle(
        new HandleCallback<List<TaskInfo<EntryType, StatusType>>>()
        {
          @Override
          public List<TaskInfo<EntryType, StatusType>> withHandle(Handle handle) throws Exception
          {
            final Query<Map<String, Object>> query = createCompletedTaskInfoQuery(
                handle,
                timestamp,
                maxNumStatuses,
                dataSource
            );
            return query.map(new TaskInfoMapper()).list();
          }
        }
    );
  }

  @Override
  public List<TaskInfo<EntryType, StatusType>> getActiveTaskInfo(final @Nullable String dataSource)
  {
    return getConnector().retryWithHandle(
        new HandleCallback<List<TaskInfo<EntryType, StatusType>>>()
        {
          @Override
          public List<TaskInfo<EntryType, StatusType>> withHandle(Handle handle) throws Exception
          {
            final Query<Map<String, Object>> query = createActiveTaskInfoQuery(
                handle,
                dataSource
            );
            return query.map(new TaskInfoMapper()).list();
          }
        }
    );
  }

  private Query<Map<String, Object>> createActiveTaskInfoQuery(Handle handle, @Nullable String dataSource)
  {
    String sql = StringUtils.format(
        "SELECT "
        + "  id, "
        + "  status_payload, "
        + "  payload, "
        + "  datasource, "
        + "  created_date "
        + "FROM "
        + "  %s "
        + "WHERE "
        + getWhereClauseForActiveStatusesQuery(dataSource)
        + "ORDER BY created_date",
        entryTable
    );

    Query<Map<String, Object>> query = handle.createQuery(sql);
    if (dataSource != null) {
      query = query.bind("ds", dataSource);
    }
    return query;
  }

  private String getWhereClauseForActiveStatusesQuery(String dataSource)
  {
    String sql = StringUtils.format("active = TRUE ");
    if (dataSource != null) {
      sql += " AND datasource = :ds ";
    }
    return sql;
  }

  class TaskInfoMapper implements ResultSetMapper<TaskInfo<EntryType, StatusType>>
  {
    @Override
    public TaskInfo<EntryType, StatusType> map(int index, ResultSet resultSet, StatementContext context) throws SQLException
    {
      final TaskInfo<EntryType, StatusType> taskInfo;
      EntryType task;
      StatusType status;
      try {
        task = getJsonMapper().readValue(resultSet.getBytes("payload"), getEntryType());
      }
      catch (IOException e) {
        log.error(e, "Encountered exception while deserializing task payload, setting task to null");
        task = null;
      }
      try {
        status = getJsonMapper().readValue(resultSet.getBytes("status_payload"), getStatusType());
      }
      catch (IOException e) {
        log.error(e, "Encountered exception while deserializing task status_payload");
        throw new SQLException(e);
      }
      taskInfo = new TaskInfo<>(
          resultSet.getString("id"),
          DateTimes.of(resultSet.getString("created_date")),
          status,
          resultSet.getString("datasource"),
          task
      );
      return taskInfo;
    }
  }

  protected abstract Query<Map<String, Object>> createCompletedTaskInfoQuery(
      Handle handle,
      DateTime timestamp,
      @Nullable Integer maxNumStatuses,
      @Nullable String datasource
  );

  @Override
  @Nullable
  public Pair<DateTime, String> getCreatedDateAndDataSource(String entryId)
  {
    return connector.retryWithHandle(
        handle -> handle
        .createQuery(
            StringUtils.format(
                "SELECT created_date, datasource FROM %s WHERE id = :entryId",
                entryTable
            )
        )
        .bind("entryId", entryId)
        .map(
            (index, resultSet, ctx) -> Pair.of(
                DateTimes.of(resultSet.getString("created_date")), resultSet.getString("datasource")
            )
        )
        .first()
    );
  }

  @Override
  public boolean addLock(final String entryId, final LockType lock)
  {
    return connector.retryWithHandle(
        new HandleCallback<Boolean>()
        {
          @Override
          public Boolean withHandle(Handle handle) throws Exception
          {
            return handle.createStatement(
                String.format(
                    "INSERT INTO %1$s (%2$s_id, lock_payload) VALUES (:entryId, :payload)",
                    lockTable, entryTypeName
                )
            )
                         .bind("entryId", entryId)
                         .bind("payload", jsonMapper.writeValueAsBytes(lock))
                         .execute() == 1;
          }
        }
    );
  }

  @Override
  public void removeLock(final long lockId)
  {
    connector.retryWithHandle(
        new HandleCallback<Void>()
        {
          @Override
          public Void withHandle(Handle handle) throws Exception
          {
            handle.createStatement(String.format("DELETE FROM %s WHERE id = :id", lockTable))
                  .bind("id", lockId)
                  .execute();

            return null;
          }
        }
    );
  }

  @Override
  public void removeTasksOlderThan(final long timestamp)
  {
    final DateTime dateTime = DateTimes.utc(timestamp);
    connector.retryWithHandle(
        new HandleCallback<Void>()
        {
          @Override
          public Void withHandle(Handle handle) throws Exception
          {
            handle.createStatement(SQLMetadataStorageActionHandler.this.getSqlRemoveLogsOlderThan())
                  .bind("date_time", dateTime.toString())
                  .execute();
            handle.createStatement(
                io.druid.common.utils.StringUtils.format(
                    "DELETE FROM %s WHERE created_date < :date_time AND active = false",
                    entryTable
                )
            )
                  .bind("date_time", dateTime.toString())
                  .execute();

            return null;
          }
        }
    );
  }

  @Override
  public boolean addLog(final String entryId, final LogType log)
  {
    return connector.retryWithHandle(
        new HandleCallback<Boolean>()
        {
          @Override
          public Boolean withHandle(Handle handle) throws Exception
          {
            return handle.createStatement(
                String.format(
                    "INSERT INTO %1$s (%2$s_id, log_payload) VALUES (:entryId, :payload)",
                    logTable, entryTypeName
                )
            )
                         .bind("entryId", entryId)
                         .bind("payload", jsonMapper.writeValueAsBytes(log))
                         .execute() == 1;
          }
        }
    );
  }

  @Override
  public List<LogType> getLogs(final String entryId)
  {
    return connector.retryWithHandle(
        new HandleCallback<List<LogType>>()
        {
          @Override
          public List<LogType> withHandle(Handle handle) throws Exception
          {
            return handle
                .createQuery(
                    String.format(
                        "SELECT log_payload FROM %1$s WHERE %2$s_id = :entryId",
                        logTable, entryTypeName
                    )
                )
                .bind("entryId", entryId)
                .map(ByteArrayMapper.FIRST)
                .fold(
                    Lists.<LogType>newLinkedList(),
                    new Folder3<List<LogType>, byte[]>()
                    {
                      @Override
                      public List<LogType> fold(
                          List<LogType> list, byte[] bytes, FoldController control, StatementContext ctx
                      ) throws SQLException
                      {
                        try {
                          list.add(
                              jsonMapper.<LogType>readValue(
                                  bytes, logType
                              )
                          );
                          return list;
                        }
                        catch (IOException e) {
                          log.makeAlert(e, "Failed to deserialize log")
                             .addData("entryId", entryId)
                             .addData("payload", StringUtils.fromUtf8(bytes))
                             .emit();
                          throw new SQLException(e);
                        }
                      }
                    }
                );
          }
        }
    );
  }

  @Deprecated
  public String getSqlRemoveLogsOlderThan()
  {
    return io.druid.common.utils.StringUtils.format("DELETE a FROM %s a INNER JOIN %s b ON a.%s_id = b.id "
                              + "WHERE b.created_date < :date_time and b.active = false",
                              logTable, entryTable, entryTypeName
    );
  }

  @Override
  public Map<Long, LockType> getLocks(final String entryId)
  {
    return connector.retryWithHandle(
        new HandleCallback<Map<Long, LockType>>()
        {
          @Override
          public Map<Long, LockType> withHandle(Handle handle) throws Exception
          {
            return handle.createQuery(
                String.format(
                    "SELECT id, lock_payload FROM %1$s WHERE %2$s_id = :entryId",
                    lockTable, entryTypeName
                )
            )
                         .bind("entryId", entryId)
                         .map(
                             new ResultSetMapper<Pair<Long, LockType>>()
                             {
                               @Override
                               public Pair<Long, LockType> map(int index, ResultSet r, StatementContext ctx)
                                   throws SQLException
                               {
                                 try {
                                   return Pair.of(
                                       r.getLong("id"),
                                       jsonMapper.<LockType>readValue(
                                           r.getBytes("lock_payload"),
                                           lockType
                                       )
                                   );
                                 }
                                 catch (IOException e) {
                                   log.makeAlert(e, "Failed to deserialize " + lockType.getType())
                                      .addData("id", r.getLong("id"))
                                      .addData(
                                          "lockPayload", StringUtils.fromUtf8(r.getBytes("lock_payload"))
                                      )
                                      .emit();
                                   throw new SQLException(e);
                                 }
                               }
                             }
                         )
                         .fold(
                             Maps.<Long, LockType>newLinkedHashMap(),
                             new Folder3<Map<Long, LockType>, Pair<Long, LockType>>()
                             {
                               @Override
                               public Map<Long, LockType> fold(
                                   Map<Long, LockType> accumulator,
                                   Pair<Long, LockType> lock,
                                   FoldController control,
                                   StatementContext ctx
                               ) throws SQLException
                               {
                                 accumulator.put(lock.lhs, lock.rhs);
                                 return accumulator;
                               }
                             }
                         );
          }
        }
    );
  }
}
