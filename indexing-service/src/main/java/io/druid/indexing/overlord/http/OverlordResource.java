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

package io.druid.indexing.overlord.http;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteSource;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import io.druid.audit.AuditInfo;
import io.druid.audit.AuditManager;
import io.druid.common.DateTimes;
import io.druid.common.Intervals;
import io.druid.common.config.JacksonConfigManager;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.JodaUtils;
import io.druid.common.utils.StringUtils;
import io.druid.guice.annotations.EscalatedGlobal;
import io.druid.guice.annotations.Self;
import io.druid.indexer.RunnerTaskState;
import io.druid.indexer.TaskInfo;
import io.druid.indexer.TaskLocation;
import io.druid.indexer.TaskState;
import io.druid.indexer.TaskStatus;
import io.druid.indexer.TaskStatusPlus;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.actions.TaskActionHolder;
import io.druid.indexing.common.task.IndexTask;
import io.druid.indexing.common.task.RealtimeIndexTask;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.IndexerMetadataStorageAdapter;
import io.druid.indexing.overlord.TaskLockbox;
import io.druid.indexing.overlord.TaskMaster;
import io.druid.indexing.overlord.TaskQueue;
import io.druid.indexing.overlord.TaskRunner;
import io.druid.indexing.overlord.TaskRunnerWorkItem;
import io.druid.indexing.overlord.TaskStorageQueryAdapter;
import io.druid.indexing.overlord.ThreadPoolTaskRunner;
import io.druid.indexing.overlord.WorkerTaskRunner;
import io.druid.indexing.overlord.autoscaling.ScalingStats;
import io.druid.indexing.overlord.http.security.TaskResourceFilter;
import io.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.http.client.HttpClient;
import io.druid.java.util.http.client.Request;
import io.druid.java.util.http.client.response.StatusResponseHandler;
import io.druid.java.util.http.client.response.StatusResponseHolder;
import io.druid.metadata.EntryExistsException;
import io.druid.query.jmx.JMXQueryRunnerFactory;
import io.druid.server.DruidNode;
import io.druid.server.http.security.ConfigResourceFilter;
import io.druid.server.http.security.StateResourceFilter;
import io.druid.server.security.Access;
import io.druid.server.security.Action;
import io.druid.server.security.AuthorizationUtils;
import io.druid.server.security.AuthorizerMapper;
import io.druid.server.security.ForbiddenException;
import io.druid.server.security.Resource;
import io.druid.server.security.ResourceAction;
import io.druid.server.security.ResourceType;
import io.druid.tasklogs.TaskLogStreamer;
import io.druid.timeline.DataSegment;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 */
@Path("/druid/indexer/v1")
public class OverlordResource
{
  private static final Logger log = new Logger(OverlordResource.class);

  private final TaskMaster taskMaster;
  private final TaskStorageQueryAdapter taskStorageQueryAdapter;
  private final IndexerMetadataStorageAdapter indexerMetadataStorageAdapter;
  private final TaskLockbox lockbox;

  private final TaskLogStreamer taskLogStreamer;
  private final JacksonConfigManager configManager;
  private final AuditManager auditManager;
  private final AuthorizerMapper authorizerMapper;
  private final HttpClient client;
  private final DruidNode node;
  private final ObjectMapper jsonMapper;

  private AtomicReference<WorkerBehaviorConfig> workerConfigRef = null;
  private static final List API_TASK_STATES = ImmutableList.of("pending", "waiting", "running", "complete");


  @Inject
  public OverlordResource(
      TaskMaster taskMaster,
      TaskStorageQueryAdapter taskStorageQueryAdapter,
      IndexerMetadataStorageAdapter indexerMetadataStorageAdapter,
      TaskLockbox lockbox,
      TaskLogStreamer taskLogStreamer,
      JacksonConfigManager configManager,
      AuditManager auditManager,
      AuthorizerMapper authorizerMapper,
      @EscalatedGlobal HttpClient client,
      @Self DruidNode node,
      ObjectMapper jsonMapper
  )
  {
    this.taskMaster = taskMaster;
    this.taskStorageQueryAdapter = taskStorageQueryAdapter;
    this.indexerMetadataStorageAdapter = indexerMetadataStorageAdapter;
    this.lockbox = lockbox;
    this.taskLogStreamer = taskLogStreamer;
    this.configManager = configManager;
    this.auditManager = auditManager;
    this.authorizerMapper = authorizerMapper;
    this.client = client;
    this.node = node;
    this.jsonMapper = jsonMapper;
  }

  @POST
  @Path("/task")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response taskPost(
      final Task task,
      @Context final HttpServletRequest req
  )
  {
    final String dataSource = task.getDataSource();
    final ResourceAction resourceAction = ResourceAction.write(Resource.dataSource(dataSource));
    final Access authResult = AuthorizationUtils.authorizeResourceAction(
        req,
        resourceAction,
        authorizerMapper
    );
    if (!authResult.isAllowed()) {
      return Response.status(Response.Status.FORBIDDEN).header("Access-Check-Result", authResult).build();
    }

    return asLeaderWith(
        taskMaster.getTaskQueue(),
        new Function<TaskQueue, Response>()
        {
          @Override
          public Response apply(TaskQueue taskQueue)
          {
            try {
              taskQueue.add(task);
              return Response.ok(ImmutableMap.of("task", task.getId())).build();
            }
            catch (EntryExistsException e) {
              return Response.status(Response.Status.BAD_REQUEST)
                             .entity(ImmutableMap.of("error", String.format("Task[%s] already exists!", task.getId())))
                             .build();
            }
          }
        }
    );
  }

  @GET
  @Path("/leader")
  @ResourceFilters(StateResourceFilter.class)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getLeader()
  {
    return Response.ok(taskMaster.getLeader()).build();
  }

  private Map<String, Object> toTaskDetail(
      final String taskId,
      final Collection<TaskRunnerWorkItem> pending,
      final Collection<TaskRunnerWorkItem> running
  )
  {
    Map<String, Object> result = Maps.newLinkedHashMap();
    result.put("task", taskId);

    Optional<Task> task = taskStorageQueryAdapter.getTask(taskId);
    Optional<TaskStatus> status = taskStorageQueryAdapter.getStatus(taskId);
    if (!task.isPresent() || !status.isPresent()) {
      result.put("status", "UNKNOWN");
      return result;
    }
    TaskState code = status.get().getStatusCode();
    result.put("status", code.name());

    if (code != TaskState.RUNNING) {
      return result;
    }
    Collection<? extends TaskRunnerWorkItem> pendingItems = Collections2.filter(
        pending, new Predicate<TaskRunnerWorkItem>()
        {
          @Override
          public boolean apply(TaskRunnerWorkItem input)
          {
            return taskId.equals(input.getTaskId());
          }
        }
    );
    Collection<? extends TaskRunnerWorkItem> runningItems = Collections2.filter(
        running, new Predicate<TaskRunnerWorkItem>()
        {
          @Override
          public boolean apply(TaskRunnerWorkItem input)
          {
            return taskId.equals(input.getTaskId());
          }
        }
    );
    if (pending.isEmpty() && running.isEmpty()) {
      result.put("statusDetail", "WAITING");
      return result;
    }
    StringBuilder builder = new StringBuilder();
    String detail = getRunningStatusDetail(taskId, task.get(), "");
    if (!pending.isEmpty()) {
      builder.append(String.format("PENDING(%d)", pendingItems.size()));
    }
    if (!running.isEmpty()) {
      if (builder.length() > 0) {
        builder.append(", ");
      }
      builder.append(String.format("RUNNING(%d)", running.size()));
    }
    if (!detail.isEmpty()) {
      builder.append(", ").append(detail);
    }
    result.put("statusDetail", builder.toString());

    List<String> locations = Lists.newArrayList();
    for (TaskRunnerWorkItem item : pendingItems) {
      TaskLocation location = item.getLocation();
      if (location != TaskLocation.unknown()) {
        locations.add(location.getHost() + ":" + location.getPort());
      }
    }
    for (TaskRunnerWorkItem item : runningItems) {
      TaskLocation location = item.getLocation();
      if (location != TaskLocation.unknown()) {
        locations.add(location.getHost() + ":" + location.getPort());
      }
    }
    result.put("locations", locations);
    return result;
  }

  @GET
  @Path("/task/{taskid}")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(TaskResourceFilter.class)
  public Response getTaskPayload(@PathParam("taskid") String taskid)
  {
    return optionalTaskResponse(taskid, "payload", taskStorageQueryAdapter.getTask(taskid));
  }

  @GET
  @Path("/task/{taskid}/status")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(TaskResourceFilter.class)
  public Response getTaskStatus(@PathParam("taskid") String taskid)
  {
    Optional<Task> task = taskStorageQueryAdapter.getTask(taskid);
    Optional<TaskStatus> status = taskStorageQueryAdapter.getStatus(taskid);
    Optional<TaskRunner> taskRunner = taskMaster.getTaskRunner();
    if (taskRunner.isPresent() && task.isPresent() &&
        status.isPresent() && status.get().getStatusCode() == TaskState.RUNNING) {
      Map<String, Object> results = makeStatusDetail(taskid, task.get(), taskRunner.get());
      results.put("task", taskid);
      results.put("status", status.get());

      return Response.status(Response.Status.OK).entity(results).build();
    }
    return optionalTaskResponse(taskid, "status", status);
  }

  @GET
  @Path("/task/{taskid}/segments")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(TaskResourceFilter.class)
  public Response getTaskSegments(@PathParam("taskid") String taskid)
  {
    final Set<DataSegment> segments = taskStorageQueryAdapter.getInsertedSegments(taskid);
    return Response.ok().entity(segments).build();
  }

  @POST
  @Path("/task/{taskid}/shutdown")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(TaskResourceFilter.class)
  public Response doShutdown(@PathParam("taskid") final String taskid)
  {
    return asLeaderWith(
        taskMaster.getTaskQueue(),
        new Function<TaskQueue, Response>()
        {
          @Override
          public Response apply(TaskQueue taskQueue)
          {
            taskQueue.shutdown(taskid);
            return Response.ok(ImmutableMap.of("task", taskid)).build();
          }
        }
    );
  }

  @GET
  @Path("/worker")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(ConfigResourceFilter.class)
  public Response getWorkerConfig()
  {
    if (workerConfigRef == null) {
      workerConfigRef = configManager.watch(WorkerBehaviorConfig.CONFIG_KEY, WorkerBehaviorConfig.class);
    }

    return Response.ok(workerConfigRef.get()).build();
  }

  // default value is used for backwards compatibility
  @POST
  @Path("/worker")
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(ConfigResourceFilter.class)
  public Response setWorkerConfig(
      final WorkerBehaviorConfig workerBehaviorConfig,
      @HeaderParam(AuditManager.X_DRUID_AUTHOR) @DefaultValue("") final String author,
      @HeaderParam(AuditManager.X_DRUID_COMMENT) @DefaultValue("") final String comment,
      @Context final HttpServletRequest req
  )
  {
    if (!configManager.set(
        WorkerBehaviorConfig.CONFIG_KEY,
        workerBehaviorConfig,
        new AuditInfo(author, comment, req.getRemoteAddr())
    )) {
      return Response.status(Response.Status.BAD_REQUEST).build();
    }

    log.info("Updating Worker configs: %s", workerBehaviorConfig);

    return Response.ok().build();
  }

  @GET
  @Path("/worker/history")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(ConfigResourceFilter.class)
  public Response getWorkerConfigHistory(
      @QueryParam("interval") final String interval,
      @QueryParam("count") final Integer count
  )
  {
    Interval theInterval = interval == null ? null : new Interval(interval);
    if (theInterval == null && count != null) {
      try {
        return Response.ok(
            auditManager.fetchAuditHistory(
                WorkerBehaviorConfig.CONFIG_KEY,
                WorkerBehaviorConfig.CONFIG_KEY,
                count
            )
        )
                       .build();
      }
      catch (IllegalArgumentException e) {
        return Response.status(Response.Status.BAD_REQUEST)
                       .entity(ImmutableMap.<String, Object>of("error", e.getMessage()))
                       .build();
      }
    }
    return Response.ok(
        auditManager.fetchAuditHistory(
            WorkerBehaviorConfig.CONFIG_KEY,
            WorkerBehaviorConfig.CONFIG_KEY,
            theInterval
        )
    )
                   .build();
  }

  @POST
  @Path("/action")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(StateResourceFilter.class)
  public Response doAction(final TaskActionHolder holder)
  {
    return asLeaderWith(
        taskMaster.getTaskActionClient(holder.getTask()),
        new Function<TaskActionClient, Response>()
        {
          @Override
          public Response apply(TaskActionClient taskActionClient)
          {
            final Map<String, Object> retMap;

            // It would be great to verify that this worker is actually supposed to be running the task before
            // actually doing the action.  Some ideas for how that could be done would be using some sort of attempt_id
            // or token that gets passed around.

            try {
              final Object ret = taskActionClient.submit(holder.getAction());
              retMap = Maps.newHashMap();
              retMap.put("result", ret);
            }
            catch (IllegalArgumentException | IllegalStateException e) {
              log.warn(e, "Failed to perform task action (not acceptable)");
              return Response.status(Response.Status.NOT_ACCEPTABLE).build();
            }
            catch (IOException e) {
              log.warn(e, "Failed to perform task action");
              return Response.serverError().build();
            }

            return Response.ok().entity(retMap).build();
          }
        }
    );
  }

  @GET
  @Path("/waitingTasks")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getWaitingTasks(@Context final HttpServletRequest req)
  {
    return getTasks("waiting", null, null, null, null, req, null);
  }

  @GET
  @Path("/pendingTasks")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getPendingTasks(@Context final HttpServletRequest req)
  {
    return getTasks("pending", null, null, null, null, req, null);
  }

  @GET
  @Path("/runningTasks")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getRunningTasks(
      @QueryParam("type") String taskType,
      @Context final HttpServletRequest req)
  {
    return getTasks("running", null, null, null, taskType, req, null);
  }

  @GET
  @Path("/completeTasks")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getCompleteTasks(
      @QueryParam("n") final Integer maxTaskStatuses,
      @Context final HttpServletRequest req
  )
  {
    return getTasks("complete", null, null, maxTaskStatuses, null, req, null);
  }

  @DELETE
  @Path("/pendingSegments/{dataSource}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response killPendingSegments(
          @PathParam("dataSource") String dataSource,
          @QueryParam("interval") String deleteIntervalString,
          @Context HttpServletRequest request
  )
  {
    final Interval deleteInterval = Intervals.of(deleteIntervalString);
    // check auth for dataSource
    final Access authResult = AuthorizationUtils.authorizeAllResourceActions(
            request,
            ImmutableList.of(
                    new ResourceAction(new Resource(dataSource, ResourceType.DATASOURCE), Action.READ),
                    new ResourceAction(new Resource(dataSource, ResourceType.DATASOURCE), Action.WRITE)
            ),
            authorizerMapper
    );

    if (!authResult.isAllowed()) {
      throw new ForbiddenException(authResult.getMessage());
    }

    if (taskMaster.isLeading()) {
      final int numDeleted = indexerMetadataStorageAdapter.deletePendingSegments(dataSource, deleteInterval);
      return Response.ok().entity(ImmutableMap.of("numDeleted", numDeleted)).build();
    } else {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
    }
  }

  @GET
  @Path("/locks/{dataSource}/{interval}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getLocksInInterval(
      @PathParam("dataSource") final String dataSource,
      @PathParam("interval") final String interval
  )
  {
    final Interval theInterval = interval.equals("_") ? Intervals.ETERNITY : new Interval(interval.replace("_", "/"));
    final Map<Interval, Map<String, Object>> result = new TreeMap<Interval, Map<String, Object>>(
        JodaUtils.intervalsByStartThenEnd()
    );

    Map<String, NavigableMap<Interval, TaskLockbox.TaskLockPosse>> locks = lockbox.getLocks();
    Iterable<Map<Interval, TaskLockbox.TaskLockPosse>> targets;
    if (dataSource.equals("_")) {
      targets = Iterables.concat(locks.values());
    } else {
      Map<Interval, TaskLockbox.TaskLockPosse> map = locks.get(dataSource);
      if (GuavaUtils.isNullOrEmpty(map)) {
        return Response.ok(Arrays.asList()).build();
      }
      targets = Arrays.asList(map);
    }

    for (Map<Interval, TaskLockbox.TaskLockPosse> values : targets) {
      for (Map.Entry<Interval, TaskLockbox.TaskLockPosse> entry : values.entrySet()) {
        if (theInterval.overlaps(entry.getKey())) {
          TaskLock lock = entry.getValue().getTaskLock();
          result.put(
              entry.getKey(),
              ImmutableMap.of(
                  "datasource", lock.getDataSource(),
                  "interval", entry.getKey(),
                  "version", lock.getVersion(),
                  "tasks", entry.getValue().getTaskIds()
              )
          );
        }
      }
    }
    return Response.ok(result.values()).build();
  }

  @GET
  @Path("/tasks")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTasks(
      @QueryParam("state") final String state,
      @QueryParam("datasource") final String dataSource,
      @QueryParam("period") final String period,
      @QueryParam("max") final Integer maxCompletedTasks,
      @QueryParam("type") final String type,
      @Context final HttpServletRequest req,
      @QueryParam("simple") final String simple
  )
  {
    //check for valid state
    if (state != null) {
      if (!API_TASK_STATES.contains(StringUtils.toLowerCase(state))) {
        return Response.status(Response.Status.BAD_REQUEST)
                       .entity(StringUtils.format("Invalid state : %s, valid values are: %s", state, API_TASK_STATES))
                       .build();
      }
    }

    // early authorization check if datasource != null
    // fail fast if user not authorized to access datasource
    if (dataSource != null) {
      final ResourceAction resourceAction = ResourceAction.read(Resource.dataSource(dataSource));

      final Access authResult = AuthorizationUtils.authorizeResourceAction(
          req,
          resourceAction,
          authorizerMapper
      );
      if (!authResult.isAllowed()) {
        throw new WebApplicationException(
            Response.status(Response.Status.FORBIDDEN)
                    .entity(StringUtils.format("Access-Check-Result: %s", authResult.toString()))
                    .build()
        );
      }
    }
    List<TaskStatusPlus> finalTaskList = new ArrayList<>();
    Function<AnyTask, TaskStatusPlus> activeTaskTransformFunc = new Function<AnyTask, TaskStatusPlus>()
    {
      @Nullable
      @Override
      public TaskStatusPlus apply(@Nullable AnyTask workItem)
      {
        return new TaskStatusPlus(
            workItem.getTaskId(),
            workItem.getTaskType(),
            workItem.getCreatedTime(),
            workItem.getQueueInsertionTime(),
            workItem.getTaskState(),
            workItem.getRunnerTaskState(),
            null,
            workItem.getLocation(),
            workItem.getDataSource(),
            null
        );
      }
    };

    Function<TaskInfo<Task, TaskStatus>, TaskStatusPlus> completeTaskTransformFunc = new Function<TaskInfo<Task, TaskStatus>, TaskStatusPlus>()
    {
      @Nullable
      @Override
      public TaskStatusPlus apply(@Nullable TaskInfo<Task, TaskStatus> taskInfo)
      {
        return new TaskStatusPlus(
            taskInfo.getId(),
            taskInfo.getTask().getType(),
            taskInfo.getCreatedTime(),
            // Would be nice to include the real queue insertion time, but the
            // TaskStorage API doesn't yet allow it.
            DateTimes.EPOCH,
            taskInfo.getStatus().getStatusCode(),
            RunnerTaskState.NONE,
            taskInfo.getStatus().getDuration(),
            TaskLocation.unknown(),
            taskInfo.getDataSource(),
            taskInfo.getStatus().getReason()
        );
      }
    };

    //checking for complete tasks first to avoid querying active tasks if user only wants complete tasks
    if (state == null || "complete".equals(StringUtils.toLowerCase(state))) {
      Duration duration = null;
      if (period != null) {
        duration = new Period(period).toStandardDuration();
      }
      final List<TaskInfo<Task, TaskStatus>> taskInfoList = taskStorageQueryAdapter.getRecentlyCompletedTaskInfo(
          maxCompletedTasks, duration, dataSource
      );
      final List<TaskStatusPlus> completedTasks = Lists.transform(taskInfoList, completeTaskTransformFunc);
      finalTaskList.addAll(completedTasks);
    }

    final List<TaskInfo<Task, TaskStatus>> allActiveTaskInfo;
    final List<AnyTask> allActiveTasks = Lists.newArrayList();
    if (state == null || !"complete".equals(StringUtils.toLowerCase(state))) {
      allActiveTaskInfo = taskStorageQueryAdapter.getActiveTaskInfo(dataSource);
      for (final TaskInfo<Task, TaskStatus> task : allActiveTaskInfo) {
        allActiveTasks.add(
            new AnyTask(
                task.getId(),
                task.getTask() == null ? null : task.getTask().getType(),
                SettableFuture.<TaskStatus>create(),
                task.getDataSource(),
                null,
                null,
                task.getCreatedTime(),
                DateTimes.EPOCH,
                TaskLocation.unknown()
            ));

      }
    }
    if (state == null || "waiting".equals(StringUtils.toLowerCase(state))) {
      final List<AnyTask> waitingWorkItems = filterActiveTasks(RunnerTaskState.WAITING, allActiveTasks);
      List<TaskStatusPlus> transformedWaitingList = Lists.transform(waitingWorkItems, activeTaskTransformFunc);
      finalTaskList.addAll(transformedWaitingList);
    }
    if (state == null || "pending".equals(StringUtils.toLowerCase(state))) {
      final List<AnyTask> pendingWorkItems = filterActiveTasks(RunnerTaskState.PENDING, allActiveTasks);
      List<TaskStatusPlus> transformedPendingList = Lists.transform(pendingWorkItems, activeTaskTransformFunc);
      finalTaskList.addAll(transformedPendingList);
    }
    if (state == null || "running".equals(StringUtils.toLowerCase(state))) {
      final List<AnyTask> runningWorkItems = filterActiveTasks(RunnerTaskState.RUNNING, allActiveTasks);
      List<TaskStatusPlus> transformedRunningList = Lists.transform(runningWorkItems, activeTaskTransformFunc);
      finalTaskList.addAll(transformedRunningList);
    }
    final List<TaskStatusPlus> authorizedList = securedTaskStatusPlus(
        finalTaskList,
        dataSource,
        type,
        req
    );
    if (simple != null) {
      List<String> ids = new ArrayList<>();
      for (TaskStatusPlus taskStatusPlus: authorizedList) {
        ids.add(taskStatusPlus.getId());
      }
      return Response.ok(ids).build();
    }
    return Response.ok(authorizedList).build();
  }

  @GET
  @Path("/workers")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(StateResourceFilter.class)
  public Response getWorkers()
  {
    return asLeaderWith(
        taskMaster.getTaskRunner(),
        new Function<TaskRunner, Response>()
        {
          @Override
          public Response apply(TaskRunner taskRunner)
          {
            if (taskRunner instanceof WorkerTaskRunner) {
              return Response.ok(((WorkerTaskRunner) taskRunner).getWorkers()).build();
            } else {
              log.debug(
                  "Task runner [%s] of type [%s] does not support listing workers",
                  taskRunner,
                  taskRunner.getClass().getCanonicalName()
              );
              return Response.serverError()
                             .entity(ImmutableMap.of("error", "Task Runner does not support worker listing"))
                             .build();
            }
          }
        }
    );
  }

  @GET
  @Path("/scaling")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(StateResourceFilter.class)
  public Response getScalingState()
  {
    // Don't use asLeaderWith, since we want to return 200 instead of 503 when missing an autoscaler.
    final Optional<ScalingStats> rms = taskMaster.getScalingStats();
    if (rms.isPresent()) {
      return Response.ok(rms.get()).build();
    } else {
      return Response.ok().build();
    }
  }

  @GET
  @Path("/task/{taskid}/log")
  @Produces("text/plain")
  @ResourceFilters(TaskResourceFilter.class)
  public Response doGetLog(
      @PathParam("taskid") final String taskid,
      @QueryParam("offset") @DefaultValue("0") final long offset
  )
  {
    try {
      final Optional<ByteSource> stream = taskLogStreamer.streamTaskLog(taskid, offset);
      if (stream.isPresent()) {
        return Response.ok(stream.get().openStream()).build();
      } else {
        return Response.status(Response.Status.NOT_FOUND)
                       .entity(
                           "No log was found for this task. "
                           + "The task may not exist, or it may not have begun running yet."
                       )
                       .build();
      }
    }
    catch (Exception e) {
      log.warn(e, "Failed to stream log for task %s", taskid);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  @GET
  @Path("/jmx")
  @Produces(MediaType.APPLICATION_JSON)
  public Response handleJMX(@QueryParam("dumpLongestStack") boolean dumpLongestStack)
  {
    Map<String, Object> results = JMXQueryRunnerFactory.queryJMX(node, null, dumpLongestStack);
    return Response.ok(results).build();
  }

  private Response workItemsResponse(final Function<TaskRunner, Collection<? extends TaskRunnerWorkItem>> fn)
  {
    return asLeaderWith(
        taskMaster.getTaskRunner(),
        new Function<TaskRunner, Response>()
        {
          @Override
          public Response apply(TaskRunner taskRunner)
          {
            return Response.ok(
                Lists.transform(
                    Lists.newArrayList(fn.apply(taskRunner)),
                    new Function<TaskRunnerWorkItem, TaskResponseObject>()
                    {
                      @Override
                      public TaskResponseObject apply(TaskRunnerWorkItem workItem)
                      {
                        return new TaskResponseObject(
                            workItem.getTaskId(),
                            workItem.getCreatedTime(),
                            workItem.getQueueInsertionTime(),
                            Optional.<TaskStatus>absent(),
                            workItem.getLocation()
                        );
                      }
                    }
                )
            ).build();
          }
        }
    );
  }

  private <T> Response optionalTaskResponse(String taskid, String objectType, Optional<T> x)
  {
    final Map<String, Object> results = Maps.newHashMap();
    results.put("task", taskid);
    if (x.isPresent()) {
      results.put(objectType, x.get());
      return Response.status(Response.Status.OK).entity(results).build();
    } else {
      return Response.status(Response.Status.NOT_FOUND).entity(results).build();
    }
  }

  private <T> Response asLeaderWith(Optional<T> x, Function<T, Response> f)
  {
    if (x.isPresent()) {
      return f.apply(x.get());
    } else {
      // Encourage client to try again soon, when we'll likely have a redirect set up
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
    }
  }

  private List<AnyTask> filterActiveTasks(
      RunnerTaskState state,
      List<OverlordResource.AnyTask> allTasks
  )
  {
    //divide active tasks into 3 lists : running, pending, waiting
    Optional<TaskRunner> taskRunnerOpt = taskMaster.getTaskRunner();
    if (!taskRunnerOpt.isPresent()) {
      throw new WebApplicationException(
          Response.serverError().entity("No task runner found").build()
      );
    }
    TaskRunner runner = taskRunnerOpt.get();
    // the order of tasks below is waiting, pending, running to prevent
    // skipping a task, it's the order in which tasks will change state
    // if they do while this is code is executing, so a task might be
    // counted twice but never skipped
    if (RunnerTaskState.WAITING.equals(state)) {
      Collection<? extends TaskRunnerWorkItem> runnersKnownTasks = runner.getKnownTasks();
      Set<String> runnerKnownTaskIds = new HashSet<>();
      for (TaskRunnerWorkItem workItem : runnersKnownTasks) {
        runnerKnownTaskIds.add(workItem.getTaskId());
      }
      final List<OverlordResource.AnyTask> waitingTasks = Lists.newArrayList();
      for (TaskRunnerWorkItem task : allTasks) {
        if (!runnerKnownTaskIds.contains(task.getTaskId())) {
          waitingTasks.add(((OverlordResource.AnyTask) task).withTaskState(
              TaskState.RUNNING,
              RunnerTaskState.WAITING,
              task.getCreatedTime(),
              task.getQueueInsertionTime(),
              task.getLocation()
          ));
        }
      }
      return waitingTasks;
    }

    if (RunnerTaskState.PENDING.equals(state)) {
      Collection<? extends TaskRunnerWorkItem> knownPendingTasks = runner.getPendingTasks();
      Set<String> pendingTaskIds = new HashSet<>();
      for (TaskRunnerWorkItem workItem : knownPendingTasks) {
        pendingTaskIds.add(workItem.getTaskId());
      }
      Map<String, TaskRunnerWorkItem> workItemIdMap = new HashMap<>();
      for (TaskRunnerWorkItem knownPendingTask : knownPendingTasks) {
        workItemIdMap.put(knownPendingTask.getTaskId(), knownPendingTask);
      }
      final List<OverlordResource.AnyTask> pendingTasks = Lists.newArrayList();
      for (TaskRunnerWorkItem task : allTasks) {
        if (pendingTaskIds.contains(task.getTaskId())) {
          pendingTasks.add(((OverlordResource.AnyTask) task).withTaskState(
              TaskState.RUNNING,
              RunnerTaskState.PENDING,
              workItemIdMap.get(task.getTaskId()).getCreatedTime(),
              workItemIdMap.get(task.getTaskId()).getQueueInsertionTime(),
              workItemIdMap.get(task.getTaskId()).getLocation()
          ));
        }
      }
      return pendingTasks;
    }

    if (RunnerTaskState.RUNNING.equals(state)) {
      Collection<? extends TaskRunnerWorkItem> knownRunningTasks = runner.getRunningTasks();
      Set<String> runningTaskIds = new HashSet<>();
      for (TaskRunnerWorkItem knownRunningTask : knownRunningTasks) {
        String taskId = knownRunningTask.getTaskId();
        runningTaskIds.add(taskId);
      }
      Map<String, TaskRunnerWorkItem> workItemIdMap = new HashMap<>();
      for (TaskRunnerWorkItem knownRunningTask : knownRunningTasks) {
        workItemIdMap.put(knownRunningTask.getTaskId(), knownRunningTask);
      }
      final List<OverlordResource.AnyTask> runningTasks = Lists.newArrayList();
      for (TaskRunnerWorkItem task : allTasks) {
        if (runningTaskIds.contains(task.getTaskId())) {
          runningTasks.add(((OverlordResource.AnyTask) task).withTaskState(
              TaskState.RUNNING,
              RunnerTaskState.RUNNING,
              workItemIdMap.get(task.getTaskId()).getCreatedTime(),
              workItemIdMap.get(task.getTaskId()).getQueueInsertionTime(),
              workItemIdMap.get(task.getTaskId()).getLocation()
          ));
        }
      }
      return runningTasks;
    }
    return allTasks;
  }

  private List<TaskStatusPlus> securedTaskStatusPlus(
      List<TaskStatusPlus> collectionToFilter,
      @Nullable String dataSource,
      @Nullable String type,
      HttpServletRequest req
  )
  {
    Function<TaskStatusPlus, Iterable<ResourceAction>> raGenerator = taskStatusPlus -> {
      final String taskId = taskStatusPlus.getId();
      final String taskDatasource = taskStatusPlus.getDataSource();
      if (taskDatasource == null) {
        throw new WebApplicationException(
            Response.serverError().entity(
                StringUtils.format("No task information found for task with id: [%s]", taskId)
            ).build()
        );
      }
      return Collections.singletonList(
          new ResourceAction(new Resource(taskDatasource, ResourceType.DATASOURCE), Action.READ)
      );
    };
    List<TaskStatusPlus> optionalTypeFilteredList = collectionToFilter;
    if (type != null) {
      optionalTypeFilteredList = collectionToFilter
          .stream()
          .filter(task -> type.equals(task.getType()))
          .collect(Collectors.toList());
    }
    if (dataSource != null) {
      //skip auth check here, as it's already done in getTasks
      return optionalTypeFilteredList;
    }
    return Lists.newArrayList(
        AuthorizationUtils.filterAuthorizedResources(
            req,
            optionalTypeFilteredList,
            raGenerator,
            authorizerMapper
        )
    );
  }

  /**
   * Use {@link TaskStatusPlus}
   */
  @Deprecated
  static class TaskResponseObject
  {
    private final String id;
    private final DateTime createdTime;
    private final DateTime queueInsertionTime;
    private final Optional<TaskStatus> status;
    private final TaskLocation location;

    private TaskResponseObject(
        String id,
        DateTime createdTime,
        DateTime queueInsertionTime,
        Optional<TaskStatus> status,
        TaskLocation location
    )
    {
      this.id = id;
      this.createdTime = createdTime;
      this.queueInsertionTime = queueInsertionTime;
      this.status = status;
      this.location = location;
    }

    @JsonValue
    public Map<String, Object> toJson()
    {
      final Map<String, Object> data = Maps.newLinkedHashMap();
      data.put("id", id);
      if (createdTime.getMillis() > 0) {
        data.put("createdTime", createdTime);
      }
      if (queueInsertionTime.getMillis() > 0) {
        data.put("queueInsertionTime", queueInsertionTime);
      }
      if (status.isPresent()) {
        data.put("statusCode", status.get().getStatusCode().toString());
        if (status.get().isComplete()) {
          data.put("duration", status.get().getDuration());
        }
      }
      if (location != null) {
        data.put("location", location);
      }
      return data;
    }
  }

  private Map<String, Object> makeStatusDetail(String taskId, Task task, TaskRunner taskRunner)
  {
    Map<String, Object> status = Maps.newHashMap();
    if (isPendingTask(taskId, taskRunner)) {
      status.put("statusDetail", "PENDING");  //in queue
    } else {
      status.put("statusDetail", getRunningStatusDetail(taskId, task, "RUNNING"));
    }
    float progress = workerProgress(taskRunner.getWorkerItem(taskId));
    if (progress >= 0) {
      status.put("progress", Math.round(progress * 1000) / 1000);
    }
    return status;
  }

  private float workerProgress(TaskRunnerWorkItem workerItem)
  {
    float progress = -1;
    if (workerItem instanceof ThreadPoolTaskRunner.ThreadPoolTaskRunnerWorkItem) {
      progress = ((ThreadPoolTaskRunner.ThreadPoolTaskRunnerWorkItem)workerItem).getTask().progress();
    } else if (workerItem != null && workerItem.getLocation() != TaskLocation.unknown()) {
      try {
        progress = execute(workerItem.getLocation(), "/task/" + workerItem.getTaskId(), Float.class);
      }
      catch (Exception e) {
        // ignore
      }
    }
    return progress;
  }

  private <T> T execute(TaskLocation location, String resource, Class<T> resultType)
  {
    try {
      URL url = new URL(String.format("http://%s:%d/druid/v2%s", location.getHost(), location.getPort(), resource));
      Request request = new Request(HttpMethod.GET, url);
      StatusResponseHolder response = client.go(request, new StatusResponseHandler(Charsets.UTF_8)).get();
      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE(
            "Error while fetching serverView status[%s] content[%s]",
            response.getStatus(),
            response.getContent()
        );
      }
      return jsonMapper.readValue(response.getContent(), resultType);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private String getRunningStatusDetail(String taskId, Task task, String defaultValue)
  {
    if (task instanceof IndexTask ||
        task instanceof RealtimeIndexTask ||
        task.getClass().getName().contains("HadoopIndexTask")) {
      List<TaskLock> locks = taskStorageQueryAdapter.getLocks(taskId);
      if (locks.isEmpty()) {
        return "INITIALIZING";
      }
      if (locks.size() == 1 && locks.get(0).getInterval().getEndMillis() == JodaUtils.MAX_INSTANT) {
        return "READY";   // ready to get events (RealtimeIndexTask only)
      }
    }
    return defaultValue;
  }

  private boolean isPendingTask(String taskId, TaskRunner taskRunner)
  {
    for (TaskRunnerWorkItem workItem : taskRunner.getPendingTasks()) {
      if (taskId.equals(workItem.getTaskId())) {
        return true;
      }
    }
    return false;
  }


  private static class AnyTask extends TaskRunnerWorkItem
  {
    private final String taskType;
    private final String dataSource;
    private final TaskState taskState;
    private final RunnerTaskState runnerTaskState;
    private final DateTime createdTime;
    private final DateTime queueInsertionTime;
    private final TaskLocation taskLocation;

    AnyTask(
        String taskId,
        String taskType,
        ListenableFuture<TaskStatus> result,
        String dataSource,
        TaskState state,
        RunnerTaskState runnerState,
        DateTime createdTime,
        DateTime queueInsertionTime,
        TaskLocation taskLocation
    )
    {
      super(taskId, result, DateTimes.EPOCH, DateTimes.EPOCH);
      this.taskType = taskType;
      this.dataSource = dataSource;
      this.taskState = state;
      this.runnerTaskState = runnerState;
      this.createdTime = createdTime;
      this.queueInsertionTime = queueInsertionTime;
      this.taskLocation = taskLocation;
    }

    @Override
    public TaskLocation getLocation()
    {
      return taskLocation;
    }

    @Override
    public String getTaskType()
    {
      return taskType;
    }

    @Override
    public String getDataSource()
    {
      return dataSource;
    }

    public TaskState getTaskState()
    {
      return taskState;
    }

    public RunnerTaskState getRunnerTaskState()
    {
      return runnerTaskState;
    }

    @Override
    public DateTime getCreatedTime()
    {
      return createdTime;
    }

    @Override
    public DateTime getQueueInsertionTime()
    {
      return queueInsertionTime;
    }

    public AnyTask withTaskState(
        TaskState newTaskState,
        RunnerTaskState runnerState,
        DateTime createdTime,
        DateTime queueInsertionTime,
        TaskLocation taskLocation
    )
    {
      return new AnyTask(
          getTaskId(),
          getTaskType(),
          getResult(),
          getDataSource(),
          newTaskState,
          runnerState,
          createdTime,
          queueInsertionTime,
          taskLocation
      );
    }
  }

}
