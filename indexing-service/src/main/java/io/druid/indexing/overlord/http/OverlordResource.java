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

package io.druid.indexing.overlord.http;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteSource;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.Pair;
import com.metamx.common.logger.Logger;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import com.sun.jersey.spi.container.ResourceFilters;
import io.druid.audit.AuditInfo;
import io.druid.audit.AuditManager;
import io.druid.common.config.JacksonConfigManager;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.JodaUtils;
import io.druid.guice.annotations.Global;
import io.druid.guice.annotations.Self;
import io.druid.indexing.common.TaskLocation;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.actions.TaskActionHolder;
import io.druid.indexing.common.task.HadoopIndexTask;
import io.druid.indexing.common.task.IndexTask;
import io.druid.indexing.common.task.RealtimeIndexTask;
import io.druid.indexing.common.task.Task;
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
import io.druid.metadata.EntryExistsException;
import io.druid.query.jmx.JMXQueryRunnerFactory;
import io.druid.server.DruidNode;
import io.druid.server.http.security.ConfigResourceFilter;
import io.druid.server.http.security.StateResourceFilter;
import io.druid.server.security.Access;
import io.druid.server.security.Action;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.AuthorizationInfo;
import io.druid.server.security.Resource;
import io.druid.server.security.ResourceType;
import io.druid.tasklogs.TaskLogStreamer;
import io.druid.timeline.DataSegment;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
@Path("/druid/indexer/v1")
public class OverlordResource
{
  private static final Logger log = new Logger(OverlordResource.class);

  private final TaskMaster taskMaster;
  private final TaskStorageQueryAdapter taskStorageQueryAdapter;
  private final TaskLogStreamer taskLogStreamer;
  private final JacksonConfigManager configManager;
  private final AuditManager auditManager;
  private final AuthConfig authConfig;
  private final HttpClient client;
  private final DruidNode node;
  private final ObjectMapper jsonMapper;

  private AtomicReference<WorkerBehaviorConfig> workerConfigRef = null;

  @Inject
  public OverlordResource(
      TaskMaster taskMaster,
      TaskStorageQueryAdapter taskStorageQueryAdapter,
      TaskLogStreamer taskLogStreamer,
      JacksonConfigManager configManager,
      AuditManager auditManager,
      AuthConfig authConfig,
      @Global HttpClient client,
      @Self DruidNode node,
      ObjectMapper jsonMapper
  )
  {
    this.taskMaster = taskMaster;
    this.taskStorageQueryAdapter = taskStorageQueryAdapter;
    this.taskLogStreamer = taskLogStreamer;
    this.configManager = configManager;
    this.auditManager = auditManager;
    this.authConfig = authConfig;
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
    if (authConfig.isEnabled()) {
      // This is an experimental feature, see - https://github.com/druid-io/druid/pull/2424
      final String dataSource = task.getDataSource();
      final AuthorizationInfo authorizationInfo = (AuthorizationInfo) req.getAttribute(AuthConfig.DRUID_AUTH_TOKEN);
      Preconditions.checkNotNull(
          authorizationInfo,
          "Security is enabled but no authorization info found in the request"
      );
      Access authResult = authorizationInfo.isAuthorized(
          new Resource(dataSource, ResourceType.DATASOURCE),
          Action.WRITE
      );
      if (!authResult.isAllowed()) {
        return Response.status(Response.Status.FORBIDDEN).header("Access-Check-Result", authResult).build();
      }
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

  @GET
  @Path("/tasks")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTasks(
      @QueryParam("full") String full,
      @QueryParam("completed") String completed,
      @QueryParam("recent") String recent,
      @Context final HttpServletRequest req)
  {
    if (completed != null) {
      List<TaskStatus> finished = taskStorageQueryAdapter.getRecentlyFinishedTaskStatuses(recent);
      if (full == null) {
        List<String> ids = Lists.newArrayList();
        for (TaskStatus status : finished) {
          ids.add(status.getId());
        }
        return Response.ok(ids).build();
      } else {
        return Response.ok(finished).build();
      }
    }
    List<Task> activeTasks = taskStorageQueryAdapter.getActiveTasks();
    if (authConfig.isEnabled()) {
      AuthorizationInfo authorization = (AuthorizationInfo) req.getAttribute(AuthConfig.DRUID_AUTH_TOKEN);
      Preconditions.checkNotNull(authorization, "Security is enabled but no authorization info found in the request");
      List<Task> filtered = Lists.newArrayList();
      for (Task task : activeTasks) {
        Access authResult = authorization.isAuthorized(
            new Resource(task.getDataSource(), ResourceType.DATASOURCE),
            Action.READ
        );
        if (authResult.isAllowed()) {
          filtered.add(task);
        }
      }
      activeTasks = filtered;
    }
    List<String> tasks = Lists.transform(
        activeTasks, new Function<Task, String>()
        {
          @Override
          public String apply(Task input)
          {
            return input.getId();
          }
        }
    );
    if (full == null) {
      return Response.ok(tasks).build();
    }

    final TaskRunner taskRunner = taskMaster.getTaskRunner().orNull();
    final Collection<TaskRunnerWorkItem> pending = getTaskWorkItems(taskRunner, Mode.PENDING, req);
    final Collection<TaskRunnerWorkItem> running = getTaskWorkItems(taskRunner, Mode.RUNNING, req);

    return Response.ok(
        Lists.transform(
            tasks, new Function<String, Map<String, Object>>()
            {
              @Override
              public Map<String, Object> apply(final String taskId)
              {
                return toTaskDetail(taskId, pending, running);
              }
            }
        )
    ).build();
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
    TaskStatus.Status code = status.get().getStatusCode();
    result.put("status", code.name());

    if (code != TaskStatus.Status.RUNNING) {
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
        status.isPresent() && status.get().getStatusCode() == TaskStatus.Status.RUNNING) {
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
    return workItemsResponse(
        new Function<TaskRunner, Collection<? extends TaskRunnerWorkItem>>()
        {
          @Override
          public Collection<? extends TaskRunnerWorkItem> apply(TaskRunner taskRunner)
          {
            // A bit roundabout, but works as a way of figuring out what tasks haven't been handed
            // off to the runner yet:
            final List<Task> allActiveTasks = taskStorageQueryAdapter.getActiveTasks();
            final List<Task> activeTasks;
            if (authConfig.isEnabled()) {
              // This is an experimental feature, see - https://github.com/druid-io/druid/pull/2424
              final Map<Pair<Resource, Action>, Access> resourceAccessMap = new HashMap<>();
              final AuthorizationInfo authorizationInfo =
                  (AuthorizationInfo) req.getAttribute(AuthConfig.DRUID_AUTH_TOKEN);
              activeTasks = ImmutableList.copyOf(
                  Iterables.filter(
                      allActiveTasks,
                      new Predicate<Task>()
                      {
                        @Override
                        public boolean apply(Task input)
                        {
                          Resource resource = new Resource(input.getDataSource(), ResourceType.DATASOURCE);
                          Action action = Action.READ;
                          Pair<Resource, Action> key = new Pair<>(resource, action);
                          if (resourceAccessMap.containsKey(key)) {
                            return resourceAccessMap.get(key).isAllowed();
                          } else {
                            Access access = authorizationInfo.isAuthorized(key.lhs, key.rhs);
                            resourceAccessMap.put(key, access);
                            return access.isAllowed();
                          }
                        }
                      }
                  )
              );
            } else {
              activeTasks = allActiveTasks;
            }
            final Set<String> runnersKnownTasks = Sets.newHashSet(
                Iterables.transform(
                    taskRunner.getKnownTasks(),
                    new Function<TaskRunnerWorkItem, String>()
                    {
                      @Override
                      public String apply(final TaskRunnerWorkItem workItem)
                      {
                        return workItem.getTaskId();
                      }
                    }
                )
            );
            final List<TaskRunnerWorkItem> waitingTasks = Lists.newArrayList();
            for (final Task task : activeTasks) {
              if (!runnersKnownTasks.contains(task.getId())) {
                waitingTasks.add(
                    // Would be nice to include the real created date, but the TaskStorage API doesn't yet allow it.
                    new TaskRunnerWorkItem(
                        task.getId(),
                        SettableFuture.<TaskStatus>create(),
                        new DateTime(0),
                        new DateTime(0)
                    )
                    {
                      @Override
                      public TaskLocation getLocation()
                      {
                        return TaskLocation.unknown();
                      }
                    }
                );
              }
            }
            return waitingTasks;
          }
        }
    );
  }

  @GET
  @Path("/pendingTasks")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getPendingTasks(@Context final HttpServletRequest req)
  {
    return workItemsResponse(
        new Function<TaskRunner, Collection<? extends TaskRunnerWorkItem>>()
        {
          @Override
          public Collection<? extends TaskRunnerWorkItem> apply(TaskRunner taskRunner)
          {
            return getTaskWorkItems(taskRunner, Mode.PENDING, req);
          }
        }
    );
  }

  @GET
  @Path("/runningTasks")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getRunningTasks(@Context final HttpServletRequest req)
  {
    return workItemsResponse(
        new Function<TaskRunner, Collection<? extends TaskRunnerWorkItem>>()
        {
          @Override
          public Collection<? extends TaskRunnerWorkItem> apply(TaskRunner taskRunner)
          {
            return getTaskWorkItems(taskRunner, Mode.RUNNING, req);
          }
        }
    );
  }

  @GET
  @Path("/knownTasks")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getAllTasks(@Context final HttpServletRequest req)
  {
    return workItemsResponse(
        new Function<TaskRunner, Collection<? extends TaskRunnerWorkItem>>()
        {
          @Override
          public Collection<? extends TaskRunnerWorkItem> apply(TaskRunner taskRunner)
          {
            return getTaskWorkItems(taskRunner, Mode.KNOWN, req);
          }
        }
    );
  }

  private static enum Mode
  {
    RUNNING, PENDING, KNOWN
  }

  private Collection<TaskRunnerWorkItem> getTaskWorkItems(TaskRunner taskRunner, Mode mode, HttpServletRequest req)
  {
    if (taskRunner == null) {
      return Collections.emptyList();
    }
    List<TaskRunnerWorkItem> tasks;
    switch (mode) {
      case RUNNING:
        tasks = GuavaUtils.cast(taskRunner.getRunningTasks());
        break;
      case PENDING:
        tasks = GuavaUtils.cast(taskRunner.getPendingTasks());
        break;
      default:
        tasks = GuavaUtils.cast(taskRunner.getKnownTasks());
    }
    if (authConfig.isEnabled()) {
      // This is an experimental feature, see - https://github.com/druid-io/druid/pull/2424
      return securedTaskRunnerWorkItem(tasks, req);
    } else {
      return tasks;
    }
  }

  @GET
  @Path("/completeTasks")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getCompleteTasks(
      @QueryParam("recent") String recent,
      @Context final HttpServletRequest req)
  {
    final List<TaskStatus> recentlyFinishedTasks;
    if (authConfig.isEnabled()) {
      // This is an experimental feature, see - https://github.com/druid-io/druid/pull/2424
      final Map<Pair<Resource, Action>, Access> resourceAccessMap = new HashMap<>();
      final AuthorizationInfo authorizationInfo = (AuthorizationInfo) req.getAttribute(AuthConfig.DRUID_AUTH_TOKEN);
      recentlyFinishedTasks = ImmutableList.copyOf(
          Iterables.filter(
              taskStorageQueryAdapter.getRecentlyFinishedTaskStatuses(recent),
              new Predicate<TaskStatus>()
              {
                @Override
                public boolean apply(TaskStatus input)
                {
                  final String taskId = input.getId();
                  final Optional<Task> optionalTask = taskStorageQueryAdapter.getTask(taskId);
                  if (!optionalTask.isPresent()) {
                    throw new WebApplicationException(
                        Response.serverError().entity(
                            String.format("No task information found for task with id: [%s]", taskId)
                        ).build()
                    );
                  }
                  Resource resource = new Resource(optionalTask.get().getDataSource(), ResourceType.DATASOURCE);
                  Action action = Action.READ;
                  Pair<Resource, Action> key = new Pair<>(resource, action);
                  if (resourceAccessMap.containsKey(key)) {
                    return resourceAccessMap.get(key).isAllowed();
                  } else {
                    Access access = authorizationInfo.isAuthorized(key.lhs, key.rhs);
                    resourceAccessMap.put(key, access);
                    return access.isAllowed();
                  }
                }
              }
          )
      );
    } else {
      recentlyFinishedTasks = taskStorageQueryAdapter.getRecentlyFinishedTaskStatuses(recent);
    }

    final List<TaskResponseObject> completeTasks = Lists.transform(
        recentlyFinishedTasks,
        new Function<TaskStatus, TaskResponseObject>()
        {
          @Override
          public TaskResponseObject apply(TaskStatus taskStatus)
          {
            // Would be nice to include the real created date, but the TaskStorage API doesn't yet allow it.
            return new TaskResponseObject(
                taskStatus.getId(),
                new DateTime(0),
                new DateTime(0),
                Optional.of(taskStatus),
                TaskLocation.unknown()
            );
          }
        }
    );
    return Response.ok(completeTasks).build();
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

  private Collection<TaskRunnerWorkItem> securedTaskRunnerWorkItem(
      Collection<TaskRunnerWorkItem> collectionToFilter,
      HttpServletRequest req
  )
  {
    final Map<Pair<Resource, Action>, Access> resourceAccessMap = new HashMap<>();
    final AuthorizationInfo authorizationInfo =
        (AuthorizationInfo) req.getAttribute(AuthConfig.DRUID_AUTH_TOKEN);
    return Collections2.filter(
        collectionToFilter,
        new Predicate<TaskRunnerWorkItem>()
        {
          @Override
          public boolean apply(TaskRunnerWorkItem input)
          {
            final String taskId = input.getTaskId();
            final Optional<Task> optionalTask = taskStorageQueryAdapter.getTask(taskId);
            if (!optionalTask.isPresent()) {
              throw new WebApplicationException(
                  Response.serverError().entity(
                      String.format("No task information found for task with id: [%s]", taskId)
                  ).build()
              );
            }
            Resource resource = new Resource(optionalTask.get().getDataSource(), ResourceType.DATASOURCE);
            Action action = Action.READ;
            Pair<Resource, Action> key = new Pair<>(resource, action);
            if (resourceAccessMap.containsKey(key)) {
              return resourceAccessMap.get(key).isAllowed();
            } else {
              Access access = authorizationInfo.isAuthorized(key.lhs, key.rhs);
              resourceAccessMap.put(key, access);
              return access.isAllowed();
            }
          }
        }
    );
  }

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
    if (task instanceof IndexTask || task instanceof RealtimeIndexTask || task instanceof HadoopIndexTask) {
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
}
