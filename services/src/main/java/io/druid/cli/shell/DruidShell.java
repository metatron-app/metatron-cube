/*
 * Licensed to SK Telecom Co., LTD. (SK Telecom) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  SK Telecom licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package io.druid.cli.shell;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.io.AppendableByteArrayInputStream;
import com.metamx.http.client.response.ClientResponse;
import com.metamx.http.client.response.InputStreamResponseHandler;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.StringUtils;
import io.druid.guice.annotations.Global;
import io.druid.metadata.DescExtractor;
import io.druid.query.LocatedSegmentDescriptor;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.initialization.IndexerZkConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.utils.ZKPaths;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceInstance;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.EndOfFileException;
import org.jline.reader.History;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.ParsedLine;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.DefaultParser;
import org.jline.reader.impl.completer.AggregateCompleter;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import javax.ws.rs.core.MediaType;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLEncoder;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 */
public class DruidShell extends CommonShell.WithUtils
{
  private static final Logger LOG = new Logger(DruidShell.class);
  private static final StatusResponseHandler RESPONSE_HANDLER = new StatusResponseHandler(Charsets.UTF_8);

  // make it blocking
  private static final InputStreamResponseHandler STREAM_HANDLER = new InputStreamResponseHandler()
  {
    @Override
    public ClientResponse<AppendableByteArrayInputStream> handleResponse(HttpResponse response)
    {
      return ClientResponse.unfinished(super.handleResponse(response).getObj());
    }
  };

  private final IndexerZkConfig zkPaths;
  private final CuratorFramework curator;
  private final ServiceDiscovery<String> discovery;
  private final HttpClient httpClient;
  private final ObjectMapper jsonMapper;

  @Inject
  public DruidShell(
      IndexerZkConfig zkPaths,
      CuratorFramework curator,
      ServiceDiscovery<String> discovery,
      @Global HttpClient httpClient,
      @JacksonInject ObjectMapper jsonMapper
  )
  {
    this.zkPaths = zkPaths;
    this.curator = curator;
    this.discovery = discovery;
    this.httpClient = httpClient;
    this.jsonMapper = jsonMapper;
  }

  public void run(List<String> arguments) throws Exception
  {
    final LeaderLatch coordinatorLatch = new LeaderLatch(
        curator,
        ZKPaths.makePath(zkPaths.getZkPathsConfig().getCoordinatorPath(), DruidCoordinator.COORDINATOR_OWNER_NODE)
    );
    LOG.info("Coordinator leader : %s", coordinatorLatch.getLeader());
    for (Participant participant : coordinatorLatch.getParticipants()) {
      if (!participant.isLeader()) {
        LOG.info("Coordinator participant : %s", participant);
      }
    }
    final LeaderLatch indexerLatch = new LeaderLatch(curator, zkPaths.getLeaderLatchPath());
    LOG.info("Overlord leader : %s", indexerLatch.getLeader());
    for (Participant participant : indexerLatch.getParticipants()) {
      if (!participant.isLeader()) {
        LOG.info("Overlord participant : %s", participant);
      }
    }
    Properties properties = loadNodeProperties("broker");
    String serviceName = Optional.ofNullable(properties.getProperty("druid.service")).orElse("broker");
    List<URL> brokerURLs = discover(serviceName);
    for (URL broker : brokerURLs) {
      LOG.info("Broker : %s", broker);
    }
    final URL coordinatorURL = new URL("http://" + coordinatorLatch.getLeader().getId());
    final URL overlordURL = new URL("http://" + indexerLatch.getLeader().getId());

    try (Terminal terminal = TerminalBuilder.builder().build()) {
      execute(coordinatorURL, overlordURL, brokerURLs, terminal, arguments);
    }
  }

  private List<URL> discover(final String service) throws Exception
  {
    Collection<ServiceInstance<String>> services = discovery.queryForInstances(service);
    if (services.isEmpty()) {
      services = Lists.newArrayList(
          Iterables.concat(Iterables.transform(Iterables.filter(discovery.queryForNames(), new Predicate<String>()
          {
            @Override
            public boolean apply(String input)
            {
              return input != null && input.contains(service);
            }
          }), new Function<String, Collection<ServiceInstance<String>>>()
          {
            @Override
            public Collection<ServiceInstance<String>> apply(String input)
            {
              try {
                return discovery.queryForInstances(input);
              }
              catch (Exception e) {
                return Collections.emptyList();
              }
            }
          }))
      );
    }
    return Lists.newArrayList(
        Iterables.transform(
            services,
            new Function<ServiceInstance<String>, URL>()
            {
              @Override
              public URL apply(ServiceInstance<String> input)
              {
                try {
                  return new URL("http://" + input.getAddress() + ":" + input.getPort());
                }
                catch (Exception e) {
                  throw Throwables.propagate(e);
                }
              }
            }
        )
    );
  }

  private static final String DEFAULT_PROMPT = "> ";
  private static final String SQL_PROMPT = "sql> ";

  private void execute(
      final URL coordinatorURL,
      final URL overlordURL,
      final List<URL> brokerURLs,
      final Terminal terminal,
      List<String> arguments
  )
      throws Exception
  {
    final PrintWriter writer = terminal.writer();
    if (arguments != null && !arguments.isEmpty()) {
      Cursor cursor = new Cursor(arguments);
      try {
        writer.println(DEFAULT_PROMPT + org.apache.commons.lang.StringUtils.join(arguments, " "));
        handleCommand(coordinatorURL, overlordURL, brokerURLs, writer, cursor);
      }
      finally {
        writer.flush();
      }
      return;
    }

    DefaultParser parser = new DefaultParser();

    final Function<String, Candidate> toCandidate = new Function<String, Candidate>()
    {
      public Candidate apply(String input) { return new Candidate(input); }
    };

    final List<String> commands = Arrays.asList(
        "loadstatus",
        "loadqueue",
        "servers",
        "server",
        "segments",
        "segment",
        "datasources",
        "datasource",
        "desc",
        "dynamicConf",
        "tiers",
        "tier",
        "rules",
        "rule",
        "lookups",
        "lookup",
        "tasks",
        "task",
        "query",
        "help",
        "sql",
        "quit",
        "exit"
    );
    Completer commandCompleter = new Completer()
    {
      @Override
      public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates)
      {
        if (line.wordIndex() == 0) {
          candidates.addAll(Lists.transform(commands, toCandidate));
        }
      }
    };

    final Set<String> dsRequired = ImmutableSet.of("datasource", "rule", "desc", "query");
    Completer dsCompleter = new Completer()
    {
      @Override
      public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates)
      {
        String command = line.words().get(0);
        if (line.wordIndex() == 1 && dsRequired.contains(command)) {
          candidates.addAll(
              Lists.transform(execute(coordinatorURL, "/druid/coordinator/v1/datasources", LIST), toCandidate)
          );
        }
      }
    };
    final Set<String> serverRequired = ImmutableSet.of("server", "segments", "segment");
    Completer serverCompleter = new Completer()
    {
      @Override
      public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates)
      {
        String command = line.words().get(0);
        if (line.wordIndex() == 1 && serverRequired.contains(command)) {
          candidates.addAll(
              Lists.transform(execute(coordinatorURL, "/druid/coordinator/v1/servers", LIST), toCandidate)
          );
        }
      }
    };
    Completer tierCompleter = new Completer()
    {
      @Override
      public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates)
      {
        String command = line.words().get(0);
        if (line.wordIndex() == 1 && "tier".equals(command)) {
          candidates.addAll(
              Lists.transform(execute(coordinatorURL, "/druid/coordinator/v1/tiers", LIST), toCandidate)
          );
        } else if (line.wordIndex() == 1 && "lookup".equals(command)) {
          candidates.addAll(
              Lists.transform(execute(coordinatorURL, "/druid/coordinator/v1/lookups", LIST), toCandidate)
          );
        }
      }
    };

    Completer descTypeCompleter = new Completer()
    {
      @Override
      public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates)
      {
        String command = line.words().get(0);
        if (line.wordIndex() == 2 && "desc".equals(command)) {
          candidates.addAll(
              Lists.transform(
                  Arrays.asList(DescExtractor.values()), Functions.compose(toCandidate, Functions.toStringFunction())
              )
          );
        }
      }
    };

    Completer lookupCompleter = new Completer()
    {
      @Override
      public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates)
      {
        String command = line.words().get(0);
        if (line.wordIndex() == 2 && "lookup".equals(command)) {
          candidates.addAll(
              Lists.transform(
                  execute(coordinatorURL, "/druid/coordinator/v1/lookups/" + line.words().get(1), LIST), toCandidate
              )
          );
        }
      }
    };

    Completer taskCompleter = new Completer()
    {
      @Override
      public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates)
      {
        String command = line.words().get(0);
        if (line.wordIndex() == 1 && "task".equals(command)) {
          candidates.addAll(
              Lists.transform(execute(overlordURL, "/druid/indexer/v1/tasks", LIST), toCandidate)
          );
        }
      }
    };

    final Candidate full = new Candidate("-full");
    final Candidate simple = new Candidate("-simple");
    final Candidate lastUpdated = new Candidate("-lastUpdated");
    final Set<String> fullOption1 = ImmutableSet.of("servers", "segments", "datasources", "rule", "tasks");
    final Set<String> fullOption2 = ImmutableSet.of("datasource", "rule");
    final Set<String> simpleOption1 = ImmutableSet.of("servers", "datasources", "tiers", "rules");
    final Set<String> simpleOption2 = ImmutableSet.of("server", "datasource", "tier");

    Completer optionCompleter = new Completer()
    {
      @Override
      public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates)
      {
        List<String> commands = line.words();
        String command = commands.get(0);
        if (line.wordIndex() > 0 && fullOption1.contains(command) ||
            line.wordIndex() > 1 && fullOption2.contains(command)) {
          if (!candidates.contains(full) && !commands.contains("-full") && !commands.contains("-simple")) {
            candidates.add(full);
          }
        }
        if (line.wordIndex() > 0 && simpleOption1.contains(command) ||
            line.wordIndex() > 1 && simpleOption2.contains(command)) {
          if (!candidates.contains(simple) && !commands.contains("-full") && !commands.contains("-simple")) {
            candidates.add(simple);
          }
        }
        if (line.wordIndex() > 1 && command.equals("datasource")) {
          if (!commands.contains("-intervals") && !commands.contains("-segments") &&
              !hasOption(commands, "-interval=") && !hasOption(commands, "-segment=") &&
              !hasOption(commands, "-tiers") && !hasOption(commands, "-disable")) {
            candidates.addAll(
                Lists.transform(
                    Arrays.asList("-intervals", "-interval=", "-segments", "-segment=", "-tiers", "-disable"),
                    toCandidate
                )
            );
          }
          if (!commands.contains("-lastUpdated")) {
            candidates.add(lastUpdated);
          }
        }
        if (line.wordIndex() == 1 && command.equals("datasources")) {
          candidates.add(new Candidate("-regex="));
        }
        if (line.wordIndex() == 1 && command.equals("lookups")) {
          candidates.add(new Candidate("-discover"));
        }
        if (line.wordIndex() > 0 && command.equals("tasks")) {
          if (commands.contains("-simple")) {
            candidates.add(new Candidate("-completed"));
          }
          if (commands.contains("-completed")) {
            candidates.add(new Candidate("-recent="));
          }
        }
        if (line.wordIndex() == 2 && command.equals("task")) {
          candidates.add(new Candidate("-status"));
          candidates.add(new Candidate("-segments"));
          candidates.add(new Candidate("-log"));
        }
      }
    };

    AggregateCompleter completer = new AggregateCompleter(
        commandCompleter,
        dsCompleter,
        serverCompleter,
        tierCompleter,
        descTypeCompleter,
        lookupCompleter,
        taskCompleter,
        optionCompleter
    );

    final StringBuilder builder = new StringBuilder();
    final AtomicBoolean inSQL = new AtomicBoolean();
    final History history = new DefaultHistory()
    {
      @Override
      public void add(Instant time, String line)
      {
        if (inSQL.get()) {
          if (builder.length() > 0) {
            builder.append('\n');
          }
          if (line.endsWith(";")) {
            line = builder.append(line).toString();
          } else {
            builder.append(line);
            return;
          }
        }
        super.add(time, line);
      }
    };
    final LineReader reader = LineReaderBuilder.builder()
                                         .history(history)
                                         .terminal(terminal)
                                         .completer(completer)
                                         .parser(parser)
                                         .build();

    while (true) {
      String line = readLine(reader, DEFAULT_PROMPT);
      if (line == null) {
        return;
      }
      if (line.equals("sql")) {
        Map<String, String> properties = Maps.newHashMap();
        inSQL.set(true);
        while (true) {
          String sqlPart = readLine(reader, SQL_PROMPT);
          if (sqlPart == null) {
            return;
          }
          if (sqlPart.endsWith(";")) {
            String SQL = builder.toString();
            if (SQL.length() == 1) {
              break;
            }
            String sqlString = SQL.substring(0, SQL.length() - 1).trim();
            if (sqlString.startsWith("?")) {
              // regard it's command
              if (sqlString.startsWith("?set")) {
                if (sqlString.length() == 4) {
                  for (Map.Entry<String, String> entry : properties.entrySet()) {
                    writer.print("     ");
                    writer.println(entry);
                  }
                } else {
                  final String[] property = sqlString.substring(4).trim().split("=");
                  final String key = property[0].trim();
                  if (property.length == 1) {
                    writer.println(String.format("     %s = %s", key, properties.get(key)));
                  } else if (property.length == 2) {
                    final String value = property[1].trim();
                    if (value.isEmpty()) {
                      properties.remove(key);
                    } else {
                      properties.put(key, value);
                      writer.println(String.format("     %s = %s", key, value));
                    }
                  } else {
                    writer.println("     ??");
                  }
                }
              }
            } else {
              runSQL(brokerURLs, writer, sqlString, properties);
            }
            builder.setLength(0);
          }
        }
        builder.setLength(0);
        inSQL.set(false);
        continue;
      }
      if (line.equalsIgnoreCase("quit") || line.equalsIgnoreCase("exit")) {
        break;
      }
      Cursor cursor = new Cursor(reader.getParser().parse(line, 0).words());
      try {
        handleCommand(coordinatorURL, overlordURL, brokerURLs, writer, cursor);
      }
      catch (ISE e) {
        LOG.info(e.toString());
      }
      catch (Exception e) {
        LOG.info(e, "Failed..");
      }
      writer.flush();
    }
  }

  private String readLine(LineReader reader, String prompt)
  {
    while (true) {
      String line = null;
      try {
        line = reader.readLine(prompt);
      }
      catch (UserInterruptException e) {
        // Ignore
      }
      catch (EndOfFileException e) {
        return null;
      }
      if (line == null) {
        continue;
      }
      line = line.trim();
      if (!line.isEmpty()) {
        return line;
      }
    }
  }

  private boolean hasOption(List<String> commands, String option) {
    for (String command : commands) {
      if (command.startsWith(option)) {
        return true;
      }
    }
    return false;
  }

  private static final String[] PREFIX = new String[]{"  >> ", "    >> ", "      >> "};

  private void handleCommand(
      URL coordinatorURL,
      URL overlordURL,
      List<URL> brokerURLs,
      PrintWriter writer,
      Cursor cursor
  )
      throws Exception
  {
    Resource resource = new Resource();
    switch (cursor.command()) {
      case "help":
        writer.println("loadstatus");
        writer.println("loadqueue");
        writer.println("servers [-full|-simple]");
        writer.println("server <server-name> [-simple]");
        writer.println("segments <server-name> [-full]");
        writer.println("segment <server-name> <segment-id>");
        writer.println("datasources [-regex=<name-regex>] [-full|-simple]");
        writer.println("datasource <datasource-name> [-full|-simple|-lastUpdated]");
        writer.println("datasource <datasource-name> -intervals|-interval=<interval> [-full|-simple]");
        writer.println("datasource <datasource-name> -segments|-segment=<segment> [-full]");
        writer.println("datasource <datasource-name> -disable -interval=<interval>|-segment=<segment>");
        writer.println("datasource <datasource-name> -tiers");
        writer.println("desc <datasource-name> DS_COMMENT");
        writer.println("desc <datasource-name> DS_PROPS");
        writer.println("desc <datasource-name> COLUMNS_COMMENTS");
        writer.println("desc <datasource-name> COLUMN_PROPS <column-name>");
        writer.println("desc <datasource-name> VALUES_COMMENTS <value>");
        writer.println("dynamicConf");
        writer.println("tiers [-simple]");
        writer.println("tier <tier-name> [-simple]");
        writer.println("rules [-simple]");
        writer.println("rule <datasource-name> [-full]");
        writer.println("lookups [-discover]");
        writer.println("lookup <tier-name> [lookup-name]");
        writer.println("tasks [-simple] [-completed [-recent=<duration>]]");
        writer.println("task <task-id> [-status|-segments|-log]");
        writer.println("query <datasource-name> <intervals> [-full]");
        writer.println("sql <sql-string>");
        return;
      case "loadstatus":
        Map<String, Object> loadStatus = execute(coordinatorURL, "/druid/coordinator/v1/loadstatus", MAP);
        for (Map.Entry<String, Object> entry : loadStatus.entrySet()) {
          writer.println(entry.toString());
        }
        break;
      case "loadqueue":
        Map<String, Map<String, Object>> loadqueue = execute(
            coordinatorURL, "/druid/coordinator/v1/loadqueue", MAP_MAP
        );
        Map<String, Object> segmentsToLoad = loadqueue.get("segmentsToLoad");
        writer.println("segmentsToLoad " + (segmentsToLoad == null ? 0 : segmentsToLoad.size()));
        if (segmentsToLoad != null) {
          for (Map.Entry<String, Object> entry : segmentsToLoad.entrySet()) {
            writer.println(PREFIX[0] + entry.toString());
          }
        }
        Map<String, Object> segmentsToDrop = loadqueue.get("segmentsToDrop");
        writer.println("segmentsToDrop " + (segmentsToDrop == null ? 0 : segmentsToDrop.size()));
        if (segmentsToDrop != null) {
          for (Map.Entry<String, Object> entry : segmentsToDrop.entrySet()) {
            writer.println(PREFIX[0] + entry.toString());
          }
        }
        break;
      case "servers":
        resource.append("/druid/coordinator/v1/servers");
        if (cursor.hasMore()) {
          resource.appendOption(cursor.next());
          for (Map<String, Object> server : execute(coordinatorURL, resource.get(), LIST_MAP)) {
            writer.println(PREFIX[0] + server);
          }
        } else {
          writer.println(PREFIX[0] + execute(coordinatorURL, resource.get(), LIST));
        }
        break;
      case "server":
        resource.append("/druid/coordinator/v1/servers");
        if (!cursor.hasMore()) {
          writer.println("needs server name");
          return;
        }
        resource.append(cursor.next());
        if (cursor.hasMore() && cursor.next().equals("-simple")) {
          resource.appendOption(cursor.current());
        }
        writer.println(PREFIX[0] + execute(coordinatorURL, resource.get(), MAP));
        break;
      case "segments":
        if (!cursor.hasMore()) {
          writer.println("needs server name");
          return;
        }
        resource.append("/druid/coordinator/v1/servers").append(cursor.next());
        if (cursor.hasMore() && cursor.next().equals("-full")) {
          resource.appendOption(cursor.current());
          for (Map<String, Object> segment : execute(coordinatorURL, resource.get(), LIST_MAP)) {
            writer.println(PREFIX[0] + segment);
          }
        } else {
          writer.println(execute(coordinatorURL, resource.get(), LIST));
        }
        break;
      case "segment":
        resource.append("/druid/coordinator/v1/servers");
        if (!cursor.hasMore(2)) {
          writer.println("needs server name & segment name");
          return;
        }

        resource.append(cursor.next()).append(cursor.next());
        writer.println(PREFIX[0] + execute(coordinatorURL, resource.get(), MAP));
        break;
      case "datasources":
        resource.append("/druid/coordinator/v1/datasources");
        String option = null;
        String regex = null;
        while (cursor.hasMore()) {
          String param = cursor.next();
          if (param.equals("-full") || param.equals("-simple")) {
            option = param;
          } else if (param.startsWith("-regex=")) {
            regex = param.substring(7).trim();
          }
        }
        if (option != null) {
          resource.appendOption(option);
        }
        if (regex != null) {
          resource.appendOption("nameRegex=" + regex);
        }
        if (option != null) {
          for (Map<String, Object> segment : execute(coordinatorURL, resource.get(), LIST_MAP)) {
            writer.println(PREFIX[0] + segment);
          }
        } else {
          writer.println(PREFIX[0] + execute(coordinatorURL, resource.get(), LIST));
        }
        break;
      case "datasource": {
        resource.append("/druid/coordinator/v1/datasources");
        if (!cursor.hasMore()) {
          writer.println("needs datasource name");
          return;
        }
        resource.append(cursor.next());

        boolean lastUpdated = false;
        boolean full = false;
        boolean simple = false;
        boolean intervals = false;
        boolean segments = false;
        boolean tiers = false;
        boolean disable = false;
        String interval = null;
        String segment = null;
        while (cursor.hasMore()) {
          String current = cursor.next();
          if (current.equals("-full")) {
            full = true;
          } else if (current.equals("-simple")) {
            simple = true;
          } else if (current.equals("-lastUpdated")) {
            lastUpdated = true;
          } else if (current.equals("-intervals")) {
            intervals = true;
          } else if (current.equals("-segments")) {
            segments = true;
          } else if (current.equals("tiers")) {
            tiers = true;
          } else if (current.startsWith("-interval=")) {
            interval = current.substring(10).trim();
          } else if (current.startsWith("-segment=")) {
            segment = current.substring(9).trim();
          } else if (current.startsWith("-disable")) {
            disable = true;
          }
        }
        if ((intervals || interval != null) && (segments || segment != null)) {
          writer.println("interval(s) or segment(s), just pick one");
          return;
        }
        if (disable) {
          if (intervals || segments) {
            writer.println("disable does not take -intervals or -segments");
            return;
          }
          if (interval != null) {
            resource.append("interval/disable/").append(interval.replace("/", "_"));
          } else {
            resource.append("segment/disable/").append(segment);
          }
          writer.println(PREFIX[0] + execute(coordinatorURL, resource.get(), INT));
        } else if (tiers) {
          writer.println(PREFIX[0] + execute(coordinatorURL, resource.append("tiers").get(), LIST));
        } else if (!intervals && interval == null && !segments && segment == null) {
          if (full) {
            resource.appendOption("-full");
          } else if (simple) {
            resource.appendOption("-simple");
          } else if (lastUpdated) {
            resource.appendOption("-lastUpdated");
          }
          writer.println(PREFIX[0] + execute(coordinatorURL, resource.get(), MAP));
        } else if (intervals || interval != null) {
          resource.append("intervals");
          if (interval != null) {
            resource.append(URLEncoder.encode(interval, StringUtils.UTF8_STRING));
          }
          if (full) {
            resource.appendOption("-full");
          } else if (simple) {
            resource.appendOption("-simple");
          }
          if (full || simple) {
            for (Map.Entry<String, Map<String, Object>> entry :
                execute(coordinatorURL, resource.get(), MAP_MAP).entrySet()) {
              writer.println(PREFIX[0] + "interval: " + entry.getKey());
              for (Map.Entry<String, Object> values : entry.getValue().entrySet()) {
                writer.println(PREFIX[1] + values);
              }
              writer.println(PREFIX[0] + entry);
            }
          } else {
            writer.println(PREFIX[0] + execute(coordinatorURL, resource.get(), LIST));
          }
        } else {
          resource.append("segments");
          if (segment != null) {
            resource.append(URLEncoder.encode(segment, StringUtils.UTF8_STRING));
            writer.println(PREFIX[0] + execute(coordinatorURL, resource.get(), MAP));
            return;
          }
          if (full) {
            resource.appendOption("-full");
            for (Map<String, Object> value : execute(coordinatorURL, resource.get(), LIST_MAP)) {
              writer.println(PREFIX[0] + value);
            }
          }
          writer.println(PREFIX[0] + execute(coordinatorURL, resource.get(), LIST));
        }
        break;
      }
      case "desc":
        resource.append("/druid/coordinator/v1/datasources");
        if (!cursor.hasMore()) {
          writer.println("needs datasource name");
          return;
        }
        resource.append(cursor.next()).append("desc");
        if (!cursor.hasMore()) {
          writer.println("needs desc type, ont of " + Arrays.toString(DescExtractor.values()));
          return;
        }
        DescExtractor extractor = getDescExtractor(cursor);
        if (extractor == null) {
          writer.println("invalid desc type " + cursor.current());
          return;
        }
        resource.append(cursor.current());
        switch (extractor) {
          case DS_COMMENT:
            writer.println(PREFIX[0] + execute(coordinatorURL, resource.get(), STRING));
            return;
          case DS_PROPS:
          case COLUMNS_COMMENTS:
            writer.println(PREFIX[0] + execute(coordinatorURL, resource.get(), MAP));
            return;
          case COLUMN_PROPS:
            if (!cursor.hasMore()) {
              writer.println("column value is missing");
              return;
            }
            resource.appendOption("column=" + cursor.next());
            writer.println(PREFIX[0] + execute(coordinatorURL, resource.get(), MAP));
            return;
          case VALUES_COMMENTS:
            Map<String, Map<String, Object>> valuesComments = execute(coordinatorURL, resource.get(), MAP_MAP);
            for (Map.Entry<String, Map<String, Object>> entry : valuesComments.entrySet()) {
              writer.println(PREFIX[0] + "column: " + entry.getKey());
              for (Map.Entry<String, Object> comments : entry.getValue().entrySet()) {
                writer.println(PREFIX[1] + comments);
              }
            }
            break;
        }
      case "dynamicConf":
        writer.println(PREFIX[0] + execute(coordinatorURL, "/druid/coordinator/v1/config", MAP));
        return;
      case "tiers":
      case "tier":
        resource.append("/druid/coordinator/v1/tiers");
        if (cursor.command().equals("tier")) {
          if (cursor.hasMore()) {
            resource.append(cursor.next());
          } else {
            writer.println("needs tier name");
            return;
          }
        }
        if (cursor.hasMore() && cursor.next().equals("-simple")) {
          resource.appendOption(cursor.current());
          for (Map.Entry<String, Map<String, Object>> entry :
              execute(coordinatorURL, resource.get(), MAP_MAP).entrySet()) {
            writer.println(PREFIX[0] + entry.getKey());
            for (Map.Entry<String, Object> value : entry.getValue().entrySet()) {
              writer.println(PREFIX[1] + value);
            }
          }
        } else {
          writer.println(PREFIX[0] + execute(coordinatorURL, resource.get(), LIST));
        }
        return;
      case "rules":
        resource.append("/druid/coordinator/v1/rules");
        for (Map.Entry<String, List<Map<String, Object>>> entry :
            execute(coordinatorURL, resource.get(), MAP_LIST_MAP).entrySet()) {
          writer.println(PREFIX[0] + "dataSource = " + entry.getKey());
          for (Map<String, Object> value : entry.getValue()) {
            writer.println(PREFIX[1] + value);
          }
        }
        return;
      case "rule":
        resource.append("/druid/coordinator/v1/rules");
        if (!cursor.hasMore()) {
          writer.println("needs datasource name");
          return;
        }
        resource.append(cursor.next());
        if (cursor.hasMore() && cursor.next().equals("-full")) {
          resource.appendOption(cursor.current());
        }
        writer.println(PREFIX[0] + execute(coordinatorURL, resource.get(), LIST_MAP));
        return;
      case "lookups":
        resource.append("/druid/coordinator/v1/lookups");
        if (cursor.hasMore() && cursor.next().equals("-discover")) {
          resource.appendOption(cursor.current());
        }
        writer.println(PREFIX[0] + execute(coordinatorURL, resource.get(), LIST));
        return;
      case "lookup":
        resource.append("/druid/coordinator/v1/lookups");
        if (!cursor.hasMore()) {
          writer.println("needs tier name");
          return;
        }
        resource.append(cursor.next());
        if (cursor.hasMore()) {
          resource.append(cursor.next());
          writer.println(PREFIX[0] + execute(coordinatorURL, resource.get(), MAP));
        } else {
          writer.println(PREFIX[0] + execute(coordinatorURL, resource.get(), LIST));
        }
        return;
      case "tasks":
        resource.append("/druid/indexer/v1/tasks");
        boolean hasSimple = false;
        boolean hasCompleted = false;
        String recentOption = null;
        while (cursor.hasMore()) {
          String current = cursor.next();
          if (current.equals("-simple")) {
            hasSimple = true;
          } else if (current.equals("-completed")) {
            hasCompleted = true;
          } else if (current.startsWith("-recent=")) {
            recentOption = current.substring(8);
          }
        }
        if (hasSimple) {
          resource.appendOption("simple");
        }
        if (hasCompleted || recentOption != null) {
          resource.appendOption("state=complete");
        }
        if (recentOption != null) {
          resource.appendOption("period=" + recentOption);
        }

        if (hasSimple) {
          writer.println(PREFIX[0] + execute(overlordURL, resource.get(), LIST));
        } else {
          resource.appendOption(cursor.current());
          for (Map<String, Object> value : execute(overlordURL, resource.get(), LIST_MAP)) {
            writer.println(PREFIX[0] + value);
          }
        }
        return;
      case "task":
        resource.append("/druid/indexer/v1/task");
        if (!cursor.hasMore()) {
          writer.println("needs task id");
          return;
        }
        resource.append(cursor.next());
        if (cursor.hasMore()) {
          String next = cursor.next();
          switch (next) {
            case "-status":
              resource.append("status");
              break;
            case "-segments":
              resource.append("segments");
              for (Map<String, Object> value : execute(overlordURL, resource.get(), LIST_MAP)) {
                writer.println(PREFIX[0] + value);
              }
              return;
            case "-log":
              resource.append("log");
              InputStream stream = stream(overlordURL, resource.get());
              writer.println(PREFIX[0] + "............... dump");
              BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
              for (String line; (line = reader.readLine()) != null;) {
                writer.println(line);
              }
              return;
          }
        }
        writer.println(PREFIX[0] + execute(overlordURL, resource.get(), MAP));
        break;
      case "query": {
        if (brokerURLs.isEmpty()) {
          writer.println("no broker..");
          return;
        }
        if (!cursor.hasMore()) {
          writer.println("needs datasource & comma separated intervals");
          return;
        }
        String dataSource = cursor.next();

        resource.append("/druid/v2/candidates");
        resource.appendOption("datasource=" + dataSource);
        if (!cursor.hasMore()) {
          writer.println("comma separated intervals");
          return;
        }
        String intervals = cursor.next();
        boolean full = cursor.hasMore() && cursor.next().equals("-full");

        resource.appendOption("intervals=" + URLEncoder.encode(intervals, StringUtils.UTF8_STRING));
        for (LocatedSegmentDescriptor located : execute(
            brokerURLs.get(0),
            resource.get(),
            new TypeReference<List<LocatedSegmentDescriptor>>()
            {
            }
        )) {
          if (full) {
            writer.println(PREFIX[0] + located);
          } else {
            StringBuilder b = new StringBuilder();
            b.append(located.getInterval()).append('[').append(located.getPartitionNumber()).append(']').append(' ');
            b.append('(').append(located.getVersion()).append(')');
            b.append(" = ");
            b.append(
                String.join(
                    ", ",
                    Lists.transform(
                        located.getLocations(), new Function<DruidServerMetadata, String>()
                        {
                          @Override
                          public String apply(DruidServerMetadata input)
                          {
                            return input.getName();
                          }
                        }
                    )
                )
            );
            writer.println(PREFIX[0] + b.toString());
          }
        }
        break;
      }
      case "sql": {
        if (brokerURLs.isEmpty()) {
          writer.println("no broker..");
          return;
        }
        if (!cursor.hasMore()) {
          writer.println("needs sql string");
          return;
        }
        runSQL(brokerURLs, writer, cursor.next(), null);
        break;
      }
      default:
        writer.println(PREFIX[0] + "invalid command " + cursor.command());
    }
  }

  private void runSQL(List<URL> brokerURLs, PrintWriter writer, String sql, Map<String, String> context)
  {
    brokerURLs = ensureBroker(brokerURLs);
    if (brokerURLs.isEmpty()) {
      writer.println("!! cannot find broker");
      return;
    }
    int numRow = 0;
    long start = System.currentTimeMillis();
    try {
      String mediaType = MediaType.TEXT_PLAIN;
      if (!GuavaUtils.isNullOrEmpty(context)) {
        // SqlQuery
        sql = jsonMapper.writeValueAsString(ImmutableMap.of("query", sql, "context", context));
        mediaType = MediaType.APPLICATION_JSON;
      }
      boolean header = false;
      for (Map<String, Object> row : execute(
          HttpMethod.POST,
          brokerURLs.get(0),
          "/druid/v2/sql",
          mediaType,
          sql.getBytes(),
          new TypeReference<List<Map<String, Object>>>() {}
      )) {
        if (!header) {
          writer.print("  ");
          String columns = row.keySet().toString();
          writer.println(columns);
          writer.print("  ");
          for (int i = 0; i < columns.length(); i++) {
            writer.print('-');
          }
          writer.println();
          header = true;
        }
        writer.print("  ");
        writer.println(row.values().toString());
        numRow++;
      }
      writer.println(String.format("> Retrieved %d rows in %,d msec", numRow, (System.currentTimeMillis() - start)));
    }
    catch (Exception e) {
      writer.println(String.format("> Failed by exception : %s", e));
    }
  }

  private List<URL> ensureBroker(List<URL> brokerURLs)
  {
    if (brokerURLs.isEmpty()) {
      try {
        return discover("broker");
      }
      catch (Exception e) {
        // ignore
      }
    }
    return brokerURLs;
  }

  private DescExtractor getDescExtractor(Cursor cursor)
  {
    try {
      return DescExtractor.valueOf(cursor.next());
    }
    catch (IllegalArgumentException e) {
      return null;
    }
  }

  private static class Resource
  {
    boolean hasOption;
    StringBuilder resource = new StringBuilder();

    Resource append(String path)
    {
      if (!path.startsWith("/")) {
        resource.append('/');
      }
      resource.append(path);
      return this;
    }

    Resource appendOption(String option)
    {
      option = option.startsWith("-") ? option.substring(1) : option;
      if (hasOption) {
        resource.append('&').append(option);
      } else {
        resource.append('?').append(option);
        hasOption = true;
      }
      return this;
    }

    String get()
    {
      return resource.toString();
    }
  }

  private static class Cursor
  {
    private final List<String> words;
    private int cursor;

    private Cursor(List<String> words) {this.words = words;}

    String command()
    {
      return words.get(0);
    }

    String current()
    {
      return words.get(cursor);
    }

    boolean hasMore()
    {
      return hasMore(1);
    }

    boolean hasMore(int expected)
    {
      return cursor + expected < words.size();
    }

    String next()
    {
      return words.get(++cursor);
    }
  }

  private static final TypeReference<Integer> INT = new TypeReference<Integer>()
  {
  };

  private static final TypeReference<String> STRING = new TypeReference<String>()
  {
  };

  private static final TypeReference<List<String>> LIST = new TypeReference<List<String>>()
  {
  };

  private static final TypeReference<Map<String, Object>> MAP = new TypeReference<Map<String, Object>>()
  {
  };

  private static final TypeReference<List<Map<String, Object>>> LIST_MAP = new TypeReference<List<Map<String, Object>>>()
  {
  };

  private static final TypeReference<Map<String, Map<String, Object>>> MAP_MAP = new TypeReference<Map<String, Map<String, Object>>>()
  {
  };

  private static final TypeReference<Map<String, List<Map<String, Object>>>> MAP_LIST_MAP = new TypeReference<Map<String, List<Map<String, Object>>>>()
  {
  };

  private <T> T execute(URL baseURL, String resource, TypeReference<T> resultType)
  {
    return execute(HttpMethod.GET, baseURL, resource, null, null, resultType);
  }

  private <T> T execute(
      HttpMethod method,
      URL baseURL,
      String resource,
      String contentType,
      byte[] content,
      TypeReference<T> resultType
  )
  {
    try {
      URL requestURL = new URL(baseURL + resource);
      Request request = new Request(method, requestURL);
      if (contentType != null && content != null) {
        request.setContent(contentType, content);
      }
      StatusResponseHolder response = httpClient.go(request, RESPONSE_HANDLER).get();
      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE(
            "Error while fetching from [%s], status[%s] content[%s]",
            requestURL,
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

  private InputStream stream(URL baseURL, String resource)
  {
    return stream(HttpMethod.GET, baseURL, resource);
  }

  private InputStream stream(HttpMethod method, URL baseURL, String resource)
  {
    try {
      URL requestURL = new URL(baseURL + resource);
      Request request = new Request(method, requestURL);
      return httpClient.go(request, STREAM_HANDLER).get();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
