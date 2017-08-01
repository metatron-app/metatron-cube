package io.druid.cli.shell;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
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
import io.druid.common.utils.StringUtils;
import io.druid.guice.annotations.Global;
import io.druid.metadata.DescExtractor;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.initialization.IndexerZkConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.utils.ZKPaths;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.ParsedLine;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.DefaultParser;
import org.jline.reader.impl.completer.AggregateCompleter;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class DruidShell implements CommonShell
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
  private final HttpClient httpClient;
  private final ObjectMapper jsonMapper;

  @Inject
  public DruidShell(
      IndexerZkConfig zkPaths,
      CuratorFramework curator,
      @Global HttpClient httpClient,
      @JacksonInject ObjectMapper jsonMapper
  )
  {
    this.zkPaths = zkPaths;
    this.curator = curator;
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

    final URL coordinatorURL = new URL("http://" + coordinatorLatch.getLeader().getId());
    final URL overlordURL = new URL("http://" + indexerLatch.getLeader().getId());

    try (Terminal terminal = TerminalBuilder.builder().build()) {
      execute(coordinatorURL, overlordURL, terminal, arguments);
    }
  }

  private void execute(final URL coordinatorURL, final URL overlordURL, Terminal terminal, List<String> arguments)
      throws Exception
  {
    final String prompt = "> ";
    final PrintWriter writer = terminal.writer();
    if (arguments != null && !arguments.isEmpty()) {
      Cursor cursor = new Cursor(arguments);
      try {
        writer.println(prompt + org.apache.commons.lang.StringUtils.join(arguments, " "));
        handleCommand(coordinatorURL, overlordURL, writer, cursor);
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
        "help",
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

    final Set<String> dsRequired = ImmutableSet.of("datasource", "rule", "desc");
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
              !hasOption(commands, "-tiers")) {
            candidates.addAll(
                Lists.transform(
                    Arrays.asList("-intervals", "-interval=", "-segments", "-segment=", "-tiers"),
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
          if (!commands.contains("-full")) {
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
    LineReader reader = LineReaderBuilder.builder()
                                         .terminal(terminal)
                                         .completer(completer)
                                         .parser(parser)
                                         .build();

    while (true) {
      String line = null;
      try {
        line = reader.readLine(prompt);
      }
      catch (UserInterruptException e) {
        // Ignore
      }
      catch (EndOfFileException e) {
        return;
      }
      if (line == null) {
        continue;
      }
      line = line.trim();
      if (line.isEmpty()) {
        continue;
      }
      if (line.equalsIgnoreCase("quit") || line.equalsIgnoreCase("exit")) {
        break;
      }
      Cursor cursor = new Cursor(reader.getParser().parse(line, 0).words());
      try {
        handleCommand(coordinatorURL, overlordURL, writer, cursor);
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

  private boolean hasOption(List<String> commands, String option) {
    for (String command : commands) {
      if (command.startsWith(option)) {
        return true;
      }
    }
    return false;
  }

  private static final String[] PREFIX = new String[]{"  >> ", "    >> ", "      >> "};

  private void handleCommand(URL coordinatorURL, URL overlordURL, PrintWriter writer, Cursor cursor)
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
        writer.println("tasks [-full] [-completed [-recent=<duration>]]");
        writer.println("task <task-id> [-status|-segments|-log]");
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
      case "datasource":
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
          }
        }
        if ((intervals || interval != null) && (segments || segment != null)) {
          writer.println("interval(s) or segment(s), just pick one");
          return;
        }
        if (tiers) {
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
        boolean hasFull = false;
        boolean hasCompleted = false;
        String recentOption = null;
        while (cursor.hasMore()) {
          String current = cursor.next();
          if (current.equals("-full")) {
            hasFull = true;
          } else if (current.equals("-completed")) {
            hasCompleted = true;
          } else if (current.startsWith("-recent=")) {
            recentOption = current;
          }
        }
        if (hasFull) {
          resource.appendOption("full");
        }
        if (hasCompleted || recentOption != null) {
          resource.appendOption("completed");
        }
        if (recentOption != null) {
          resource.appendOption(recentOption);
        }
        if (hasFull) {
          resource.appendOption(cursor.current());
          for (Map<String, Object> value : execute(overlordURL, resource.get(), LIST_MAP)) {
            writer.println(PREFIX[0] + value);
          }
        } else {
          writer.println(PREFIX[0] + execute(overlordURL, resource.get(), LIST));
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
        return;
      default:
        writer.println(PREFIX[0] + "invalid command " + cursor.command());
    }
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
    return execute(HttpMethod.GET, baseURL, resource, resultType);
  }

  private <T> T execute(HttpMethod method, URL baseURL, String resource, TypeReference<T> resultType)
  {
    try {
      URL requestURL = new URL(baseURL + resource);
      Request request = new Request(method, requestURL);
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
