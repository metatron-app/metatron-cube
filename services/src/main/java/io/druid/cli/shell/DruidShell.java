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
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.common.utils.StringUtils;
import io.druid.guice.annotations.Global;
import io.druid.metadata.DescExtractor;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.initialization.ZkPathsConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.utils.ZKPaths;
import org.jboss.netty.handler.codec.http.HttpMethod;
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

import java.io.PrintWriter;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class DruidShell
{
  private static final Logger LOG = new Logger(DruidShell.class);
  private static final StatusResponseHandler RESPONSE_HANDLER = new StatusResponseHandler(Charsets.UTF_8);

  private final ZkPathsConfig zkPaths;
  private final CuratorFramework curator;
  private final HttpClient httpClient;
  private final ObjectMapper jsonMapper;

  @Inject
  public DruidShell(
      ZkPathsConfig zkPaths,
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

  public void run() throws Exception
  {
    final LeaderLatch leaderLatch = new LeaderLatch(
        curator, ZKPaths.makePath(zkPaths.getCoordinatorPath(), DruidCoordinator.COORDINATOR_OWNER_NODE), "dummy"
    );
    LOG.info("Leader : %s", leaderLatch.getLeader());
    for (Participant participant : leaderLatch.getParticipants()) {
      if (!participant.isLeader()) {
        LOG.info("Participant : %s", participant);
      }
    }

    final URL coordinatorURL = new URL("http://" + leaderLatch.getLeader().getId());

    TerminalBuilder builder = TerminalBuilder.builder();
    Terminal terminal = builder.build();

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

    final Candidate full = new Candidate("-full");
    final Candidate simple = new Candidate("-simple");
    final Set<String> fullOption1 = ImmutableSet.of("servers", "segments", "datasources", "rule");
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
        }
        if (line.wordIndex() == 1 && command.equals("datasources")) {
          candidates.add(new Candidate("-regex="));
        }
        if (line.wordIndex() == 1 && command.equals("lookups")) {
          candidates.add(new Candidate("-discover"));
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
        optionCompleter
    );
    LineReader reader = LineReaderBuilder.builder()
                                         .terminal(terminal)
                                         .completer(completer)
                                         .parser(parser)
                                         .build();

    final String prompt = "> ";

    PrintWriter writer = terminal.writer();
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
        handleCommand(coordinatorURL, writer, cursor);
      }
      catch (ISE e) {
        LOG.info(e.toString());
      }
      catch (Exception e) {
        LOG.info(e, "Failed..");
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

  private void handleCommand(URL coordinatorURL, PrintWriter writer, Cursor cursor)
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
        writer.println("datasource <datasource-name> [-full|-simple]");
        writer.println("datasource <datasource-name> -intervals|-interval=<interval> [-full|-simple]");
        writer.println("datasource <datasource-name> -segments|-segment=<segment> [-full]");
        writer.println("datasource <datasource-name> -tiers");
        writer.println("desc <datasource-name> DS_COMMENT");
        writer.println("desc <datasource-name> DS_PROPS");
        writer.println("desc <datasource-name> COLUMNS_COMMENTS");
        writer.println("desc <datasource-name> COLUMN_PROPS <column-name>");
        writer.println("desc <datasource-name> VALUES_COMMENTS");
        writer.println("dynamicConf");
        writer.println("tiers [-simple]");
        writer.println("tier <tier-name> [-simple]");
        writer.println("rules [-simple]");
        writer.println("rule <datasource-name> [-full]");
        writer.println("lookups [-discover]");
        writer.println("lookup <tier-name> [lookup-name]");
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
          } else if (current.equals("-intervals")) {
            intervals = true;
          } else if (current.equals("-segments")) {
            segments = true;
          } else if (current.equals("tiers")) {
            tiers = true;
          } else if (current.startsWith("interval=")) {
            interval = current.substring(9).trim();
          } else if (current.startsWith("segment=")) {
            segment = current.substring(8).trim();
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
        String columnOption = null;
        String valueOption = null;
        while (cursor.hasMore()) {
          String current = cursor.next();
          if (current.startsWith("column=")) {
            columnOption = current;
          } else if (current.startsWith("value=")) {
            valueOption = current;
          }
        }
        switch (extractor) {
          case DS_COMMENT:
            writer.println(PREFIX[0] + execute(coordinatorURL, resource.get(), STRING));
            return;
          case DS_PROPS:
          case COLUMNS_COMMENTS:
            writer.println(PREFIX[0] + execute(coordinatorURL, resource.get(), MAP));
            return;
          case COLUMN_PROPS:
            if (columnOption == null) {
              writer.println("column value is missing");
              return;
            }
            resource.appendOption(columnOption);
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
}
