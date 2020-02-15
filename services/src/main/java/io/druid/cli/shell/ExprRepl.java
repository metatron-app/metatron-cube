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

import com.google.common.collect.Maps;
import io.druid.data.Pair;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.Parser;
import org.jline.reader.History;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.impl.DefaultParser;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import java.io.PrintWriter;
import java.time.Instant;
import java.util.List;
import java.util.Map;

public class ExprRepl extends CommonShell.WithUtils
{
  private static final String DEFAULT_PROMPT = "expr> ";

  @Override
  public void run(List<String> arguments) throws Exception
  {
    try (Terminal terminal = TerminalBuilder.builder().build()) {
      execute(terminal);
    }
  }

  private void execute(final Terminal terminal) throws Exception
  {
    final StringBuilder builder = new StringBuilder();
    final History history = new DefaultHistory()
    {
      @Override
      public void add(Instant time, String line)
      {
        if (builder.length() > 0) {
          builder.append('\n');
        }
        if (line.endsWith(";") || (builder.length() == 0 && line.startsWith("?"))) {
          line = builder.append(line).toString();
          if (line.length() == 1) {
            return;   // skip quit
          }
        } else {
          builder.append(line);
          return;
        }
        super.add(time, line);
      }
    };
    final PrintWriter writer = terminal.writer();
    final LineReader reader = LineReaderBuilder.builder()
                                               .history(history)
                                               .terminal(terminal)
                                               .parser(new DefaultParser())
                                               .build();

    final Map<String, Object> params = Maps.newHashMap();
    final Expr.NumericBinding binding = Parser.withMap(params);
    while (true) {
      String line = readLine(reader, DEFAULT_PROMPT);
      if (line == null) {
        break;
      }
      if (line.endsWith(";")) {
        String string = builder.toString();
        if (string.length() == 1) {
          break;
        }
        String expression = string.substring(0, string.length() - 1).trim();
        try {
          Expr expr = Parser.parse(expression);
          if (Evals.isAssign(expr)) {
            Pair<Expr, Expr> assign = Evals.splitAssign(expr);
            String key = Evals.toAssigneeEval(assign.lhs).asString();
            ExprEval value = Evals.eval(assign.rhs, binding);
            Object prev = params.put(key, value);
            writer.println(String.format("%s = %s%s", key, value, prev == null ? "" : String.format(" (%s)", prev)));
          } else {
            ExprEval eval = expr.eval(binding);
            writer.println(String.format("%s : %s", eval.type(), eval.value()));
          }
        }
        catch (Exception e) {
          writer.println(String.format("!! failed %s by %s", expression, e));
        }
        builder.setLength(0);
      }
    }
  }
}
