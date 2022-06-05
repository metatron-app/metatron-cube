/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.druid.segment.lucene;

import com.google.common.base.Throwables;
import com.google.common.primitives.Ints;
import io.druid.collections.IntList;
import io.druid.java.util.common.UOE;
import io.druid.java.util.common.logger.Logger;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.RegExp;
import org.apache.lucene.util.automaton.Transition;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Util;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.ToIntFunction;


/**
 * copied from org.apache.pinot.segment.local.utils.fst.RegexpMatcher (Apache Pinot)
 *
 * RegexpMatcher is a helper to retrieve matching values for a given regexp query.
 * Regexp query is converted into an automaton, and we run the matching algorithm on FST.
 *
 * Two main functions of this class are
 *   regexMatchOnFST() Function runs matching on FST (See function comments for more details)
 *   match(input) Function builds the automaton and matches given input.
 */
public class AutomatonMatcher
{
  private static final Logger LOG = new Logger(AutomatonMatcher.class);

  // damn lucene..
  private static final ToIntFunction<FST.Arc> LABEL = GET_LABEL();
  private static final Function<FST.Arc<Long>, Long> OUTPUT = GET_OUTPUT();

  @SuppressWarnings("JavaReflectionMemberAccess")
  private static ToIntFunction<FST.Arc> GET_LABEL()
  {
    try {
      Method accessor = FST.Arc.class.getMethod("label");
      return fst -> {
        try {
          return (int) accessor.invoke(fst);
        }
        catch (Throwable e) {
          throw Throwables.propagate(e);
        }
      };
    }
    catch (Throwable e) {
    }
    try {
      Field field = FST.Arc.class.getField("label");
      return fst -> {
        try {
          return field.getInt(fst);
        }
        catch (Throwable e) {
          throw Throwables.propagate(e);
        }
      };
    }
    catch (Throwable e) {
    }
    return fst -> {
      throw new UOE("label?");
    };
  }

  @SuppressWarnings("JavaReflectionMemberAccess")
  private static Function<FST.Arc<Long>, Long> GET_OUTPUT()
  {
    try {
      Method accessor = FST.Arc.class.getMethod("output");
      return fst -> {
        try {
          return (Long) accessor.invoke(fst);
        }
        catch (Throwable e) {
          throw Throwables.propagate(e);
        }
      };
    }
    catch (Throwable e) {
    }
    try {
      Field field = FST.Arc.class.getField("output");
      return fst -> {
        try {
          return (Long) field.get(fst);
        }
        catch (Throwable e) {
          throw Throwables.propagate(e);
        }
      };
    }
    catch (Throwable e) {
    }
    return fst -> {
      throw new UOE("output?");
    };
  }

  private final FST<Long> _fst;
  private final Automaton _automaton;

  public AutomatonMatcher(Automaton automaton, FST<Long> fst)
  {
    _automaton = automaton;
    _fst = fst;
  }

  public AutomatonMatcher(String regexQuery, FST<Long> fst)
  {
    this(new RegExp(regexQuery).toAutomaton(), fst);
  }

  public static IntList regexMatch(String regexQuery, FST<Long> fst)
      throws IOException
  {
    return new AutomatonMatcher(regexQuery, fst).matchOnFST();
  }

  public static IntList match(Automaton automaton, FST<Long> fst)
      throws IOException
  {
    return new AutomatonMatcher(automaton, fst).matchOnFST();
  }

  // Matches "input" string with _regexQuery Automaton.
  public boolean match(String input)
  {
    CharacterRunAutomaton characterRunAutomaton = new CharacterRunAutomaton(_automaton);
    return characterRunAutomaton.run(input);
  }

  /**
   * This function runs matching on automaton built from regexQuery and the FST.
   * FST stores key (string) to a value (Long). Both are state machines and state transition is based on
   * a input character.
   *
   * This algorithm starts with Queue containing (Automaton Start Node, FST Start Node).
   * Each step an entry is popped from the queue:
   *    1) if the automaton state is accept and the FST Node is final (i.e. end node) then the value stored for that FST
   *       is added to the set of result.
   *    2) Else next set of transitions on automaton are gathered and for each transition target node for that character
   *       is figured out in FST Node, resulting pair of (automaton state, fst node) are added to the queue.
   *    3) This process is bound to complete since we are making progression on the FST (which is a DAG) towards final
   *       nodes.
   * @return
   * @throws IOException
   */
  public IntList matchOnFST() throws IOException
  {
    final List<Path<Long>> queue = new ArrayList<>();
    final List<Path<Long>> endNodes = new ArrayList<>();
    if (_automaton.getNumStates() == 0) {
      return new IntList(0);
    }

    // Automaton start state and FST start node is added to the queue.
    queue.add(new Path<>(0, _fst.getFirstArc(new FST.Arc<Long>()), _fst.outputs.getNoOutput(), new IntsRefBuilder()));

    final FST.Arc<Long> scratchArc = new FST.Arc<>();
    final FST.BytesReader fstReader = _fst.getBytesReader();

    final Transition t = new Transition();
    while (queue.size() != 0) {
      final Path<Long> path = queue.remove(queue.size() - 1);

      // If automaton is in accept state and the fstNode is final (i.e. end node) then add the entry to endNodes which
      // contains the result set.
      if (_automaton.isAccept(path.state)) {
        if (path.fstNode.isFinal()) {
          endNodes.add(path);
        }
      }

      // Gather next set of transitions on automaton and find target nodes in FST.
      IntsRefBuilder currentInput = path.input;
      int count = _automaton.initTransition(path.state, t);
      for (int i = 0; i < count; i++) {
        _automaton.getNextTransition(t);
        final int min = t.min;
        final int max = t.max;
        if (min == max) {
          final FST.Arc<Long> nextArc = _fst.findTargetArc(t.min, path.fstNode, scratchArc, fstReader);
          if (nextArc != null) {
            final IntsRefBuilder newInput = new IntsRefBuilder();
            newInput.copyInts(currentInput.get());
            newInput.append(t.min);
            queue.add(new Path<Long>(t.dest, new FST.Arc<Long>().copyFrom(nextArc),
                                     _fst.outputs.add(path.output, OUTPUT.apply(nextArc)), newInput
            ));
          }
        } else {
          FST.Arc<Long> nextArc = Util.readCeilArc(min, _fst, path.fstNode, scratchArc, fstReader);
          while (nextArc != null && LABEL.applyAsInt(nextArc) <= max) {
            final IntsRefBuilder newInput = new IntsRefBuilder();
            newInput.copyInts(currentInput.get());
            newInput.append(LABEL.applyAsInt(nextArc));
            queue.add(new Path<>(
                t.dest, new FST.Arc<Long>().copyFrom(nextArc),
                _fst.outputs.add(path.output, OUTPUT.apply(nextArc)), newInput
            ));
            nextArc = nextArc.isLast() ? null : _fst.readNextRealArc(nextArc, fstReader);
          }
        }
      }
    }

    // From the result set of matched entries gather the values stored and return.
    IntList matchedIds = new IntList();
    for (Path<Long> path : endNodes) {
      matchedIds.add(Ints.checkedCast(path.output));
    }
    return matchedIds;
  }

  public static final class Path<T>
  {
    public final int state;
    public final FST.Arc<T> fstNode;
    public final T output;
    public final IntsRefBuilder input;

    public Path(int state, FST.Arc<T> fstNode, T output, IntsRefBuilder input)
    {
      this.state = state;
      this.fstNode = fstNode;
      this.output = output;
      this.input = input;
    }
  }
}
