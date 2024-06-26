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

package io.druid.math.expr;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.druid.math.expr.Expression.AndExpression;
import io.druid.math.expr.Expression.NotExpression;
import io.druid.math.expr.Expression.OrExpression;
import io.druid.math.expr.Expression.RelationExpression;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 */
public class Expressions
{
  public static <T extends Expression> T convertToCNF(T start, Expression.Factory<T> factory)
  {
    T current = start;
    try {
      current = pushDownNot(current, factory);
      current = flatten(current, factory);
      current = convertToCNFInternal(current, factory);
      current = flatten(current, factory);
      return current;
    }
    catch (Exception e) {
      return start;
    }
  }

  // CNF conversion functions were adapted from Apache Hive, see:
  // https://github.com/apache/hive/blob/branch-2.0/storage-api/src/java/org/apache/hadoop/hive/ql/io/sarg/SearchArgumentImpl.java
  private static <T extends Expression> T pushDownNot(T current, Expression.Factory<T> factory)
  {
    if (current instanceof NotExpression) {
      T child = ((NotExpression) current).getChild();
      if (child instanceof NotExpression) {
        return pushDownNot(((NotExpression) child).<T>getChild(), factory);
      }
      if (child instanceof AndExpression) {
        List<T> children = Lists.newArrayList();
        for (T grandChild : ((AndExpression) child).<T>getChildren()) {
          children.add(pushDownNot(factory.not(grandChild), factory));
        }
        return factory.or(children);
      }
      if (child instanceof OrExpression) {
        List<T> children = Lists.newArrayList();
        for (T grandChild : ((OrExpression) child).<T>getChildren()) {
          children.add(pushDownNot(factory.not(grandChild), factory));
        }
        return factory.and(children);
      }
    }

    if (current instanceof AndExpression) {
      boolean changed = false;
      List<T> children = Lists.newArrayList();
      for (T child : ((AndExpression) current).<T>getChildren()) {
        T pushed = pushDownNot(child, factory);
        changed |= child != pushed;
        children.add(pushed);
      }
      return changed ? factory.and(children) : current;
    }

    if (current instanceof OrExpression) {
      boolean changed = false;
      List<T> children = Lists.newArrayList();
      for (T child : ((OrExpression) current).<T>getChildren()) {
        T pushed = pushDownNot(child, factory);
        changed |= child != pushed;
        children.add(pushed);
      }
      return changed ? factory.or(children) : current;
    }
    return current;
  }

  // CNF conversion functions were adapted from Apache Hive, see:
  // https://github.com/apache/hive/blob/branch-2.0/storage-api/src/java/org/apache/hadoop/hive/ql/io/sarg/SearchArgumentImpl.java
  private static <T extends Expression> T convertToCNFInternal(T current, Expression.Factory<T> factory)
  {
    if (current instanceof NotExpression) {
      T child = ((NotExpression) current).<T>getChild();
      T converted = convertToCNFInternal(child, factory);
      return child != converted ? factory.not(converted) : current;
    }
    if (current instanceof AndExpression) {
      boolean changed = false;
      List<T> children = Lists.newArrayList();
      for (T child : ((AndExpression) current).<T>getChildren()) {
        T converted = convertToCNFInternal(child, factory);
        changed |= child != converted;
        children.add(converted);
      }
      return changed ? factory.and(children) : current;
    }
    if (current instanceof OrExpression) {
      // a list of leaves that weren't under AND expressions
      List<T> nonAndList = new ArrayList<T>();
      // a list of AND expressions that we need to distribute
      List<AndExpression> andList = new ArrayList<AndExpression>();
      for (T child : ((OrExpression) current).<T>getChildren()) {
        if (child instanceof AndExpression) {
          andList.add((AndExpression) child);
        } else if (child instanceof OrExpression) {
          // pull apart the kids of the OR expression
          nonAndList.addAll(((OrExpression) child).<T>getChildren());
        } else {
          nonAndList.add(child);
        }
      }
      if (!andList.isEmpty()) {
        List<T> result = Lists.newArrayList();
        generateAllCombinations(result, andList, nonAndList, factory);
        return factory.and(result);
      }
    }
    return current;
  }

  // CNF conversion functions were adapted from Apache Hive, see:
  // https://github.com/apache/hive/blob/branch-2.0/storage-api/src/java/org/apache/hadoop/hive/ql/io/sarg/SearchArgumentImpl.java
  private static <T extends Expression> T flatten(T root, Expression.Factory<T> factory)
  {
    if (root instanceof RelationExpression) {
      RelationExpression parent = (RelationExpression) root;
      if (parent instanceof NotExpression) {
        T child = ((NotExpression) parent).getChild();
        T flatten = flatten(child, factory);
        if (flatten instanceof NotExpression) {
          return ((NotExpression) flatten).getChild();
        }
        return child != flatten ? factory.not(flatten) : root;
      }
      boolean changed = false;
      List<T> children = new ArrayList<>(parent.<T>getChildren());
      // iterate through the index, so that if we add more children,
      // they don't get re-visited
      for (int i = 0; i < children.size(); ++i) {
        T child = children.get(i);
        T flatten = flatten(child, factory);
        if (flatten == null) {
          throw new IllegalStateException("null child from " + child);
        }
        // do we need to flatten?
        if (flatten.getClass() == parent.getClass()) {
          changed = true;
          boolean first = true;
          List<T> grandKids = ((RelationExpression) flatten).getChildren();
          for (T grandkid : grandKids) {
            // for the first grandkid replace the original parent
            if (first) {
              first = false;
              children.set(i, grandkid);
            } else {
              children.add(++i, grandkid);
            }
          }
        } else {
          changed |= child != flatten;
          children.set(i, flatten);
        }
      }
      if (!changed) {
        return root;
      }
      // if we have a singleton AND or OR, just return the child
      if (parent instanceof AndExpression) {
        return factory.and(children);
      } else if (parent instanceof OrExpression) {
        return factory.or(children);
      }
    }
    return root;
  }

  // CNF conversion functions were adapted from Apache Hive, see:
  // https://github.com/apache/hive/blob/branch-2.0/storage-api/src/java/org/apache/hadoop/hive/ql/io/sarg/SearchArgumentImpl.java
  private static <T extends Expression> void generateAllCombinations(
      List<T> result,
      List<AndExpression> andList,
      List<T> nonAndList,
      Expression.Factory<T> factory
  )
  {
    List<T> children = andList.get(0).getChildren();
    if (result.isEmpty()) {
      for (T child : children) {
        List<T> a = Lists.newArrayList(nonAndList);
        a.add(child);
        result.add(factory.or(a));
      }
    } else {
      List<T> work = new ArrayList<>(result);
      result.clear();
      for (T child : children) {
        for (T or : work) {
          List<T> a = Lists.<T>newArrayList();
          if (or instanceof OrExpression) {
            a.addAll(((OrExpression) or).<T>getChildren());
          } else {
            a.add(or);
          }
          a.add(child);
          result.add(factory.or(a));
        }
      }
    }
    if (andList.size() > 1) {
      if (result.size() + checkCombination(andList) > CNF_COMBINATIONS_THRESHOLD) {
        throw new RuntimeException();
      }
      generateAllCombinations(
          result, andList.subList(1, andList.size()), nonAndList, factory
      );
    }
  }

  private static final int CNF_COMBINATIONS_THRESHOLD = 16;

  private static int checkCombination(List<AndExpression> andList)
  {
    int numComb = 1;
    for (AndExpression tree : andList) {
      numComb *= tree.getChildren().size();
    }
    return numComb;
  }

  public static interface Visitor<T extends Expression, V>
  {
    // return false for exit
    boolean visit(T expression);

    V get();

    public abstract class Void<T extends Expression> implements Visitor<T, Void>
    {
      @Override
      public final Void get() { return null; }
    }
  }

  public static interface Rewriter<T extends Expression>
  {
    T visit(T expression);
  }

  @SuppressWarnings("unchecked")
  public static <T extends Expression, V> boolean traverse(T expression, Visitor<T, V> visitor)
  {
    if (expression instanceof NotExpression) {
      return traverse(((NotExpression) expression).getChild(), visitor);
    } else if (expression instanceof RelationExpression) {
      for (Expression child : ((RelationExpression) expression).getChildren()) {
        if (!traverse((T) child, visitor)) {
          return false;
        }
      }
      return true;
    }
    return visitor.visit(expression);
  }

  @SuppressWarnings("unchecked")
  public static <T extends Expression, V> T rewrite(T expression, Expression.Factory<T> factory, Rewriter<T> visitor)
  {
    if (expression instanceof NotExpression) {
      T child = ((NotExpression) expression).getChild();
      T rewritten = rewrite(child, factory, visitor);
      if (child != rewritten) {
        expression = factory.not(rewritten);
      }
    } else if (expression instanceof RelationExpression) {
      List<T> rewrittens = Lists.newArrayList();
      boolean changed = false;
      for (Expression child : ((RelationExpression) expression).getChildren()) {
        T rewritten = rewrite((T) child, factory, visitor);
        changed |= child != rewritten;
        if (rewritten != null) {
          rewrittens.add(rewritten);
        }
      }
      if (changed) {
        if (expression instanceof AndExpression) {
          expression = factory.and(rewrittens);
        } else if (expression instanceof OrExpression) {
          expression = factory.or(rewrittens);
        }
      }
    } else {
      expression = visitor.visit(expression);
    }
    return expression;
  }

  private static final Set<String> COMPARES = ImmutableSet.of("==", ">", "<", ">=", "<=");

  public static boolean isCompare(String op)
  {
    return COMPARES.contains(op);
  }
}
