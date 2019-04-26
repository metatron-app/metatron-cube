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

package io.druid.hive;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.metamx.common.logger.Logger;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.math.expr.Evals;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.math.expr.Function;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ObjectInspectors;

import java.util.List;

public class HiveFunctions implements Function.Provider
{
  private static final Logger LOG = new Logger(HiveFunctions.class);

  @Override
  public Iterable<Function.Factory> getFunctions()
  {
    // ReflectionUtils uses context loader
    final ClassLoader prev = Thread.currentThread().getContextClassLoader();
    final List<Function.Factory> factories;
    try {
      Thread.currentThread().setContextClassLoader(HiveFunctions.class.getClassLoader());
      factories = Lists.newArrayList();
      for (String name : FunctionRegistry.getFunctionNames()) {
        FunctionInfo function;
        try {
          function = FunctionRegistry.getFunctionInfo(name);
        }
        catch (SemanticException e) {
          continue; // ignore.. blocked function ?
        }
        if (function.isGenericUDF()) {
          factories.add(new HiveAdapter("hive." + name, function));
        }
      }
    }
    finally {
      Thread.currentThread().setContextClassLoader(prev);
    }
    return factories;
  }

  private static class HiveAdapter implements Function.Factory
  {
    private final String name;
    private final FunctionInfo functionInfo;

    private HiveAdapter(String name, FunctionInfo functionInfo)
    {
      this.name = name;
      this.functionInfo = functionInfo;
    }

    @Override
    public String name()
    {
      return name;
    }

    @Override
    public Function create(List<Expr> args)
    {
      final GenericUDF genericUDF = functionInfo.getGenericUDF();
      final GenericUDF.DeferredObject[] params = new GenericUDF.DeferredObject[args.size()];
      return new Function()
      {
        private ObjectInspector[] arguments;
        private ObjectInspector output;
        private ValueDesc outputType;

        @Override
        public String name()
        {
          return name;
        }

        @Override
        public ValueDesc apply(List<Expr> args, TypeResolver bindings)
        {
          return intialize(args, bindings);
        }

        @Override
        public ExprEval apply(List<Expr> args, Expr.NumericBinding bindings)
        {
          Preconditions.checkArgument(bindings instanceof TypeResolver, "Hive function needs type binding");
          intialize(args, (TypeResolver) bindings);
          for (int i = 0; i < params.length; i++) {
            params[i] = new GenericUDF.DeferredJavaObject(Evals.eval(args.get(i), bindings).value());
          }
          try {
            Object result = genericUDF.evaluate(params);
            return ExprEval.of(ObjectInspectors.evaluate(output, result), outputType);
          }
          catch (HiveException e) {
            throw Throwables.propagate(e);
          }
        }

        public ValueDesc intialize(List<Expr> args, TypeResolver bindings)
        {
          if (arguments == null) {
            arguments = toObjectInspectors(args, bindings);
            try {
              output = genericUDF.initializeAndFoldConstants(arguments);
            }
            catch (UDFArgumentException e) {
              throw new IllegalArgumentException(e);
            }
            outputType = ObjectInspectors.typeOf(output, ValueDesc.UNKNOWN);
          }
          return outputType;
        }
      };
    }

    private ObjectInspector[] toObjectInspectors(List<Expr> args, TypeResolver bindings)
    {
      final List<ObjectInspector> inspectors = Lists.newArrayList();
      for (Expr arg : args) {
        ObjectInspector inspector = ObjectInspectors.toObjectInspector(arg.resolve(bindings));
        if (inspector == null) {
          throw new IllegalArgumentException("cannot resolve " + args);
        }
        inspectors.add(inspector);
      }

      return inspectors.toArray(new ObjectInspector[0]);
    }
  }
}
