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

package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils.ConversionHelper;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

import java.lang.reflect.Method;
import java.lang.reflect.Type;

public class Hack
{
  public static GenericUDAFEvaluator synchronize(final GenericUDAFBridge.GenericUDAFBridgeEvaluator evaluator)
      throws UDFArgumentException
  {
    if (!evaluator.conversionHelper.conversionNeeded) {
      return evaluator;
    }
    final boolean isVariableLengthArgument = evaluator.conversionHelper.lastParaElementType != null;
    if (isVariableLengthArgument) {
      throw new UnsupportedOperationException("Not supports var-arg evaluator, yet");
    }
    final Method aggregateMethod;
    if (evaluator.mode == Mode.PARTIAL1 || evaluator.mode == Mode.COMPLETE) {
      aggregateMethod = evaluator.iterateMethod;
    } else {
      aggregateMethod = evaluator.mergeMethod;
    }

    final ObjectInspector[] parameterOI = evaluator.parameterOIs;
    final ObjectInspector[] expectedOI = toExpectedOI(aggregateMethod, parameterOI, evaluator.conversionHelper);
    final Converter[] converter = new Converter[parameterOI.length];
    for (int i = 0; i < parameterOI.length; i++) {
      converter[i] = ObjectInspectorConverters.getConverter(parameterOI[i], expectedOI[i]);
    }
    evaluator.conversionHelper = new ConversionHelper(aggregateMethod, parameterOI)
    {
      @Override
      public Object[] convertIfNecessary(final Object... parameters)
      {
        final Object[] converted = new Object[parameters.length];
        for (int i = 0; i < parameters.length; i++) {
          converted[i] = converter[i].convert(parameters[i]);
        }
        return converted;
      }
    };
    return evaluator;
  }

  // copied from ConversionHelper
  private static ObjectInspector[] toExpectedOI(Method m, ObjectInspector[] parameterOIs, ConversionHelper converter)
  {
    final Type[] methodParameterTypes = converter.methodParameterTypes;

    // Create the output OI array
    final ObjectInspector[] methodParameterOIs = new ObjectInspector[parameterOIs.length];

    for (int i = 0; i < methodParameterTypes.length; i++) {
      // This method takes Object, so it accepts whatever types that are passed in.
      if (methodParameterTypes[i] == Object.class) {
        methodParameterOIs[i] = ObjectInspectorUtils.getStandardObjectInspector(
            parameterOIs[i], ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA
        );
      } else {
        methodParameterOIs[i] = ObjectInspectorFactory.getReflectionObjectInspector(
            methodParameterTypes[i], ObjectInspectorFactory.ObjectInspectorOptions.JAVA
        );
      }
    }
    return methodParameterOIs;
  }
}
