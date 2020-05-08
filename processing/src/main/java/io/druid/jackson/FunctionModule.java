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

package io.druid.jackson;

import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.AnnotatedClass;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.jsontype.SubtypeResolver;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Binder;
import com.google.inject.Inject;
import io.druid.initialization.DruidModule;
import io.druid.java.util.common.logger.Logger;
import io.druid.math.expr.Function;
import io.druid.math.expr.Parser;
import io.druid.query.ModuleBuiltinFunctions;
import io.druid.query.sql.SQLFunctions;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class FunctionModule implements DruidModule
{
  private static final Logger LOG = new Logger(FunctionModule.class);

  @Inject
  public static void init(ObjectMapper mapper)
  {
    LOG.info("finding expression functions..");
    Parser.parse("1");  // load builtin functions first
    for (NamedType subType : resolveSubtypes(mapper, Function.Library.class)) {
      Parser.register(subType.getType());
    }
  }

  public static List<NamedType> resolveSubtypes(ObjectMapper mapper, Class<?> clazz)
  {
    JavaType type = mapper.getTypeFactory().constructType(clazz);

    DeserializationConfig config = mapper.getDeserializationConfig();
    AnnotatedClass annotated = config.introspectClassAnnotations(type).getClassInfo();
    AnnotationIntrospector inspector = config.getAnnotationIntrospector();

    SubtypeResolver resolver = mapper.getSubtypeResolver();

    List<NamedType> found = Lists.newArrayList();
    for (NamedType resolved : resolver.collectAndResolveSubtypes(annotated, config, inspector)) {
      if (resolved.getType() != clazz) {  // filter self
        found.add(resolved);
      }
    }
    return found;
  }

  public static Map<String, Class> resolveSubtypesAsMap(ObjectMapper mapper, Class<?> clazz)
  {
    Map<String, Class> mapping = Maps.newHashMap();
    for (NamedType namedType : resolveSubtypes(mapper, clazz)) {
      mapping.put(namedType.getName(), namedType.getType());
    }
    return mapping;
  }

  public static Map<Class, String> resolveSubtypesAsInverseMap(ObjectMapper mapper, Class<?> clazz)
  {
    Map<Class, String> mapping = Maps.newHashMap();
    for (NamedType namedType : resolveSubtypes(mapper, clazz)) {
      mapping.put(namedType.getType(), namedType.getName());
    }
    return mapping;
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Arrays.<Module>asList(
        new SimpleModule("FunctionModule")
            .registerSubtypes(ModuleBuiltinFunctions.class)
            .registerSubtypes(SQLFunctions.class)
    );
  }

  @Override
  public void configure(Binder binder)
  {
    binder.requestStaticInjection(FunctionModule.class);
    binder.requestStaticInjection(ModuleBuiltinFunctions.class);
  }
}
