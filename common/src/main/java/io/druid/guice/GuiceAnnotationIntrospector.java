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

package io.druid.guice;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.databind.introspect.AnnotatedField;
import com.fasterxml.jackson.databind.introspect.AnnotatedMember;
import com.fasterxml.jackson.databind.introspect.AnnotatedMethod;
import com.fasterxml.jackson.databind.introspect.AnnotatedParameter;
import com.fasterxml.jackson.databind.introspect.NopAnnotationIntrospector;
import com.google.inject.BindingAnnotation;
import com.google.inject.Key;
import io.druid.java.util.common.IAE;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.lang.reflect.Type;

/**
 */
public class GuiceAnnotationIntrospector extends NopAnnotationIntrospector
{
  @Override
  public Object findInjectableValueId(AnnotatedMember m)
  {
    if (m.getAnnotation(JacksonInject.class) == null) {
      return null;
    }

    Annotation guiceAnnotation = null;
    for (Annotation annotation : m.annotations()) {
      if (annotation.annotationType().isAnnotationPresent(BindingAnnotation.class)) {
        guiceAnnotation = annotation;
        break;
      }
    }

    if (guiceAnnotation == null) {
      if (m instanceof AnnotatedMethod) {
        throw new IAE("Annotated methods don't work very well yet...");
      }
      return Key.get(genericType(m));
    }
    return Key.get(genericType(m), guiceAnnotation);
  }

  // Jackson 2.7+ removed AnnotatedMember.getGenericType(); reconstruct the
  // reflective generic type from the concrete member.
  private static Type genericType(AnnotatedMember m)
  {
    if (m instanceof AnnotatedField) {
      return ((AnnotatedField) m).getAnnotated().getGenericType();
    }
    if (m instanceof AnnotatedParameter) {
      // NB: AnnotatedParameter.getParameterType() returns Jackson's JavaType (it
      // implements java.lang.reflect.Type, but Guice's Key only accepts
      // Class/ParameterizedType/GenericArrayType). Resolve the real reflective
      // parameter type from the owning constructor/method instead.
      final AnnotatedParameter p = (AnnotatedParameter) m;
      final Member owner = p.getOwner() == null ? null : p.getOwner().getMember();
      if (owner instanceof Constructor) {
        return ((Constructor<?>) owner).getGenericParameterTypes()[p.getIndex()];
      }
      if (owner instanceof Method) {
        return ((Method) owner).getGenericParameterTypes()[p.getIndex()];
      }
    }
    // Raw class is a Guice-compatible java.lang.reflect.Type; never hand Guice a JavaType.
    return m.getType().getRawClass();
  }
}
