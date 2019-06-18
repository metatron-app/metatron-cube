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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Binder;
import com.google.inject.Binding;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.spi.ProviderInstanceBinding;
import com.google.inject.util.Types;

import java.lang.annotation.Annotation;
import java.lang.reflect.ParameterizedType;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;


/**
 * Provides a singleton value of type {@code <T>} from {@code Properties} bound in guice.
 * <br/>
 * <h3>Usage</h3>
 * To install this provider, bind it in your guice module, like below.
 *
 * <pre>
 * JsonConfigProvider.bind(binder, "druid.server", DruidServerConfig.class);
 * </pre>
 * <br/>
 * In the above case, {@code druid.server} should be a key found in the {@code Properties} bound elsewhere.
 * The value of that key should directly relate to the fields in {@code DruidServerConfig.class}.
 *
 * <h3>Implementation</h3>
 * <br/>
 * The state of {@code <T>} is defined by the value of the property {@code propertyBase}.
 * This value is a json structure, decoded via {@link JsonConfigurator#configurate(java.util.Properties, String, Class)}.
 * <br/>
 *
 * An example might be if DruidServerConfig.class were
 *
 * <pre>
 *   public class DruidServerConfig
 *   {
 *     @JsonProperty @NotNull public String hostname = null;
 *     @JsonProperty @Min(1025) public int port = 8080;
 *   }
 * </pre>
 *
 * And your Properties object had in it
 *
 * <pre>
 *   druid.server.hostname=0.0.0.0
 *   druid.server.port=3333
 * </pre>
 *
 * Then this would bind a singleton instance of a DruidServerConfig object with hostname = "0.0.0.0" and port = 3333.
 *
 * If the port weren't set in the properties, then the default of 8080 would be taken.  Essentially, it is the same as
 * subtracting the "druid.server" prefix from the properties and building a Map which is then passed into
 * ObjectMapper.convertValue()
 *
 * @param <T> type of config object to provide.
 */
public class JsonConfigProvider<T> implements Provider<Supplier<T>>
{
  private static final Map<String, Key> BASE_TO_CLASS = Maps.newConcurrentMap();

  @SuppressWarnings("unchecked")
  public static <T> void bind(Binder binder, String propertyBase, Class<T> classToProvide)
  {
    bind(
        binder,
        propertyBase,
        classToProvide,
        Key.get(classToProvide),
        (Key) Key.get(Types.newParameterizedType(Supplier.class, classToProvide))
    );
  }

  @SuppressWarnings("unchecked")
  public static <T> void bind(Binder binder, String propertyBase, Class<T> classToProvide, T defaultValue)
  {
    bind(
        binder,
        propertyBase,
        classToProvide,
        defaultValue,
        Key.get(classToProvide),
        (Key) Key.get(Types.newParameterizedType(Supplier.class, classToProvide))
    );
  }

  @SuppressWarnings("unchecked")
  public static <T> void bind(Binder binder, String propertyBase, Class<T> classToProvide, Annotation annotation)
  {
    bind(
        binder,
        propertyBase,
        classToProvide,
        Key.get(classToProvide, annotation),
        (Key) Key.get(Types.newParameterizedType(Supplier.class, classToProvide), annotation)
    );
  }

  @SuppressWarnings("unchecked")
  public static <T> void bind(
      Binder binder,
      String propertyBase,
      Class<T> classToProvide,
      Class<? extends Annotation> annotation
  )
  {
    bind(
        binder,
        propertyBase,
        classToProvide,
        Key.get(classToProvide, annotation),
        (Key) Key.get(Types.newParameterizedType(Supplier.class, classToProvide), annotation)
    );
  }

  @SuppressWarnings("unchecked")
  public static <T> void bind(
      Binder binder,
      String propertyBase,
      Class<T> clazz,
      Key<T> instanceKey,
      Key<Supplier<T>> supplierKey
  )
  {
    bind(binder, propertyBase, clazz, null, instanceKey, supplierKey);
  }

  @SuppressWarnings("unchecked")
  public static <T> void bind(
      Binder binder,
      String propertyBase,
      Class<T> clazz,
      T defaultValue,
      Key<T> instanceKey,
      Key<Supplier<T>> supplierKey
  )
  {
    binder.bind(supplierKey).toProvider((Provider) of(propertyBase, clazz, defaultValue)).in(LazySingleton.class);
    binder.bind(instanceKey).toProvider(new SupplierProvider<T>(supplierKey));
    BASE_TO_CLASS.put(propertyBase, supplierKey);
  }

  public static boolean configure(Injector injector, String propertyBase, Map<String, String> properties)
  {
    Provider provider = getProvider(injector, propertyBase);
    if (provider instanceof JsonConfigProvider) {
      try {
        ((JsonConfigProvider) provider).configure(properties);
        return true;
      }
      catch (Exception e) {
        // ignore
      }
    }
    return false;
  }

  public static <T> T get(Injector injector, String propertyBase)
  {
    Provider<T> provider = getProvider(injector, propertyBase);
    return provider == null ? null : provider.get();
  }

  public static List<String> getKeys()
  {
    List<String> keys = Lists.newArrayList(BASE_TO_CLASS.keySet());
    Collections.sort(keys);
    return keys;
  }

  @SuppressWarnings("unchecked")
  private static <T> Provider<T> getProvider(Injector injector, String propertyBase)
  {
    Key supplierKey = BASE_TO_CLASS.get(propertyBase);
    if (supplierKey != null) {
      Binding<T> binding = injector.getBinding(supplierKey);
      if (binding instanceof ProviderInstanceBinding) {
        return ((ProviderInstanceBinding) binding).getProviderInstance();
      }
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  public static <T> void bindInstance(
      Binder binder,
      Key<T> bindKey,
      T instance
  )
  {
    binder.bind(bindKey).toInstance(instance);

    final ParameterizedType supType = Types.newParameterizedType(Supplier.class, bindKey.getTypeLiteral().getType());
    final Key supplierKey;

    if (bindKey.getAnnotationType() != null) {
      supplierKey = Key.get(supType, bindKey.getAnnotationType());
    }
    else if (bindKey.getAnnotation() != null) {
      supplierKey = Key.get(supType, bindKey.getAnnotation());
    }
    else {
      supplierKey = Key.get(supType);
    }

    binder.bind(supplierKey).toInstance(Suppliers.<T>ofInstance(instance));
  }

  public static <T> JsonConfigProvider<T> of(String propertyBase, Class<T> classToProvide)
  {
    return of(propertyBase, classToProvide, null);
  }

  public static <T> JsonConfigProvider<T> of(String propertyBase, Class<T> classToProvide, T defaultValue)
  {
    return new JsonConfigProvider<T>(propertyBase, classToProvide, defaultValue);
  }

  private final String propertyBase;
  private final Class<T> classToProvide;
  private final T defaultValue;

  private Properties props;
  private JsonConfigurator configurator;

  private Supplier<T> retVal = null;

  public JsonConfigProvider(
      String propertyBase,
      Class<T> classToProvide,
      T defaultValue
  )
  {
    this.propertyBase = propertyBase;
    this.classToProvide = classToProvide;
    this.defaultValue = defaultValue;
  }

  @Inject
  public void inject(
      Properties props,
      JsonConfigurator configurator
  )
  {
    this.props = props;
    this.configurator = configurator;
  }

  @Override
  public Supplier<T> get()
  {
    if (retVal != null) {
      return retVal;
    }

    try {
      final T config = configurator.configurate(props, propertyBase, classToProvide, defaultValue, null);
      retVal = Suppliers.ofInstance(config);
    }
    catch (RuntimeException e) {
      // When a runtime exception gets thrown out, this provider will get called again if the object is asked for again.
      // This will have the same failed result, 'cause when it's called no parameters will have actually changed.
      // Guice will then report the same error multiple times, which is pretty annoying. Cache a null supplier and
      // return that instead.  This is technically enforcing a singleton, but such is life.
      retVal = Suppliers.ofInstance(null);
      throw e;
    }
    return retVal;
  }

  private void configure(Map<String, String> properties)
  {
    Supplier<T> supplier = get();
    retVal = Suppliers.ofInstance(
        configurator.configurate(props, propertyBase, classToProvide, supplier.get(), properties)
    );
  }
}
