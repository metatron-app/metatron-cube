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

package io.druid.initialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.ObjectArrays;
import com.google.common.collect.Sets;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import io.druid.common.guava.GuavaUtils;
import io.druid.curator.CuratorModule;
import io.druid.curator.discovery.DiscoveryModule;
import io.druid.guice.AWSModule;
import io.druid.guice.AnnouncerModule;
import io.druid.guice.CoordinatorDiscoveryModule;
import io.druid.guice.DruidProcessingModule;
import io.druid.guice.DruidSecondaryModule;
import io.druid.guice.ExtensionsConfig;
import io.druid.guice.FirehoseModule;
import io.druid.guice.IndexingServiceDiscoveryModule;
import io.druid.guice.JacksonConfigManagerModule;
import io.druid.guice.JavaScriptModule;
import io.druid.guice.JerseyModule;
import io.druid.guice.Jerseys;
import io.druid.guice.LifecycleModule;
import io.druid.guice.LocalDataStorageDruidModule;
import io.druid.guice.MetadataConfigModule;
import io.druid.guice.ParsersModule;
import io.druid.guice.QueryRunnerFactoryModule;
import io.druid.guice.QueryableModule;
import io.druid.guice.ServerModule;
import io.druid.guice.ServerViewModule;
import io.druid.guice.StartupLoggingModule;
import io.druid.guice.StorageNodeModule;
import io.druid.guice.annotations.Client;
import io.druid.guice.annotations.EscalatedClient;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import io.druid.guice.http.HttpClientModule;
import io.druid.guice.security.AuthenticatorModule;
import io.druid.guice.security.AuthorizerModule;
import io.druid.guice.security.DruidAuthModule;
import io.druid.guice.security.EscalatorModule;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.metadata.storage.derby.DerbyMetadataStorageDruidModule;
import io.druid.query.ordering.StringComparators;
import io.druid.server.initialization.AuthenticatorMapperModule;
import io.druid.server.initialization.AuthorizerMapperModule;
import io.druid.server.initialization.EmitterModule;
import io.druid.server.initialization.jetty.JettyServerModule;
import io.druid.server.metrics.MetricsModule;
import org.apache.commons.io.FileUtils;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.jetty.util.ConcurrentHashSet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.Set;

/**
 */
public class Initialization
{
  private static final Logger log = new Logger(Initialization.class);

  public static final String git_tag;
  public static final String git_revision;

  private static String readString(String fileName, String defaultValue)
  {
    File file = new File(System.getProperty("user.dir"), fileName);
    try (InputStream stream = new FileInputStream(file)) {
      if (stream != null) {
        String s = new BufferedReader(new InputStreamReader(stream)).readLine();
        if (s != null && !s.isEmpty()) {
          return s;
        }
      }
    }
    catch (Throwable e) {
      log.info("cannot find/read '%s' file", fileName);
    }
    return defaultValue;
  }

  static {
    git_tag = readString("git.tag", "??");
    git_revision = readString("git.version", "??");
  }

  private static final Map<String, ClassLoader> loadersMap = Maps.newHashMap();

  private static final Map<Class, Set> extensionsMap = Maps.<Class, Set>newHashMap();
  private static final Set<String> failed = new ConcurrentHashSet<>();

  /**
   * @param clazz Module class
   * @param <T>
   *
   * @return Returns the set of modules loaded.
   */
  @SuppressWarnings("unchecked")
  public static <T> Set<T> getLoadedModules(Class<T> clazz)
  {
    Set<T> retVal = extensionsMap.get(clazz);
    if (retVal == null) {
      return Sets.newHashSet();
    }
    return retVal;
  }

  /**
   * Used for testing only
   */
  static void clearLoadedModules()
  {
    extensionsMap.clear();
  }

  /**
   * Used for testing only
   */
  static Map<String, ClassLoader> getLoadersMap()
  {
    return loadersMap;
  }

  /**
   * Look for extension modules for the given class from both classpath and extensions directory. A user should never
   * put the same two extensions in classpath and extensions directory, if he/she does that, the one that is in the
   * classpath will be loaded, the other will be ignored.
   *
   * @param config Extensions configuration
   * @param clazz  The class of extension module (e.g., DruidModule)
   *
   * @return A collection that contains distinct extension modules
   */
  public synchronized static <T> Collection<T> getFromExtensions(ExtensionsConfig config, Class<T> clazz)
  {
    final Set<T> retVal = Sets.newHashSet();
    final Set<String> loadedExtensionNames = Sets.newHashSet();

    if (config.searchCurrentClassloader()) {
      for (T module : ServiceLoader.load(clazz, Thread.currentThread().getContextClassLoader())) {
        final String moduleName = module.getClass().getCanonicalName();
        if (moduleName == null) {
          log.warn(
              "Extension module [%s] was ignored because it doesn't have a canonical name, is it a local or anonymous class?",
              module.getClass().getName()
          );
        } else if (!loadedExtensionNames.contains(moduleName)) {
          log.info("Adding classpath extension module [%s] for class [%s]", moduleName, clazz.getName());
          loadedExtensionNames.add(moduleName);
          retVal.add(module);
        }
      }
    }

    for (File extension : getExtensionFilesToLoad(config)) {
      if (loadedExtensionNames.contains(extension.getName())) {
        continue;
      }
      log.debug("Loading extension [%s] for service type [%s]", extension.getName(), clazz.getName());
      try {
        final ClassLoader loader = getClassLoaderForExtension(config, extension);
        final Iterator<T> modules = ServiceLoader.load(clazz, loader).iterator();
        if (clazz == DruidModule.class && !modules.hasNext() && !ABSTRACT_MODULES.contains(extension.getName())) {
          log.warn("Cannot find any druid module in extension [%s].. skipping", extension.getName());
          continue;
        }
        while (modules.hasNext()) {
          T module = modules.next();
          if (module.getClass().getClassLoader() != loader) {
            continue;
          }
          if (module instanceof DruidModule.WithServices) {
            for (Class<?> service : ((DruidModule.WithServices) module).getServices()) {
              log.info(".. Loading aux service [%s] for extension [%s]", service.getName(), module.getClass());
              Iterator auxLoader = ServiceLoader.load(service, loader).iterator();
              // for aux service, we can ignore loading failure (avatica.UnregisteredDriver, for example)
              while (auxLoader.hasNext()) {
                try {
                  auxLoader.next();   // loaded here
                }
                catch (Throwable e) {
                  if (failed.add(e.toString())) {
                    log.info(e, ".... Failed to load one of aux service.. will ignore similar exceptions");
                  }
                }
              }
            }
          }
          final String moduleName = module.getClass().getCanonicalName();
          if (moduleName == null) {
            log.warn(
                "Extension module [%s] was ignored because it doesn't have a canonical name, is it a local or anonymous class?",
                module.getClass().getName()
            );
          } else if (!loadedExtensionNames.contains(moduleName)) {
            log.info("Found extension module [%s] for [%s]", extension.getName(), moduleName);
            loadedExtensionNames.add(moduleName);
            retVal.add(module);
          }
        }
      }
      catch (Throwable e) {
        log.warn(e, "Failed to load extension [%s]", extension.getName());
        throw Throwables.propagate(e);
      }
    }

    // update the map with currently loaded modules
    extensionsMap.put(clazz, retVal);

    return retVal;
  }

  /**
   * Find all the extension files that should be loaded by druid.
   * <p/>
   * If user explicitly specifies druid.extensions.loadList, then it will look for those extensions under root
   * extensions directory. If one of them is not found, druid will fail loudly.
   * <p/>
   * If user doesn't specify druid.extension.toLoad (or its value is empty), druid will load all the extensions
   * under the root extensions directory.
   *
   * @param config ExtensionsConfig configured by druid.extensions.xxx
   *
   * @return an array of druid extension files that will be loaded by druid process
   */
  public static File[] getExtensionFilesToLoad(ExtensionsConfig config)
  {
    final File rootExtensionsDir = new File(config.getDirectory());
    if (rootExtensionsDir.exists() && !rootExtensionsDir.isDirectory()) {
      throw new ISE("Root extensions directory [%s] is not a directory!?", rootExtensionsDir);
    }
    final List<String> toLoad = config.getLoadList();
    if (GuavaUtils.isNullOrEmpty(toLoad)) {
      return new File[0];
    }
    final Set<String> extensionsToLoad = Sets.newLinkedHashSet();
    for (final String extensionName : toLoad) {
      findRecursive(extensionsToLoad, extensionName);
    }
    final List<File> moduleDirectories = Lists.newArrayList();
    for (String extensionName : extensionsToLoad) {
      File moduleDirectory = toModuleDirectory(rootExtensionsDir, extensionName);
      if (moduleDirectory != null) {
        moduleDirectories.add(moduleDirectory);
      }
    }
    return moduleDirectories.toArray(new File[0]);
  }

  private static void findRecursive(final Set<String> extensionsToLoad, final String extension)
  {
    final String parent = PARENT_MODULES.get(extension);
    if (parent != null && !extensionsToLoad.contains(parent)) {
      findRecursive(extensionsToLoad, parent);
    }
    if (!extensionsToLoad.contains(extension)) {
      extensionsToLoad.add(extension);
    }
  }

  // -_-
  private static final ImmutableMap<String, String> PARENT_MODULES = ImmutableMap.<String, String>builder()
      .put("druid-lucene-common", "druid-geometry-extensions")
      .put("druid-geotools-extensions", "druid-lucene-common")
      .put("druid-lucene-extensions", "druid-lucene-common")
      .put("druid-lucene8-extensions", "druid-lucene-common")
      .put("druid-lucene9-extensions", "druid-lucene-common")
      .put("druid-orc-extensions", "druid-hive-extensions")
      .put("druid-hive-udf-extensions", "druid-hive-extensions")
      .put("druid-hdfs-storage", "druid-indexing-hadoop")
      .put("druid-hadoop-firehose", "druid-indexing-hadoop")
      .put("druid-parquet-extensions", "druid-indexing-hadoop")
      .put("druid-hive-extensions", "druid-indexing-hadoop")
      .put("druid-kafka-eight-simple-consumer", "druid-kafka-extraction-namespace")
      .build();

  private static final String INTERNAL_HADOOP_CLIENT = "$HADOOP_CILENT$";

  // -_-;;;
  private static final ImmutableSet<String> ABSTRACT_MODULES = ImmutableSet.of(
      "druid-lucene-common"
  );

  // -_-;;;
  private static final ImmutableSet<String> HADOOP_DEPENDENT = ImmutableSet.of(
      "druid-indexing-hadoop",
      "druid-geometry-extensions"   // for shape formatter
  );

  public static File toModuleDirectory(File rootExtensionsDir, String extensionName)
  {
    final File extensionDir = new File(rootExtensionsDir, extensionName);
    if (!extensionDir.exists()) {
      log.warn("Failed to find extension [%s]", extensionName);
      return null;
    }
    if (!extensionDir.isDirectory()) {
      log.warn("Extension [%s] should be a directory, not a file", extensionName);
      return null;
    }
    return extensionDir;
  }

  /**
   * Find all the hadoop dependencies that should be loaded by druid
   *
   * @param hadoopDependencyCoordinates e.g.["org.apache.hadoop:hadoop-client:2.3.0"]
   * @param extensionsConfig            ExtensionsConfig configured by druid.extensions.xxx
   *
   * @return an array of hadoop dependency files that will be loaded by druid process
   */
  public static File[] getHadoopDependencyFilesToLoad(
      List<String> hadoopDependencyCoordinates,
      ExtensionsConfig extensionsConfig
  )
  {
    final File rootHadoopDependenciesDir = new File(extensionsConfig.getHadoopDependenciesDir());
    if (rootHadoopDependenciesDir.exists() && !rootHadoopDependenciesDir.isDirectory()) {
      throw new ISE("Root Hadoop dependencies directory [%s] is not a directory!?", rootHadoopDependenciesDir);
    }
    final File[] hadoopDependenciesToLoad = new File[hadoopDependencyCoordinates.size()];
    int i = 0;
    for (final String coordinate : hadoopDependencyCoordinates) {
      final DefaultArtifact artifact = new DefaultArtifact(coordinate);
      final File hadoopDependencyDir = new File(rootHadoopDependenciesDir, artifact.getArtifactId());
      final File versionDir = new File(hadoopDependencyDir, artifact.getVersion());
      // find the hadoop dependency with the version specified in coordinate
      if (!hadoopDependencyDir.isDirectory() || !versionDir.isDirectory()) {
        throw new ISE("Hadoop dependency [%s] didn't exist!?", versionDir.getAbsolutePath());
      }
      hadoopDependenciesToLoad[i++] = versionDir;
    }
    return hadoopDependenciesToLoad;
  }

  private static File getHadoopDependencyFilesToLoad(ExtensionsConfig extensionsConfig)
  {
    final File rootHadoopDependenciesDir = new File(extensionsConfig.getHadoopDependenciesDir());
    if (rootHadoopDependenciesDir.exists() && !rootHadoopDependenciesDir.isDirectory()) {
      throw new ISE("Root Hadoop dependencies directory [%s] is not a directory!?", rootHadoopDependenciesDir);
    }
    final File hadoopDependencyDir = new File(rootHadoopDependenciesDir, "hadoop-client");
    final String[] versions = hadoopDependencyDir.list();
    if (versions == null || versions.length == 0) {
      return null;
    }
    Arrays.sort(versions, StringComparators.ALPHANUMERIC);
    return new File(hadoopDependencyDir, versions[versions.length - 1]);  // use latest version
  }

  /**
   * @param extension The File instance of the extension we want to load
   *
   * @return a URLClassLoader that loads all the jars on which the extension is dependent
   *
   * @throws MalformedURLException
   */
  public static ClassLoader getClassLoaderForExtension(ExtensionsConfig config, File extension)
      throws MalformedURLException
  {
    String extensionName = extension.getName();
    ClassLoader loader = getClassLoaderForExtension(extensionName);
    if (loader == null) {
      URL[] urls = toURLs(extension, true);
      ClassLoader parent = null;
      if (HADOOP_DEPENDENT.contains(extensionName)) {
        parent = getHadoopLoader(config);
      } else if (PARENT_MODULES.containsKey(extensionName)) {
        String parentModule = PARENT_MODULES.get(extensionName);
        parent = getClassLoaderForExtension(parentModule);
        if (parent instanceof URLClassLoader && ABSTRACT_MODULES.contains(parentModule)) {
          // merge.. (for lucene-common)
          urls = ObjectArrays.concat(((URLClassLoader) parent).getURLs(), urls, URL.class);
          parent = parent.getParent();
        }
      }
      parent = parent == null ? Initialization.class.getClassLoader() : parent;
      loader = new NamedURLCloassLoader(extension.getName(), urls, parent);
      loadersMap.put(extensionName, loader);
    }
    return loader;
  }

  private static URL[] toURLs(File extension, boolean withFileLoader) throws MalformedURLException
  {
    final List<URL> urls = Lists.newArrayList();
    for (File jar : FileUtils.listFiles(extension, new String[]{"jar"}, false)) {
      urls.add(jar.toURI().toURL());
    }
    if (withFileLoader) {
      // include extension directory itself to load resource files (like hive.function.properties)
      urls.add(extension.toURI().toURL());
    }
    return urls.toArray(new URL[0]);
  }

  public static ClassLoader getClassLoaderForExtension(String extensionName, ClassLoader defaultLoader)
  {
    return Optional.ofNullable(getClassLoaderForExtension(extensionName)).orElse(defaultLoader);
  }

  public static ClassLoader getClassLoaderForExtension(String extensionName)
  {
    return extensionName == null ? null : loadersMap.get(extensionName);
  }

  private static ClassLoader getHadoopLoader(ExtensionsConfig config) throws MalformedURLException
  {
    ClassLoader parent = Initialization.class.getClassLoader();
    ClassLoader hadoop = getClassLoaderForExtension(INTERNAL_HADOOP_CLIENT);
    if (hadoop == null) {
      File directory = getHadoopDependencyFilesToLoad(config);
      hadoop = new NamedURLCloassLoader("hadoop-client-" + directory.getName(), toURLs(directory, false), parent);
      loadersMap.put(INTERNAL_HADOOP_CLIENT, hadoop);
      log.info("Using hadoop in %s", directory.getPath());
    }
    return hadoop;
  }

  private static class NamedURLCloassLoader extends URLClassLoader
  {
    private final String name;

    public NamedURLCloassLoader(String name, URL[] urls, ClassLoader parent)
    {
      super(urls, parent);
      this.name = name;
      log.debug("Creating class loader for %s (parent = %s)", name, parent);
    }

    @Override
    public String toString()
    {
      return name;
    }
  }

  public static List<URL> getURLsForClasspath(String cp)
  {
    try {
      String[] paths = cp.split(File.pathSeparator);

      List<URL> urls = new ArrayList<>();
      for (int i = 0; i < paths.length; i++) {
        File f = new File(paths[i]);
        if ("*".equals(f.getName())) {
          File parentDir = f.getParentFile();
          if (parentDir.isDirectory()) {
            File[] jars = parentDir.listFiles(
                new FilenameFilter()
                {
                  @Override
                  public boolean accept(File dir, String name)
                  {
                    return name != null && (name.endsWith(".jar") || name.endsWith(".JAR"));
                  }
                }
            );
            for (File jar : jars) {
              urls.add(jar.toURI().toURL());
            }
          }
        } else {
          urls.add(new File(paths[i]).toURI().toURL());
        }
      }
      return urls;
    }
    catch (IOException ex) {
      throw Throwables.propagate(ex);
    }
  }

  public static Injector makeInjectorWithModulesInHadoop(Injector baseInjector, Iterable<? extends Module> modules)
  {
    return _makeInjectorWithModules(baseInjector, modules, true);
  }

  public static Injector makeInjectorWithModules(Injector baseInjector, Iterable<? extends Module> modules)
  {
    return _makeInjectorWithModules(baseInjector, modules, false);
  }

  private static Injector _makeInjectorWithModules(
      final Injector baseInjector,
      final Iterable<? extends Module> modules,
      final boolean loadExtensionsFromClassLoader
  )
  {
    final ModuleList defaultModules = new ModuleList(baseInjector);
    defaultModules.addModules(
        // New modules should be added after Log4jShutterDownerModule
        new Log4jShutterDownerModule(),
        new DruidAuthModule(),
        new LifecycleModule(),
        EmitterModule.class,
        HttpClientModule.global(),
        HttpClientModule.escalatedGlobal(),
        new HttpClientModule("druid.broker.http", Client.class),
        new HttpClientModule("druid.broker.http", EscalatedClient.class),
        new CuratorModule(),
        new AnnouncerModule(),
        new DruidProcessingModule(),
        new AWSModule(),
        new MetricsModule(),
        new ServerModule(),
        new StorageNodeModule(),
        new JettyServerModule(),
        new QueryableModule(),
        new QueryRunnerFactoryModule(),
        new DiscoveryModule(),
        new ServerViewModule(),
        new MetadataConfigModule(),
        new DerbyMetadataStorageDruidModule(),
        new JacksonConfigManagerModule(),
        new IndexingServiceDiscoveryModule(),
        new CoordinatorDiscoveryModule(),
        new LocalDataStorageDruidModule(),
        new FirehoseModule(),
        new ParsersModule(),
        new JavaScriptModule(),
        new AuthenticatorModule(),
        new AuthenticatorMapperModule(),
        new EscalatorModule(),
        new AuthorizerModule(),
        new AuthorizerMapperModule(),
        new StartupLoggingModule()
    );

    final List<Class> resources = Lists.newArrayList();
    ModuleList actualModules = new ModuleList(baseInjector);
    actualModules.addModule(DruidSecondaryModule.class);
    for (Object module : modules) {
      actualModules.addModule(module);
      if (module instanceof JerseyModule) {
        resources.addAll(((JerseyModule) module).getResources());
      }
    }
    actualModules.addModule(new Module()
    {
      @Override
      public void configure(Binder binder)
      {
        for (Class resource : resources) {
          Jerseys.addResource(binder, resource);
        }
      }
    });

    Module intermediateModules = Modules.override(defaultModules.getModules()).with(actualModules.getModules());

    ModuleList extensionModules = new ModuleList(baseInjector);
    ExtensionsConfig config = baseInjector.getInstance(ExtensionsConfig.class);
    if (loadExtensionsFromClassLoader && !config.searchCurrentClassloader()) {
      config = config.withSearchCurrentClassloader(true);
    }
    for (DruidModule module : Initialization.getFromExtensions(config, DruidModule.class)) {
      extensionModules.addModule(module);
    }

    return Guice.createInjector(
        Modules.override(intermediateModules).with(extensionModules.getModules())
    );
  }

  private static class ModuleList
  {
    private final Injector baseInjector;
    private final ObjectMapper jsonMapper;
    private final ObjectMapper smileMapper;
    private final List<Module> modules;

    public ModuleList(Injector baseInjector)
    {
      this.baseInjector = baseInjector;
      this.jsonMapper = baseInjector.getInstance(Key.get(ObjectMapper.class, Json.class));
      this.smileMapper = baseInjector.getInstance(Key.get(ObjectMapper.class, Smile.class));
      this.modules = Lists.newArrayList();
    }

    private List<Module> getModules()
    {
      return Collections.unmodifiableList(modules);
    }

    @SuppressWarnings("unchecked")
    public void addModule(Object input)
    {
      if (input instanceof DruidModule) {
        baseInjector.injectMembers(input);
        modules.add(registerJacksonModules(((DruidModule) input)));
      } else if (input instanceof Module) {
        baseInjector.injectMembers(input);
        modules.add((Module) input);
      } else if (input instanceof Class) {
        if (DruidModule.class.isAssignableFrom((Class) input)) {
          modules.add(registerJacksonModules(baseInjector.getInstance((Class<? extends DruidModule>) input)));
        } else if (Module.class.isAssignableFrom((Class) input)) {
          modules.add(baseInjector.getInstance((Class<? extends Module>) input));
        } else {
          throw new ISE("Class[%s] does not implement %s", input.getClass(), Module.class);
        }
      } else {
        throw new ISE("Unknown module type[%s]", input.getClass());
      }
    }

    public void addModules(Object... object)
    {
      for (Object o : object) {
        addModule(o);
      }
    }

    private DruidModule registerJacksonModules(DruidModule module)
    {
      for (com.fasterxml.jackson.databind.Module jacksonModule : module.getJacksonModules()) {
        jsonMapper.registerModule(jacksonModule);
        smileMapper.registerModule(jacksonModule);
      }
      return module;
    }
  }
}
