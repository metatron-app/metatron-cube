/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.java.util.common.lifecycle;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A manager of object Lifecycles.
 *
 * This object has methods for registering objects that should be started and stopped.  The Lifecycle allows for
 * two stages: Stage.NORMAL and Stage.LAST.
 *
 * Things added at Stage.NORMAL will be started first (in the order that they are added to the Lifecycle instance) and
 * then things added at Stage.LAST will be started.
 *
 * The close operation goes in reverse order, starting with the last thing added at Stage.LAST and working backwards.
 *
 * There are two sets of methods to add things to the Lifecycle.  One set that will just add instances and enforce that
 * start() has not been called yet.  The other set will add instances and, if the lifecycle is already started, start
 * them.
 */
public class Lifecycle
{
  private static final Logger log = new Logger(Lifecycle.class);

  private final Map<Stage, CopyOnWriteArrayList<Handler>> handlers;
  private final AtomicBoolean started = new AtomicBoolean(false);
  private final AtomicBoolean shutdownHookRegistered = new AtomicBoolean(false);
  private volatile Stage currStage = null;

  public static enum Stage
  {
    NORMAL,
    LAST
  }

  public Lifecycle()
  {
    handlers = Maps.newHashMap();
    for (Stage stage : Stage.values()) {
      handlers.put(stage, new CopyOnWriteArrayList<Handler>());
    }
  }

  /**
   * Adds a "managed" instance (annotated with {@link LifecycleStart} and {@link LifecycleStop}) to the Lifecycle at
   * Stage.NORMAL. If the lifecycle has already been started, it throws an {@link ISE}
   *
   * @param o The object to add to the lifecycle
   *
   * @throws ISE {@link Lifecycle#addHandler(Handler, Stage)}
   */
  public <T> T addManagedInstance(T o)
  {
    addHandler(new AnnotationBasedHandler(o));
    return o;
  }

  /**
   * Adds a "managed" instance (annotated with {@link LifecycleStart} and {@link LifecycleStop}) to the Lifecycle.
   * If the lifecycle has already been started, it throws an {@link ISE}
   *
   * @param o     The object to add to the lifecycle
   * @param stage The stage to add the lifecycle at
   *
   * @throws ISE {@link Lifecycle#addHandler(Handler, Stage)}
   */
  public <T> T addManagedInstance(T o, Stage stage)
  {
    addHandler(new AnnotationBasedHandler(o), stage);
    return o;
  }

  /**
   * Adds an instance with a start() and/or close() method to the Lifecycle at Stage.NORMAL.  If the lifecycle has
   * already been started, it throws an {@link ISE}
   *
   * @param o The object to add to the lifecycle
   *
   * @throws ISE {@link Lifecycle#addHandler(Handler, Stage)}
   */
  public <T> T addStartCloseInstance(T o)
  {
    addHandler(new StartCloseHandler(o));
    return o;
  }

  /**
   * Adds an instance with a start() and/or close() method to the Lifecycle.  If the lifecycle has already been started,
   * it throws an {@link ISE}
   *
   * @param o     The object to add to the lifecycle
   * @param stage The stage to add the lifecycle at
   *
   * @throws ISE {@link Lifecycle#addHandler(Handler, Stage)}
   */
  public <T> T addStartCloseInstance(T o, Stage stage)
  {
    addHandler(new StartCloseHandler(o), stage);
    return o;
  }

  /**
   * Adds a handler to the Lifecycle at the Stage.NORMAL stage. If the lifecycle has already been started, it throws
   * an {@link ISE}
   *
   * @param handler The hander to add to the lifecycle
   *
   * @throws ISE {@link Lifecycle#addHandler(Handler, Stage)}
   */
  public void addHandler(Handler handler)
  {
    addHandler(handler, Stage.NORMAL);
  }

  /**
   * Adds a handler to the Lifecycle. If the lifecycle has already been started, it throws an {@link ISE}
   *
   * @param handler The hander to add to the lifecycle
   * @param stage   The stage to add the lifecycle at
   *
   * @throws ISE indicates that the lifecycle has already been started and thus cannot be added to
   */
  public void addHandler(Handler handler, Stage stage)
  {
    synchronized (handlers) {
      if (started.get()) {
        throw new ISE("Cannot add a handler after the Lifecycle has started, it doesn't work that way.");
      }
      handlers.get(stage).add(handler);
    }
  }

  /**
   * Adds a "managed" instance (annotated with {@link LifecycleStart} and {@link LifecycleStop}) to the Lifecycle at
   * Stage.NORMAL and starts it if the lifecycle has already been started.
   *
   * @param o The object to add to the lifecycle
   *
   * @throws Exception {@link Lifecycle#addMaybeStartHandler(Handler, Stage)}
   */
  public <T> T addMaybeStartManagedInstance(T o) throws Exception
  {
    addMaybeStartHandler(new AnnotationBasedHandler(o));
    return o;
  }

  /**
   * Adds a "managed" instance (annotated with {@link LifecycleStart} and {@link LifecycleStop}) to the Lifecycle
   * and starts it if the lifecycle has already been started.
   *
   * @param o     The object to add to the lifecycle
   * @param stage The stage to add the lifecycle at
   *
   * @throws Exception {@link Lifecycle#addMaybeStartHandler(Handler, Stage)}
   */
  public <T> T addMaybeStartManagedInstance(T o, Stage stage) throws Exception
  {
    addMaybeStartHandler(new AnnotationBasedHandler(o), stage);
    return o;
  }

  /**
   * Adds an instance with a start() and/or close() method to the Lifecycle at Stage.NORMAL and starts it if the
   * lifecycle has already been started.
   *
   * @param o The object to add to the lifecycle
   *
   * @throws Exception {@link Lifecycle#addMaybeStartHandler(Handler, Stage)}
   */
  public <T> T addMaybeStartStartCloseInstance(T o) throws Exception
  {
    addMaybeStartHandler(new StartCloseHandler(o));
    return o;
  }

  /**
   * Adds an instance with a start() and/or close() method to the Lifecycle and starts it if the lifecycle has
   * already been started.
   *
   * @param o     The object to add to the lifecycle
   * @param stage The stage to add the lifecycle at
   *
   * @throws Exception {@link Lifecycle#addMaybeStartHandler(Handler, Stage)}
   */
  public <T> T addMaybeStartStartCloseInstance(T o, Stage stage) throws Exception
  {
    addMaybeStartHandler(new StartCloseHandler(o), stage);
    return o;
  }

  /**
   * Adds a handler to the Lifecycle at the Stage.NORMAL stage and starts it if the lifecycle has already been started.
   *
   * @param handler The hander to add to the lifecycle
   *
   * @throws Exception {@link Lifecycle#addMaybeStartHandler(Handler, Stage)}
   */
  public void addMaybeStartHandler(Handler handler) throws Exception
  {
    addMaybeStartHandler(handler, Stage.NORMAL);
  }

  /**
   * Adds a handler to the Lifecycle and starts it if the lifecycle has already been started.
   *
   * @param handler The hander to add to the lifecycle
   * @param stage   The stage to add the lifecycle at
   *
   * @throws Exception an exception thrown from handler.start().  If an exception is thrown, the handler is *not* added
   */
  public void addMaybeStartHandler(Handler handler, Stage stage) throws Exception
  {
    synchronized (handlers) {
      if (started.get()) {
        if (currStage == null || stage.compareTo(currStage) < 1) {
          handler.start();
        }
      }
      handlers.get(stage).add(handler);
    }
  }

  public void start() throws Exception
  {
    synchronized (handlers) {
      if (!started.compareAndSet(false, true)) {
        throw new ISE("Already started");
      }
      for (Stage stage : stagesOrdered()) {
        currStage = stage;
        for (Handler handler : handlers.get(stage)) {
          handler.start();
        }
      }
    }
  }

  public void stop()
  {
    synchronized (handlers) {
      if (!started.compareAndSet(true, false)) {
        log.info("Already stopped and stop was called. Silently skipping");
        return;
      }
      List<Exception> exceptions = Lists.newArrayList();

      for (Stage stage : Lists.reverse(stagesOrdered())) {
        final CopyOnWriteArrayList<Handler> stageHandlers = handlers.get(stage);
        final ListIterator<Handler> iter = stageHandlers.listIterator(stageHandlers.size());
        while (iter.hasPrevious()) {
          final Handler handler = iter.previous();
          try {
            handler.stop();
          }
          catch (Exception e) {
            log.warn(e, "exception thrown when stopping %s", handler);
            exceptions.add(e);
          }
        }
      }

      if (!exceptions.isEmpty()) {
        throw Throwables.propagate(exceptions.get(0));
      }
    }
  }

  public void ensureShutdownHook()
  {
    if (shutdownHookRegistered.compareAndSet(false, true)) {
      Runtime.getRuntime().addShutdownHook(
          new Thread(
              new Runnable()
              {
                @Override
                public void run()
                {
                  log.info("Running shutdown hook");
                  stop();
                }
              }
          )
      );
    }
  }

  public void join() throws InterruptedException
  {
    ensureShutdownHook();
    Thread.currentThread().join();
  }

  private static List<Stage> stagesOrdered()
  {
    return Arrays.asList(Stage.NORMAL, Stage.LAST);
  }


  public static interface Handler
  {
    public void start() throws Exception;

    public void stop();
  }

  private static class AnnotationBasedHandler implements Handler
  {
    private static final Logger log = new Logger(AnnotationBasedHandler.class);

    private final Object o;

    public AnnotationBasedHandler(Object o)
    {
      this.o = o;
    }

    @Override
    public void start() throws Exception
    {
      for (Method method : o.getClass().getMethods()) {
        if (method.getAnnotation(LifecycleStart.class) != null) {
          log.info("Invoking start method [%s] on object [%s].", method.getName(), o);
          method.invoke(o);
        }
      }
    }

    @Override
    public void stop()
    {
      for (Method method : o.getClass().getMethods()) {
        if (method.getAnnotation(LifecycleStop.class) != null) {
          log.info("Invoking stop method[%s] on object[%s].", method, o);
          try {
            method.invoke(o);
          }
          catch (Exception e) {
            log.error(e, "Exception when stopping method[%s] on object[%s]", method, o);
          }
        }
      }
    }
  }

  public boolean isStarted()
  {
    return started.get();
  }

  private static class StartCloseHandler implements Handler
  {
    private static final Logger log = new Logger(StartCloseHandler.class);

    private final Object o;
    private final Method startMethod;
    private final Method stopMethod;

    public StartCloseHandler(Object o)
    {
      this.o = o;
      try {
        startMethod = o.getClass().getMethod("start");
        stopMethod = o.getClass().getMethod("close");
      }
      catch (NoSuchMethodException e) {
        throw new RuntimeException(e);
      }
    }


    @Override
    public void start() throws Exception
    {
      log.info("Starting object[%s]", o);
      startMethod.invoke(o);
    }

    @Override
    public void stop()
    {
      log.info("Stopping object[%s]", o);
      try {
        stopMethod.invoke(o);
      }
      catch (Exception e) {
        log.error(e, "Unable to invoke stopMethod() on %s", o.getClass());
      }
    }
  }
}
