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

package io.druid.client;

import io.druid.server.coordination.DruidServerMetadata;
import io.druid.timeline.DataSegment;

import java.util.concurrent.Executor;
import java.util.function.BiFunction;

/**
 */
public interface ServerView
{
  void registerServerCallback(Executor exec, ServerCallback callback);
  void registerSegmentCallback(Executor exec, SegmentCallback callback);
  void removeSegmentCallback(SegmentCallback callback);

  enum CallbackAction
  {
    CONTINUE,
    UNREGISTER,
  }

  static interface ServerCallback
  {
    default CallbackAction serverAdded(DruidServer server)
    {
      return CallbackAction.CONTINUE;
    }

    /**
     * Called when a server is removed.
     *
     * The return value indicates if this callback has completed its work.  Note that even if this callback
     * indicates that it should be unregistered, it is not possible to guarantee that this callback will not
     * get called again.  There is a race condition between when this callback runs and other events that can cause
     * the callback to be queued for running.  Thus, callbacks shouldn't assume that they will not get called
     * again after they are done.  The contract is that the callback will eventually be unregistered, enforcing
     * a happens-before relationship is not part of the contract.
     *
     * @param server The server that was removed.
     * @return UNREGISTER if the callback has completed its work and should be unregistered.  CONTINUE if the callback
     * should remain registered.
     */
    default CallbackAction serverRemoved(DruidServer server)
    {
      return CallbackAction.CONTINUE;
    }

    default CallbackAction serverUpdated(DruidServer prev, DruidServer current)
    {
      return CallbackAction.CONTINUE;
    }
  }

  static interface SegmentCallback
  {
    /**
     * Called when a segment is added to a server.
     *
     * The return value indicates if this callback has completed its work.  Note that even if this callback
     * indicates that it should be unregistered, it is not possible to guarantee that this callback will not
     * get called again.  There is a race condition between when this callback runs and other events that can cause
     * the callback to be queued for running.  Thus, callbacks shouldn't assume that they will not get called
     * again after they are done.  The contract is that the callback will eventually be unregistered, enforcing
     * a happens-before relationship is not part of the contract.
     *
     * @param server The server that added a segment
     * @param segment The segment that was added
     * @return UNREGISTER if the callback has completed its work and should be unregistered.  CONTINUE if the callback
     * should remain registered.
     */
    default CallbackAction segmentAdded(DruidServerMetadata server, DataSegment segment)
    {
      return CallbackAction.CONTINUE;
    }

    /**
     * Called when a segment is removed from a server.
     *
     * The return value indicates if this callback has completed its work.  Note that even if this callback
     * indicates that it should be unregistered, it is not possible to guarantee that this callback will not
     * get called again.  There is a race condition between when this callback runs and other events that can cause
     * the callback to be queued for running.  Thus, callbacks shouldn't assume that they will not get called
     * again after they are done.  The contract is that the callback will eventually be unregistered, enforcing
     * a happens-before relationship is not part of the contract.
     *
     * @param server The server that removed a segment
     * @param segment The segment that was removed
     * @return UNREGISTER if the callback has completed its work and should be unregistered.  CONTINUE if the callback
     * should remain registered.
     */
    default CallbackAction segmentRemoved(DruidServerMetadata server, DataSegment segment)
    {
      return CallbackAction.CONTINUE;
    }

    default CallbackAction segmentViewInitialized()
    {
      return CallbackAction.CONTINUE;
    }
  }

  static SegmentCallback segmentAdded(BiFunction<DruidServerMetadata, DataSegment, CallbackAction> function)
  {
    return new SegmentCallback()
    {
      @Override
      public CallbackAction segmentAdded(DruidServerMetadata server, DataSegment segment)
      {
        return function.apply(server, segment);
      }
    };
  }
}
