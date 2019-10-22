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

package io.druid.java.util.metrics;

import com.google.common.base.Preconditions;
import io.druid.java.util.emitter.service.ServiceMetricEvent;

public abstract class FeedDefiningMonitor extends AbstractMonitor
{
  public static final String DEFAULT_METRICS_FEED = "metrics";
  protected final String feed;

  public FeedDefiningMonitor(String feed)
  {
    Preconditions.checkNotNull(feed);
    this.feed = feed;
  }

  protected ServiceMetricEvent.Builder builder()
  {
    return ServiceMetricEvent.builder().setFeed(feed);
  }
}
