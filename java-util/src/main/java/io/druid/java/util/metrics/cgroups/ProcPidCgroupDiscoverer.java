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


package io.druid.java.util.metrics.cgroups;

import io.druid.java.util.metrics.PidDiscoverer;

import java.nio.file.Path;
import java.nio.file.Paths;

public class ProcPidCgroupDiscoverer implements CgroupDiscoverer
{
  private final ProcCgroupDiscoverer delegate;

  public ProcPidCgroupDiscoverer(PidDiscoverer pidDiscoverer)
  {
    delegate = new ProcCgroupDiscoverer(Paths.get("/proc", Long.toString(pidDiscoverer.getPid())));
  }

  @Override
  public Path discover(String cgroup)
  {
    return delegate.discover(cgroup);
  }
}
