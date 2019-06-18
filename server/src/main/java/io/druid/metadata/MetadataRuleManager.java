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

package io.druid.metadata;

import io.druid.audit.AuditInfo;
import io.druid.server.coordinator.rules.Rule;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 */
public interface MetadataRuleManager
{
  public void start();

  public void stop();

  public void poll();

  public Map<String, List<Rule>> getAllRules();

  public List<Rule> getRules(final String dataSource);

  public List<Rule> getRulesWithDefault(final String dataSource);

  public boolean overrideRule(final String dataSource, final List<Rule> rulesConfig, final AuditInfo auditInfo);

  abstract class Abstract implements MetadataRuleManager {

    @Override
    public void start() {}

    @Override
    public void stop() {}

    @Override
    public void poll() {}

    @Override
    public Map<String, List<Rule>> getAllRules() { return Collections.emptyMap();}

    @Override
    public List<Rule> getRules(String dataSource) { return getRulesWithDefault(dataSource);}

    @Override
    public List<Rule> getRulesWithDefault(String dataSource) { return Collections.emptyList();}

    @Override
    public boolean overrideRule(String dataSource, List<Rule> rulesConfig, AuditInfo auditInfo) { return false;}
  }
}
