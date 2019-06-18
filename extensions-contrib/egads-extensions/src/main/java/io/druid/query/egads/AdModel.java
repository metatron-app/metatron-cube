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

package io.druid.query.egads;

import com.google.common.collect.Lists;
import com.yahoo.egads.models.adm.AnomalyDetectionModel;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 */
public enum AdModel
{
  ExtremeLowDensityModel {
    @Override
    public List<Parameter> parameters()
    {
      List<Parameter> parameters = Lists.newArrayList(super.parameters());
      parameters.add(Parameter.optional("THRESHOLD", Map.class, null));
      return parameters;
    }
  },
  AdaptiveKernelDensityChangePointDetector {
    @Override
    public List<Parameter> parameters()
    {
      List<Parameter> parameters = Lists.newArrayList(super.parameters());
      parameters.add(Parameter.of("PRE_WINDOW_SIZE", int.class));
      parameters.add(Parameter.of("POST_WINDOW_SIZE", int.class));
      parameters.add(Parameter.optional("CONFIDENCE", float.class, 0.8F));
      return parameters;
    }
  },
  KSigmaModel {
    @Override
    public List<Parameter> parameters()
    {
      List<Parameter> parameters = Lists.newArrayList(super.parameters());
      parameters.add(Parameter.optional("THRESHOLD", Map.class, null));
      return parameters;
    }
  },
  NaiveModel {
    @Override
    public List<Parameter> parameters()
    {
      List<Parameter> parameters = Lists.newArrayList(super.parameters());
      parameters.add(Parameter.of("THRESHOLD", Map.class));
      parameters.add(Parameter.of("WINDOW_SIZE", float.class));
      return parameters;
    }
  },
  DBScanModel {
    @Override
    public List<Parameter> parameters()
    {
      List<Parameter> parameters = Lists.newArrayList(super.parameters());
      parameters.add(Parameter.optional("THRESHOLD", Map.class, null));
      return parameters;
    }
  },
  SimpleThresholdModel {
    @Override
    public List<Parameter> parameters()
    {
      List<Parameter> parameters = Lists.newArrayList(super.parameters());
      parameters.add(Parameter.optional("THRESHOLD", Map.class, null));
      parameters.add(Parameter.optional("SIMPLE_THRESHOLD_TYPE", String.class, "AdaptiveKSigmaSensitivity"));
      return parameters;
    }
  };

  public List<Parameter> parameters()
  {
    return Arrays.asList(
        Parameter.optional("MAX_ANOMALY_TIME_AGO", int.class, 999999999),
        Parameter.optional("DETECTION_WINDOW_START_TIME", long.class, 0)
    );
  }

  public AnomalyDetectionModel newInstance(Properties properties)
  {
    return Utils.newInstance(toClassName(), AnomalyDetectionModel.class, properties);
  }

  protected String toClassName()
  {
    return "com.yahoo.egads.models.adm." + name();
  }
}
