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

package io.druid.query.aggregation.corr;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableSet;
import io.druid.common.guava.GuavaUtils;
import io.druid.data.TypeResolver;
import io.druid.data.ValueDesc;
import io.druid.query.aggregation.Description;
import io.druid.query.aggregation.PostAggregator;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;

/**
 * This is ported from apache hive (GenericUDAFBinarySetFunctions)
 */
public class Regressions
{
  static abstract class RegrBase extends PostAggregator.Stateless
  {
    final String name;
    final String fieldName;

    public RegrBase(String name, String fieldName)
    {
      this.name = name;
      this.fieldName = fieldName;
    }

    @Override
    public String getName()
    {
      return name;
    }

    @Override
    public Set<String> getDependentFields()
    {
      return ImmutableSet.of(fieldName);
    }

    @Override
    public Comparator getComparator()
    {
      return GuavaUtils.NULL_FIRST_NATURAL;
    }

    @Override
    public ValueDesc resolve(TypeResolver bindings)
    {
      return ValueDesc.DOUBLE;
    }

    abstract class BaseProcessor extends AbstractProcessor
    {
      @Override
      public Object compute(DateTime timestamp, Map<String, Object> combinedAggregators)
      {
        final Object value = combinedAggregators.get(fieldName);
        return value instanceof PearsonAggregatorCollector ? calculate((PearsonAggregatorCollector) value) : null;
      }

      abstract Object calculate(PearsonAggregatorCollector corr);
    }
  }

  @JsonTypeName("regr_slope")
  @Description(
      name = "regr_slope",
      value = "_FUNC_(y,x) - returns the slope of the linear regression line",
      extended = "The function takes as arguments any pair of numeric types and returns a double.\n" +
                 "Any pair with a NULL is ignored.\n" +
                 "If applied to an empty set: NULL is returned.\n" +
                 "If N*SUM(x*x) = SUM(x)*SUM(x): NULL is returned (the fit would be a vertical).\n" +
                 "Otherwise, it computes the following:\n" +
                 "   (N*SUM(x*y)-SUM(x)*SUM(y)) / (N*SUM(x*x)-SUM(x)*SUM(x))")
  public static class RegrSlope extends RegrBase
  {
    @JsonCreator
    public RegrSlope(
        @JsonProperty("name") String name,
        @JsonProperty("fieldName") String fieldName
    )
    {
      super(name, fieldName);
    }

    @Override
    protected Processor createStateless()
    {
      return new BaseProcessor()
      {
        @Override
        public Double calculate(PearsonAggregatorCollector corr)
        {
          if (corr.count < 2 || corr.xvar == 0.0d) {
            return null;
          } else {
            return corr.covar / corr.xvar;
          }
        }
      };
    }
  }

  @JsonTypeName("regr_r2")
  @Description(
      name = "regr_r2",
      value = "_FUNC_(y,x) - returns the coefficient of determination (also called R-squared or goodness of fit) for the regression line.",
      extended = "The function takes as arguments any pair of numeric types and returns a double.\n" +
                 "Any pair with a NULL is ignored.\n" +
                 "If applied to an empty set: NULL is returned.\n" +
                 "If N*SUM(x*x) = SUM(x)*SUM(x): NULL is returned.\n" +
                 "If N*SUM(y*y) = SUM(y)*SUM(y): 1 is returned.\n" +
                 "Otherwise, it computes the following:\n" +
                 "   POWER( N*SUM(x*y)-SUM(x)*SUM(y) ,2)  /  ( (N*SUM(x*x)-SUM(x)*SUM(x)) * (N*SUM(y*y)-SUM(y)*SUM(y)) )")
  public static class RegrR2 extends RegrBase
  {
    @JsonCreator
    public RegrR2(
        @JsonProperty("name") String name,
        @JsonProperty("fieldName") String fieldName
    )
    {
      super(name, fieldName);
    }

    @Override
    protected Processor createStateless()
    {
      return new BaseProcessor()
      {
        @Override
        public Double calculate(PearsonAggregatorCollector corr)
        {
          if (corr.count < 2 || corr.xvar == 0.0d) {
            return null;
          } else if (corr.yvar == 0.0d) {
            return 1.0d;
          } else {
            return corr.covar * corr.covar / corr.xvar / corr.yvar;
          }
        }
      };
    }
  }

  @JsonTypeName("regr_sxy")
  @Description(name = "regr_sxy",
      value = "_FUNC_(y,x) - return a value that can be used to evaluate the statistical validity of a regression model.",
      extended = "The function takes as arguments any pair of numeric types and returns a double.\n" +
                 "Any pair with a NULL is ignored.\n" +
                 "If applied to an empty set: NULL is returned.\n" +
                 "If N*SUM(x*x) = SUM(x)*SUM(x): NULL is returned.\n" +
                 "Otherwise, it computes the following:\n" +
                 "   SUM(x*y)-SUM(x)*SUM(y)/N")
  public static class RegrSXY extends RegrBase
  {
    @JsonCreator
    public RegrSXY(
        @JsonProperty("name") String name,
        @JsonProperty("fieldName") String fieldName
    )
    {
      super(name, fieldName);
    }

    @Override
    protected PostAggregator.Processor createStateless()
    {
      return new BaseProcessor()
      {
        @Override
        public Double calculate(PearsonAggregatorCollector corr)
        {
          return corr.count == 0 ? null : corr.covar;
        }
      };
    }
  }

  @JsonTypeName("regr_intercept")
  @Description(name = "regr_intercept",
      value = "_FUNC_(y,x) - returns the y-intercept of the regression line.",
      extended = "The function takes as arguments any pair of numeric types and returns a double.\n" +
                 "Any pair with a NULL is ignored.\n" +
                 "If applied to an empty set: NULL is returned.\n" +
                 "If N*SUM(x*x) = SUM(x)*SUM(x): NULL is returned.\n" +
                 "Otherwise, it computes the following:\n" +
                 "   ( SUM(y)*SUM(x*x)-SUM(X)*SUM(x*y) )  /  ( N*SUM(x*x)-SUM(x)*SUM(x) )")
  public static class RegrIntercept extends RegrBase
  {
    @JsonCreator
    public RegrIntercept(
        @JsonProperty("name") String name,
        @JsonProperty("fieldName") String fieldName
    )
    {
      super(name, fieldName);
    }

    @Override
    protected PostAggregator.Processor createStateless()
    {
      return new BaseProcessor()
      {
        @Override
        public Double calculate(PearsonAggregatorCollector corr)
        {
          if (corr.count == 0 || corr.xvar == 0.0d) {
            return null;
          } else {
            final double slope = corr.covar / corr.xvar;
            return corr.yavg - slope * corr.xavg;
          }
        }
      };
    }
  }

  @JsonTypeName("regr_sxx")
  @Description(name = "regr_sxx",
      value = "_FUNC_(y,x) - auxiliary analytic function",
      extended = "The function takes as arguments any pair of numeric types and returns a double.\n" +
                 "Any pair with a NULL is ignored.\n" +
                 "If applied to an empty set: NULL is returned.\n" +
                 "Otherwise, it computes the following:\n" +
                 "   SUM(x*x)-SUM(x)*SUM(x)/N\n")
  public static class RegrSXX extends RegrBase
  {
    @JsonCreator
    public RegrSXX(
        @JsonProperty("name") String name,
        @JsonProperty("fieldName") String fieldName
    )
    {
      super(name, fieldName);
    }

    @Override
    protected PostAggregator.Processor createStateless()
    {
      return new BaseProcessor()
      {
        @Override
        public Double calculate(PearsonAggregatorCollector corr)
        {
          return corr.count == 0 ? null : corr.xvar;
        }
      };
    }
  }

  @JsonTypeName("regr_syy")
  @Description(
      name = "regr_syy",
      value = "_FUNC_(y,x) - auxiliary analytic function",
      extended = "The function takes as arguments any pair of numeric types and returns a double.\n" +
                 "Any pair with a NULL is ignored.\n" +
                 "If applied to an empty set: NULL is returned.\n" +
                 "Otherwise, it computes the following:\n" +
                 "   SUM(y*y)-SUM(y)*SUM(y)/N\n")
  public static class RegrSYY extends RegrBase
  {
    @JsonCreator
    public RegrSYY(
        @JsonProperty("name") String name,
        @JsonProperty("fieldName") String fieldName
    )
    {
      super(name, fieldName);
    }

    @Override
    protected PostAggregator.Processor createStateless()
    {
      return new BaseProcessor()
      {
        @Override
        public Double calculate(PearsonAggregatorCollector corr)
        {
          return corr.count == 0 ? null : corr.yvar;
        }
      };
    }
  }

  @JsonTypeName("regr_avgx")
  public static class RegrAvgX extends RegrBase
  {
    @JsonCreator
    public RegrAvgX(
        @JsonProperty("name") String name,
        @JsonProperty("fieldName") String fieldName
    )
    {
      super(name, fieldName);
    }

    @Override
    protected PostAggregator.Processor createStateless()
    {
      return new BaseProcessor()
      {
        @Override
        public Double calculate(PearsonAggregatorCollector corr)
        {
          return corr.count == 0 ? null : corr.xavg;
        }
      };
    }
  }

  @JsonTypeName("regr_avgy")
  public static class RegrAvgY extends RegrBase
  {
    @JsonCreator
    public RegrAvgY(
        @JsonProperty("name") String name,
        @JsonProperty("fieldName") String fieldName
    )
    {
      super(name, fieldName);
    }

    @Override
    protected PostAggregator.Processor createStateless()
    {
      return new BaseProcessor()
      {
        @Override
        public Double calculate(PearsonAggregatorCollector corr)
        {
          return corr.count == 0 ? null : corr.yavg;
        }
      };
    }
  }

  @JsonTypeName("regr_count")
  public static class RegrCount extends RegrBase
  {
    @JsonCreator
    public RegrCount(
        @JsonProperty("name") String name,
        @JsonProperty("fieldName") String fieldName
    )
    {
      super(name, fieldName);
    }

    @Override
    public ValueDesc resolve(TypeResolver bindings)
    {
      return ValueDesc.LONG;
    }

    @Override
    protected PostAggregator.Processor createStateless()
    {
      return new BaseProcessor()
      {
        @Override
        public Long calculate(PearsonAggregatorCollector corr)
        {
          return corr.count;
        }
      };
    }
  }
}
