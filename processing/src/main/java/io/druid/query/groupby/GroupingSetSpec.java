/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.groupby;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;

import java.util.Arrays;
import java.util.List;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = GroupingSetSpec.Names.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "names", value = GroupingSetSpec.Names.class),
    @JsonSubTypes.Type(name = "indices", value = GroupingSetSpec.Indices.class),
    @JsonSubTypes.Type(name = "ids", value = GroupingSetSpec.Ids.class),
    @JsonSubTypes.Type(name = "rollup", value = GroupingSetSpec.Rollup.class),
})
public interface GroupingSetSpec
{
  void validate(List<String> dimensions);

  int[][] getGroupings(List<String> dimensions);

  boolean isEmpty();

  public static class Names implements GroupingSetSpec
  {
    private final List<List<String>> names;

    @JsonCreator
    public Names(@JsonProperty("names") List<List<String>> names)
    {
      this.names = names == null ? ImmutableList.<List<String>>of() : names;
    }

    @JsonProperty
    public List<List<String>> getNames()
    {
      return names;
    }

    @Override
    public void validate(List<String> dimensions)
    {
      for (List<String> group : names) {
        Preconditions.checkArgument(dimensions.containsAll(group), "invalid column in grouping set " + group);
        Preconditions.checkArgument(group.size() == Sets.newHashSet(group).size(), "duplicated columns in " + group);
        int prev = -1;
        for (String dimension : group) {
          int index = dimensions.indexOf(dimension);
          Preconditions.checkArgument(prev < 0 || index > prev, "invalid grouping set " + group);
          prev = index;
        }
      }
    }

    @Override
    public int[][] getGroupings(List<String> dimensions)
    {
      int[][] groupings = new int[names.size()][];
      for (int i = 0; i < groupings.length; i++) {
        List<String> groupingSet = names.get(i);
        groupings[i] = new int[groupingSet.size()];
        for (int j = 0; j < groupings[i].length; j++) {
          groupings[i][j] = dimensions.indexOf(groupingSet.get(j));
          Preconditions.checkArgument(groupings[i][j] >= 0, "invalid column " + groupingSet.get(j));
        }
      }
      return groupings;
    }

    @Override
    public boolean isEmpty()
    {
      return names.isEmpty();
    }

    @Override
    public boolean equals(Object o)
    {
      return o instanceof Names && names.equals(((Names) o).names);
    }

    @Override
    public String toString()
    {
      return "Names{" +
             "names=" + names +
             '}';
    }

    public static class Builder
    {
      List<List<String>> groups = Lists.newArrayList();

      public Builder add(List<String> group)
      {
        groups.add(group);
        return this;
      }

      public Builder add(String... group)
      {
        groups.add(Arrays.asList(group));
        return this;
      }

      Names build()
      {
        return new Names(groups);
      }
    }
  }

  public static class Indices implements GroupingSetSpec
  {
    private final List<List<Integer>> indices;

    @JsonCreator
    public Indices(@JsonProperty("indices") List<List<Integer>> indices)
    {
      this.indices = indices == null ? ImmutableList.<List<Integer>>of() : indices;
    }

    @JsonProperty
    public List<List<Integer>> getIndices()
    {
      return indices;
    }

    @Override
    public void validate(List<String> dimensions)
    {
      for (List<Integer> group : indices) {
        int prev = -1;
        for (Integer index : group) {
          Preconditions.checkArgument(prev < 0 || index > prev, "invalid grouping set " + group);
          prev = index;
        }
      }
    }

    @Override
    public int[][] getGroupings(List<String> dimensions)
    {
      int[][] groupings = new int[indices.size()][];
      for (int i = 0; i < groupings.length; i++) {
        List<Integer> groupingSet = indices.get(i);
        groupings[i] = new int[groupingSet.size()];
        for (int j = 0; j < groupings[i].length; j++) {
          groupings[i][j] = groupingSet.get(j);
          Preconditions.checkArgument(
              groupings[i][j] >= 0 && groupings[i][j] < dimensions.size(),
              "invalid index " + groupingSet.get(j)
          );
        }
      }
      return groupings;
    }

    @Override
    public boolean isEmpty()
    {
      return indices.isEmpty();
    }

    @Override
    public boolean equals(Object o)
    {
      return o instanceof Indices && indices.equals(((Indices) o).indices);
    }

    @Override
    public String toString()
    {
      return "Indices{" +
             "indices=" + indices +
             '}';
    }

    public static class Builder
    {
      List<List<Integer>> groups = Lists.newArrayList();

      public Builder add(List<Integer> group)
      {
        groups.add(group);
        return this;
      }

      public Builder add(Integer... group)
      {
        groups.add(Arrays.asList(group));
        return this;
      }

      Indices build()
      {
        return new Indices(groups);
      }
    }
  }

  public static class Ids implements GroupingSetSpec
  {
    private final List<Integer> ids;

    @JsonCreator
    public Ids(@JsonProperty("ids") List<Integer> ids)
    {
      this.ids = ids == null ? ImmutableList.<Integer>of() : ids;
    }

    @JsonProperty
    public List<Integer> getIds()
    {
      return ids;
    }

    @Override
    public void validate(List<String> dimensions)
    {
      int max = (int) Math.pow(2, dimensions.size());
      for (Integer group : ids) {
        Preconditions.checkArgument(group >= 0 || group < max, "invalid grouping set " + group);
      }
    }

    @Override
    public int[][] getGroupings(List<String> dimensions)
    {
      int[][] groupings = new int[ids.size()][];
      for (int i = 0; i < groupings.length; i++) {
        int groupId = ids.get(i);
        List<Integer> bitset = Lists.newArrayList();
        for (int j = 0; groupId > 0; groupId >>>= 1, j++) {
          if ((groupId & 0x01) != 0) {
            bitset.add(j);
          }
        }
        groupings[i] = Ints.toArray(bitset);
      }
      return groupings;
    }

    @Override
    public boolean isEmpty()
    {
      return ids.isEmpty();
    }

    @Override
    public boolean equals(Object o)
    {
      return o instanceof Ids && ids.equals(((Ids) o).ids);
    }

    @Override
    public String toString()
    {
      return "Ids{" +
             "ids=" + ids +
             '}';
    }
  }

  public static class Rollup implements GroupingSetSpec
  {
    @JsonCreator
    public Rollup()
    {
    }

    @Override
    public void validate(List<String> dimensions)
    {
    }

    @Override
    public int[][] getGroupings(List<String> dimensions)
    {
      final int length = dimensions.size();
      int[][] groups = new int[length + 1][];
      for (int i = 0; i < length; i++) {
        groups[i] = new int[length - i];
        for (int j = 0; j < groups[i].length; j++) {
          groups[i][j] = j;
        }
      }
      groups[length] = new int[0];
      return groups;
    }

    @Override
    public boolean isEmpty()
    {
      return false;
    }

    @Override
    public boolean equals(Object o)
    {
      return o instanceof Rollup;
    }

    @Override
    public String toString()
    {
      return "Rollup{}";
    }
  }
}
