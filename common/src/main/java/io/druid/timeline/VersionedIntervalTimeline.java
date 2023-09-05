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

package io.druid.timeline;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.common.Intervals;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.guava.KVArraySortedMap;
import io.druid.common.utils.JodaUtils;
import io.druid.java.util.common.UOE;
import io.druid.timeline.partition.ImmutablePartitionHolder;
import io.druid.timeline.partition.PartitionChunk;
import io.druid.timeline.partition.PartitionHolder;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * VersionedIntervalTimeline is a data structure that manages objects on a specific timeline.
 *
 * It associates a jodatime Interval and a generically-typed version with the object that is being stored.
 *
 * In the event of overlapping timeline entries, timeline intervals may be chunked. The underlying data associated
 * with a timeline entry remains unchanged when chunking occurs.
 *
 * After loading objects via the add() method, the lookup(Interval) method can be used to get the list of the most
 * recent objects (according to the version) that match the given interval.  The intent is that objects represent
 * a certain time period and when you do a lookup(), you are asking for all the objects that you need to look
 * at in order to get a correct answer about that time period.
 *
 * The findOvershadowed() method returns a list of objects that will never be returned by a call to lookup() because
 * they are overshadowed by some other object.  This can be used in conjunction with the add() and remove() methods
 * to achieve "atomic" updates.  First add new items, then check if those items caused anything to be overshadowed, if
 * so, remove the overshadowed elements, and you have effectively updated your data set without any user impact.
 */
public class VersionedIntervalTimeline<ObjectType> implements TimelineLookup<ObjectType>
{
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

  final NavigableMap<Interval, TimelineEntry> completePartitions = new TreeMap<Interval, TimelineEntry>(
      JodaUtils.intervalsByStartThenEnd()
  );
  final NavigableMap<Interval, TimelineEntry> incompletePartitions = new TreeMap<Interval, TimelineEntry>(
      JodaUtils.intervalsByStartThenEnd()
  );
  private final Map<Interval, KVArraySortedMap<String, TimelineEntry>> allTimelineEntries = Maps.newHashMap();

  public Interval coverage()
  {
    Interval from1 = Iterables.getFirst(completePartitions.keySet(), null);
    Interval from2 = Iterables.getFirst(incompletePartitions.keySet(), null);
    if (from1 == null && from2 == null) {
      return null;
    }
    long from;
    if (from1 == null) {
      from = from2.getStartMillis();
    } else if (from2 == null) {
      from = from1.getStartMillis();
    } else {
      from = Math.min(from1.getStartMillis(), from2.getStartMillis());
    }
    Interval to1 = completePartitions.lastKey();
    Interval to2 = incompletePartitions.lastKey();
    long to;
    if (to1 == null) {
      to = to2.getEndMillis();
    } else if (from2 == null) {
      to = to1.getEndMillis();
    } else {
      to = Math.max(to1.getEndMillis(), to2.getEndMillis());
    }
    return new Interval(from, to);
  }

  public List<PartitionChunk<ObjectType>> clear()
  {
    List<PartitionChunk<ObjectType>> chunks = Lists.newArrayList();
    for (KVArraySortedMap<String, TimelineEntry> map : allTimelineEntries.values()) {
      for (TimelineEntry entry : map.values()) {
        Iterables.addAll(chunks, entry.partitionHolder);
      }
    }
    incompletePartitions.clear();
    completePartitions.clear();
    allTimelineEntries.clear();
    return chunks;
  }

  public List<ObjectType> getAll()
  {
    List<ObjectType> objects = Lists.newArrayList();
    for (KVArraySortedMap<String, TimelineEntry> map : allTimelineEntries.values()) {
      for (TimelineEntry entry : map.values()) {
        Iterables.addAll(objects, entry.partitionHolder.payloads());
      }
    }
    return objects;
  }

  public boolean isEmpty()
  {
    return allTimelineEntries.isEmpty();
  }

  public void add(final Interval interval, String version, PartitionChunk<ObjectType> object)
  {
    lock.writeLock().lock();
    try {
      KVArraySortedMap<String, TimelineEntry> exists = allTimelineEntries.get(interval);

      TimelineEntry entry;
      if (exists == null) {
        entry = new TimelineEntry(interval, version, new PartitionHolder<ObjectType>(object));
        allTimelineEntries.put(interval, KVArraySortedMap.incremental(version, entry, 2));
      } else {
        entry = exists.get(version);

        if (entry == null) {
          entry = new TimelineEntry(interval, version, new PartitionHolder<ObjectType>(object));
          exists.put(version, entry);
        } else {
          entry.partitionHolder.add(object);
        }
      }

      if (entry.partitionHolder.isComplete()) {
        add(completePartitions, interval, entry);
      }

      add(incompletePartitions, interval, entry);
    }
    finally {
      lock.writeLock().unlock();
    }
  }

  public PartitionChunk<ObjectType> remove(Interval interval, String version, PartitionChunk<ObjectType> chunk)
  {
    lock.writeLock().lock();
    try {
      Map<String, TimelineEntry> versionEntries = allTimelineEntries.get(interval);
      if (versionEntries == null) {
        return null;
      }

      TimelineEntry entry = versionEntries.get(version);
      if (entry == null) {
        return null;
      }

      PartitionHolder<ObjectType> holder = entry.partitionHolder;
      PartitionChunk<ObjectType> retVal = holder.remove(chunk);
      if (holder.isEmpty()) {
        versionEntries.remove(version);
        if (versionEntries.isEmpty()) {
          allTimelineEntries.remove(interval);
        }
        remove(incompletePartitions, interval, entry, true);
      }

      remove(completePartitions, interval, entry, false);

      return retVal;
    }
    finally {
      lock.writeLock().unlock();
    }
  }

  public <T> T find(Function<Map.Entry<Interval, KVArraySortedMap<String, TimelineEntry>>, T> finder)
  {
    lock.readLock().lock();
    try {
      for (Map.Entry<Interval, KVArraySortedMap<String, TimelineEntry>> entry : allTimelineEntries.entrySet()) {
        T found = finder.apply(entry);
        if (found != null) {
          return found;
        }
      }
    }
    finally {
      lock.readLock().unlock();
    }
    return null;
  }

  @Override
  public PartitionHolder<ObjectType> findEntry(Interval interval, String version)
  {
    return findEntry(interval, version, e -> new ImmutablePartitionHolder<ObjectType>(e.partitionHolder));
  }

  public TimelineObjectHolder<ObjectType> findTimeline(Interval interval, String version)
  {
    return findEntry(interval, version, e -> new TimelineObjectHolder<ObjectType>(interval, version, e.partitionHolder));
  }

  private <V> V findEntry(Interval interval, String version, Function<TimelineEntry, V> function)
  {
    lock.readLock().lock();
    try {
      KVArraySortedMap<String, TimelineEntry> entryMap = allTimelineEntries.get(interval);
      if (entryMap != null) {
        TimelineEntry foundEntry = entryMap.get(version);
        if (foundEntry != null) {
          return function.apply(foundEntry);
        }
      }
      for (Map.Entry<Interval, KVArraySortedMap<String, TimelineEntry>> entry : allTimelineEntries.entrySet()) {
        if (entry.getKey().contains(interval)) {
          TimelineEntry foundEntry = entry.getValue().get(version);
          if (foundEntry != null) {
            return function.apply(foundEntry);
          }
        }
      }
      return null;
    }
    finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Does a lookup for the objects representing the given time interval.  Will *only* return
   * PartitionHolders that are complete.
   *
   * @param interval interval to find objects for
   *
   * @return Holders representing the interval that the objects exist for, PartitionHolders
   *         are guaranteed to be complete
   */
  @Override
  public List<TimelineObjectHolder<ObjectType>> lookup(Interval interval)
  {
    return lookup(interval, false);
  }

  @Override
  public Iterable<TimelineObjectHolder<ObjectType>> lookupWithIncompletePartitions(Interval interval)
  {
    return lookup(interval, true);
  }

  private List<TimelineObjectHolder<ObjectType>> lookup(Interval interval, boolean incompleteOk)
  {
    List<TimelineObjectHolder<ObjectType>> retVal = _lookup(interval, incompleteOk ? incompletePartitions : completePartitions);

    // this trimming thing is very stupid idea. but it infested over all kind of modules, let it in that way.
    if (retVal.isEmpty()) {
      return retVal;
    }

    if (retVal.size() == 1) {
      return Arrays.asList(retVal.get(0).withOverlap(interval));
    }

    TimelineObjectHolder<ObjectType> firstEntry = retVal.get(0);
    if (interval.getStartMillis() > firstEntry.getInterval().getStartMillis()) {
      retVal.set(0, firstEntry.withOverlap(interval.getStartMillis(), Long.MAX_VALUE));
    }

    TimelineObjectHolder<ObjectType> lastEntry = retVal.get(retVal.size() - 1);
    if (interval.getEndMillis() < lastEntry.getInterval().getEndMillis()) {
      retVal.set(retVal.size() - 1, lastEntry.withOverlap(Long.MIN_VALUE, interval.getEndMillis()));
    }

    return retVal;
  }

  private List<TimelineObjectHolder<ObjectType>> _lookup(Interval interval, Map<Interval, TimelineEntry> timeline)
  {
    lock.readLock().lock();
    try {
      return Lists.newArrayList(
          GuavaUtils.transform(Maps.filterKeys(timeline, k -> k.overlaps(interval)), (k, v) -> v.toPublic(k))
      );
    }
    finally {
      lock.readLock().unlock();
    }
  }

  public Set<TimelineObjectHolder<ObjectType>> findOvershadowed()
  {
    Map<Interval, Map<String, TimelineEntry>> snapshot = Maps.newHashMapWithExpectedSize(allTimelineEntries.size());

    lock.readLock().lock();
    try {
      for (Map.Entry<Interval, KVArraySortedMap<String, TimelineEntry>> versionEntry : allTimelineEntries.entrySet()) {
        snapshot.put(versionEntry.getKey(), Maps.newHashMap(versionEntry.getValue()));
      }
      for (TimelineEntry entry : Iterables.concat(completePartitions.values(), incompletePartitions.values())) {
        snapshot.computeIfPresent(
            entry.trueInterval, (k, v) -> v.remove(entry.version) != null && v.isEmpty() ? null : v
        );
      }
    }
    finally {
      lock.readLock().unlock();
    }

    return Sets.newHashSet(GuavaUtils.explode(
        snapshot.values(), timeline -> Iterables.transform(timeline.values(), TimelineEntry::toPublic)
    ));
  }

  @VisibleForTesting
  public boolean isOvershadowed(Interval interval, String version)
  {
    try {
      lock.readLock().lock();

      TimelineEntry entry = completePartitions.get(interval);
      if (entry != null) {
        return version.compareTo(entry.version) < 0;
      }

      Interval lower = completePartitions.floorKey(
          new Interval(interval.getStartMillis(), JodaUtils.MAX_INSTANT)
      );

      if (lower == null || !lower.overlaps(interval)) {
        return false;
      }

      Interval prev = null;
      Interval curr = lower;

      do {
        if (curr == null ||  //no further keys
            (prev != null && curr.getStartMillis() > prev.getEndMillis()) || //a discontinuity
            //lower or same version
            version.compareTo(completePartitions.get(curr).version) >= 0) {
          return false;
        }

        prev = curr;
        curr = completePartitions.higherKey(curr);

      } while (interval.getEndMillis() > prev.getEndMillis());

      return true;
    }
    finally {
      lock.readLock().unlock();
    }
  }

  private void add(NavigableMap<Interval, TimelineEntry> timeline, Interval interval, TimelineEntry entry)
  {
    TimelineEntry existsInTimeline = timeline.get(interval);

    if (existsInTimeline != null) {
      int compare = entry.version.compareTo(existsInTimeline.version);
      if (compare > 0) {
        addIntervalToTimeline(interval, entry, timeline);
      }
      return;
    }

    Interval lowerKey = timeline.lowerKey(interval);

    if (lowerKey != null) {
      if (addAtKey(timeline, lowerKey, entry)) {
        return;
      }
    }

    Interval higherKey = timeline.higherKey(interval);

    if (higherKey != null) {
      if (addAtKey(timeline, higherKey, entry)) {
        return;
      }
    }

    addIntervalToTimeline(interval, entry, timeline);
  }

  /**
   *
   * @param timeline
   * @param key
   * @param entry
   * @return boolean flag indicating whether we inserted or discarded something
   */
  private boolean addAtKey(
      final NavigableMap<Interval, TimelineEntry> timeline,
      final Interval key,
      final TimelineEntry entry
  )
  {
    if (!key.overlaps(entry.trueInterval)) {
      return false;
    }

    boolean retVal = false;
    Interval currKey = key;
    Interval entryInterval = entry.trueInterval;

    while (entryInterval != null && currKey != null && currKey.overlaps(entryInterval)) {

      Interval nextKey = timeline.higherKey(currKey);
      TimelineEntry currEntry = timeline.get(currKey);

      int versionCompare = entry.version.compareTo(currEntry.version);

      if (versionCompare < 0) {
        if (currKey.contains(entryInterval)) {
          return true;
        }
        if (currKey.getStartMillis() > entryInterval.getStartMillis()) {
          addIntervalToTimeline(Intervals.aheadOf(entryInterval, currKey), entry, timeline);
        }
        if (entryInterval.getEndMillis() > currKey.getEndMillis()) {
          entryInterval = Intervals.behindOf(entryInterval, currKey);
        } else {
          entryInterval = null; // discard this entry
        }
      } else if (versionCompare > 0) {

        TimelineEntry oldEntry = timeline.remove(currKey);

        if (currKey.contains(entryInterval)) {
          addIntervalToTimeline(Intervals.aheadOf(currKey, entryInterval), oldEntry, timeline);
          addIntervalToTimeline(Intervals.behindOf(currKey, entryInterval), oldEntry, timeline);
          addIntervalToTimeline(entryInterval, entry, timeline);

          return true;  // no need to go further
        }
        if (currKey.getStartMillis() < entryInterval.getStartMillis()) {
          addIntervalToTimeline(Intervals.aheadOf(currKey, entryInterval), oldEntry, timeline);
        } else if (currKey.getEndMillis() > entryInterval.getEndMillis()) {
          addIntervalToTimeline(Intervals.behindOf(currKey, entryInterval), oldEntry, timeline);
        }
      } else {
        if (currEntry.equals(entry)) {
          // This occurs when restoring segments
          timeline.remove(currKey);
        } else {
          throw new UOE(
              "Cannot add overlapping segments [%s and %s] with the same version [%s]",
              currKey,
              entryInterval,
              entry.version
          );
        }
      }

      currKey = nextKey;
      retVal = true;
    }

    addIntervalToTimeline(entryInterval, entry, timeline);

    return retVal;
  }

  private void addIntervalToTimeline(
      Interval interval,
      TimelineEntry entry,
      NavigableMap<Interval, TimelineEntry> timeline
  )
  {
    if (interval != null && interval.toDurationMillis() > 0) {
      timeline.put(interval, entry);
    }
  }

  private void remove(
      NavigableMap<Interval, TimelineEntry> timeline,
      Interval interval,
      TimelineEntry entry,
      boolean incompleteOk
  )
  {
    List<Interval> intervalsToRemove = Lists.newArrayList();
    TimelineEntry removed = timeline.get(interval);

    if (removed == null) {
      for (Map.Entry<Interval, TimelineEntry> timelineEntry : timeline.entrySet()) {
        if (timelineEntry.getValue() == entry) {
          intervalsToRemove.add(timelineEntry.getKey());
        }
      }
    } else {
      intervalsToRemove.add(interval);
    }

    for (Interval i : intervalsToRemove) {
      remove(timeline, i, incompleteOk);
    }
  }

  private void remove(
      NavigableMap<Interval, TimelineEntry> timeline,
      Interval interval,
      boolean incompleteOk
  )
  {
    timeline.remove(interval);

    for (Map.Entry<Interval, KVArraySortedMap<String, TimelineEntry>> versionEntry : allTimelineEntries.entrySet()) {
      Interval key = versionEntry.getKey();
      if (interval.overlaps(key)) {
        KVArraySortedMap<String, TimelineEntry> value = versionEntry.getValue();
        if (incompleteOk) {
          add(timeline, key, value.lastEntry().getValue());
        } else {
          for (TimelineEntry entry : value.descendingValues()) {
            if (entry.partitionHolder.isComplete()) {
              add(timeline, key, entry);
              break;
            }
          }
        }
      }
    }
  }


  public class TimelineEntry
  {
    private final Interval trueInterval;
    private final String version;
    private final PartitionHolder<ObjectType> partitionHolder;

    public TimelineEntry(Interval trueInterval, String version, PartitionHolder<ObjectType> partitionHolder)
    {
      this.trueInterval = trueInterval;
      this.version = version;
      this.partitionHolder = partitionHolder;
    }

    public Interval getTrueInterval()
    {
      return trueInterval;
    }

    public String getVersion()
    {
      return version;
    }

    public PartitionHolder<ObjectType> getPartitionHolder()
    {
      return partitionHolder;
    }

    public TimelineObjectHolder<ObjectType> toPublic(Interval interval)
    {
      return new TimelineObjectHolder<ObjectType>(interval, version, partitionHolder);
    }

    public TimelineObjectHolder<ObjectType> toPublic()
    {
      return new TimelineObjectHolder<ObjectType>(trueInterval, version, partitionHolder);
    }
  }
}
