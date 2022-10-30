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

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.druid.common.guava.GuavaUtils;
import io.druid.common.utils.JodaUtils;
import io.druid.java.util.common.UOE;
import io.druid.timeline.partition.ImmutablePartitionHolder;
import io.druid.timeline.partition.PartitionChunk;
import io.druid.timeline.partition.PartitionHolder;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
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
public class VersionedIntervalTimeline<VersionType, ObjectType> implements TimelineLookup<VersionType, ObjectType>
{
  private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

  final NavigableMap<Interval, TimelineEntry> completePartitionsTimeline = new TreeMap<Interval, TimelineEntry>(
      JodaUtils.intervalsByStartThenEnd()
  );
  final NavigableMap<Interval, TimelineEntry> incompletePartitionsTimeline = new TreeMap<Interval, TimelineEntry>(
      JodaUtils.intervalsByStartThenEnd()
  );
  private final Map<Interval, TreeMap<VersionType, TimelineEntry>> allTimelineEntries = Maps.newHashMap();

  private final Comparator<? super VersionType> versionComparator;

  public VersionedIntervalTimeline()
  {
    this(GuavaUtils.noNullableNatural());
  }

  public VersionedIntervalTimeline(Comparator<? super VersionType> versionComparator)
  {
    this.versionComparator = versionComparator;
  }

  public Interval coverage()
  {
    Interval from1 = Iterables.getFirst(completePartitionsTimeline.keySet(), null);
    Interval from2 = Iterables.getFirst(incompletePartitionsTimeline.keySet(), null);
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
    Interval to1 = completePartitionsTimeline.lastKey();
    Interval to2 = incompletePartitionsTimeline.lastKey();
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
    for (TreeMap<VersionType, TimelineEntry> map : allTimelineEntries.values()) {
      for (TimelineEntry entry : map.values()) {
        Iterables.addAll(chunks, entry.getPartitionHolder());
      }
    }
    incompletePartitionsTimeline.clear();
    completePartitionsTimeline.clear();
    allTimelineEntries.clear();
    return chunks;
  }

  public List<ObjectType> getAll()
  {
    List<ObjectType> objects = Lists.newArrayList();
    for (TreeMap<VersionType, TimelineEntry> map : allTimelineEntries.values()) {
      for (TimelineEntry entry : map.values()) {
        Iterables.addAll(objects, entry.getPartitionHolder().payloads());
      }
    }
    return objects;
  }

  public boolean isEmpty()
  {
    return allTimelineEntries.isEmpty();
  }

  public void add(final Interval interval, VersionType version, PartitionChunk<ObjectType> object)
  {
    lock.writeLock().lock();
    try {
      Map<VersionType, TimelineEntry> exists = allTimelineEntries.get(interval);
      TimelineEntry entry;

      if (exists == null) {
        entry = new TimelineEntry(interval, version, new PartitionHolder<ObjectType>(object));
        TreeMap<VersionType, TimelineEntry> versionEntry = new TreeMap<VersionType, TimelineEntry>(versionComparator);
        versionEntry.put(version, entry);
        allTimelineEntries.put(interval, versionEntry);
      } else {
        entry = exists.get(version);

        if (entry == null) {
          entry = new TimelineEntry(interval, version, new PartitionHolder<ObjectType>(object));
          exists.put(version, entry);
        } else {
          PartitionHolder<ObjectType> partitionHolder = entry.getPartitionHolder();
          partitionHolder.add(object);
        }
      }

      if (entry.getPartitionHolder().isComplete()) {
        add(completePartitionsTimeline, interval, entry);
      }

      add(incompletePartitionsTimeline, interval, entry);
    }
    finally {
      lock.writeLock().unlock();
    }
  }

  public PartitionChunk<ObjectType> remove(Interval interval, VersionType version, PartitionChunk<ObjectType> chunk)
  {
    lock.writeLock().lock();
    try {
      Map<VersionType, TimelineEntry> versionEntries = allTimelineEntries.get(interval);
      if (versionEntries == null) {
        return null;
      }

      TimelineEntry entry = versionEntries.get(version);
      if (entry == null) {
        return null;
      }

      PartitionHolder<ObjectType> holder = entry.getPartitionHolder();
      PartitionChunk<ObjectType> retVal = holder.remove(chunk);
      if (holder.isEmpty()) {
        versionEntries.remove(version);
        if (versionEntries.isEmpty()) {
          allTimelineEntries.remove(interval);
        }
        remove(incompletePartitionsTimeline, interval, entry, true);
      }

      remove(completePartitionsTimeline, interval, entry, false);

      return retVal;
    }
    finally {
      lock.writeLock().unlock();
    }
  }

  public <T> T find(Function<Map.Entry<Interval, TreeMap<VersionType, TimelineEntry>>, T> finder)
  {
    lock.readLock().lock();
    try {
      for (Map.Entry<Interval, TreeMap<VersionType, TimelineEntry>> entry : allTimelineEntries.entrySet()) {
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
  public PartitionHolder<ObjectType> findEntry(Interval interval, VersionType version)
  {
    lock.readLock().lock();
    try {
      TreeMap<VersionType, TimelineEntry> entryMap = allTimelineEntries.get(interval);
      if (entryMap != null) {
        TimelineEntry foundEntry = entryMap.get(version);
        if (foundEntry != null) {
          return new ImmutablePartitionHolder<ObjectType>(foundEntry.getPartitionHolder());
        }
      }
      for (Map.Entry<Interval, TreeMap<VersionType, TimelineEntry>> entry : allTimelineEntries.entrySet()) {
        if (entry.getKey().equals(interval) || entry.getKey().contains(interval)) {
          TimelineEntry foundEntry = entry.getValue().get(version);
          if (foundEntry != null) {
            return new ImmutablePartitionHolder<ObjectType>(
                foundEntry.getPartitionHolder()
            );
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
  public List<TimelineObjectHolder<VersionType, ObjectType>> lookup(Interval interval)
  {
    lock.readLock().lock();
    try {
      return lookup(interval, false);
    }
    finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public Iterable<TimelineObjectHolder<VersionType, ObjectType>> lookupWithIncompletePartitions(Interval interval)
  {
    lock.readLock().lock();
    try {
      return lookup(interval, true);
    }
    finally {
      lock.readLock().unlock();
    }
  }

  public Set<TimelineObjectHolder<VersionType, ObjectType>> findOvershadowed()
  {
    lock.readLock().lock();
    try {
      Set<TimelineObjectHolder<VersionType, ObjectType>> retVal = Sets.newHashSet();

      Map<Interval, Map<VersionType, TimelineEntry>> overShadowed = Maps.newHashMap();
      for (Map.Entry<Interval, TreeMap<VersionType, TimelineEntry>> versionEntry : allTimelineEntries.entrySet()) {
        overShadowed.put(versionEntry.getKey(), Maps.newHashMap(versionEntry.getValue()));
      }

      for (Map.Entry<Interval, TimelineEntry> entry : completePartitionsTimeline.entrySet()) {
        Map<VersionType, TimelineEntry> versionEntry = overShadowed.get(entry.getValue().getTrueInterval());
        if (versionEntry != null) {
          versionEntry.remove(entry.getValue().getVersion());
          if (versionEntry.isEmpty()) {
            overShadowed.remove(entry.getValue().getTrueInterval());
          }
        }
      }

      for (Map.Entry<Interval, TimelineEntry> entry : incompletePartitionsTimeline.entrySet()) {
        Map<VersionType, TimelineEntry> versionEntry = overShadowed.get(entry.getValue().getTrueInterval());
        if (versionEntry != null) {
          versionEntry.remove(entry.getValue().getVersion());
          if (versionEntry.isEmpty()) {
            overShadowed.remove(entry.getValue().getTrueInterval());
          }
        }
      }

      for (Map.Entry<Interval, Map<VersionType, TimelineEntry>> versionEntry : overShadowed.entrySet()) {
        for (Map.Entry<VersionType, TimelineEntry> entry : versionEntry.getValue().entrySet()) {
          TimelineEntry object = entry.getValue();
          retVal.add(
              new TimelineObjectHolder<VersionType, ObjectType>(
                  object.getTrueInterval(),
                  object.getVersion(),
                  object.getPartitionHolder()
              )
          );
        }
      }

      return retVal;
    }
    finally {
      lock.readLock().unlock();
    }
  }

  public boolean isOvershadowed(Interval interval, VersionType version)
  {
    try {
      lock.readLock().lock();

      TimelineEntry entry = completePartitionsTimeline.get(interval);
      if (entry != null) {
        return versionComparator.compare(version, entry.getVersion()) < 0;
      }

      Interval lower = completePartitionsTimeline.floorKey(
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
            versionComparator.compare(version, completePartitionsTimeline.get(curr).getVersion()) >= 0
            ) {
          return false;
        }

        prev = curr;
        curr = completePartitionsTimeline.higherKey(curr);

      } while (interval.getEndMillis() > prev.getEndMillis());

      return true;
    }
    finally {
      lock.readLock().unlock();
    }
  }

  private void add(
      NavigableMap<Interval, TimelineEntry> timeline,
      Interval interval,
      TimelineEntry entry
  )
  {
    TimelineEntry existsInTimeline = timeline.get(interval);

    if (existsInTimeline != null) {
      int compare = versionComparator.compare(entry.getVersion(), existsInTimeline.getVersion());
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
    if (!key.overlaps(entry.getTrueInterval())) {
      return false;
    }

    boolean retVal = false;
    Interval currKey = key;
    Interval entryInterval = entry.getTrueInterval();

    while (entryInterval != null && currKey != null && currKey.overlaps(entryInterval)) {

      Interval nextKey = timeline.higherKey(currKey);
      TimelineEntry currEntry = timeline.get(currKey);

      int versionCompare = versionComparator.compare(entry.getVersion(), currEntry.getVersion());

      if (versionCompare < 0) {
        if (currKey.contains(entryInterval)) {
          return true;
        } else if (currKey.getStartMillis() < entryInterval.getStartMillis()) {
          entryInterval = new Interval(currKey.getEndMillis(), entryInterval.getEndMillis(), currKey.getChronology());
        } else {
          addIntervalToTimeline(
              new Interval(entryInterval.getStartMillis(), currKey.getStartMillis(), entryInterval.getChronology()),
              entry,
              timeline
          );

          if (entryInterval.getEndMillis() > currKey.getEndMillis()) {
            entryInterval = new Interval(currKey.getEndMillis(), entryInterval.getEndMillis(), currKey.getChronology());
          } else {
            entryInterval = null; // discard this entry
          }
        }
      } else if (versionCompare > 0) {

        if (currKey.equals(entryInterval)) {
          addIntervalToTimeline(entryInterval, entry, timeline);
          return true;
        }
        TimelineEntry oldEntry = timeline.remove(currKey);

        if (currKey.contains(entryInterval)) {
          addIntervalToTimeline(
              new Interval(currKey.getStartMillis(), entryInterval.getStartMillis(), currKey.getChronology()),
              oldEntry,
              timeline
          );
          addIntervalToTimeline(
              new Interval(entryInterval.getEndMillis(), currKey.getEndMillis(), entryInterval.getChronology()),
              oldEntry, timeline
          );
          addIntervalToTimeline(entryInterval, entry, timeline);

          return true;
        } else if (currKey.getStartMillis() < entryInterval.getStartMillis()) {
          addIntervalToTimeline(
              new Interval(currKey.getStartMillis(), entryInterval.getStartMillis(), currKey.getChronology()),
              oldEntry,
              timeline
          );
        } else if (currKey.getEndMillis() > entryInterval.getEndMillis()) {
          addIntervalToTimeline(
              new Interval(entryInterval.getEndMillis(), currKey.getEndMillis(), entryInterval.getChronology()),
              oldEntry,
              timeline
          );
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
              entry.getVersion()
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

    for (Map.Entry<Interval, TreeMap<VersionType, TimelineEntry>> versionEntry : allTimelineEntries.entrySet()) {
      if (versionEntry.getKey().overlap(interval) != null) {
        if (incompleteOk) {
          add(timeline, versionEntry.getKey(), versionEntry.getValue().lastEntry().getValue());
        } else {
          for (VersionType ver : versionEntry.getValue().descendingKeySet()) {
            TimelineEntry timelineEntry = versionEntry.getValue().get(ver);
            if (timelineEntry.getPartitionHolder().isComplete()) {
              add(timeline, versionEntry.getKey(), timelineEntry);
              break;
            }
          }
        }
      }
    }
  }

  private List<TimelineObjectHolder<VersionType, ObjectType>> lookup(Interval interval, boolean incompleteOk)
  {
    List<TimelineObjectHolder<VersionType, ObjectType>> retVal = new ArrayList<TimelineObjectHolder<VersionType, ObjectType>>();
    NavigableMap<Interval, TimelineEntry> timeline = (incompleteOk)
                                                     ? incompletePartitionsTimeline
                                                     : completePartitionsTimeline;

    for (Map.Entry<Interval, TimelineEntry> entry : timeline.entrySet()) {
      Interval timelineInterval = entry.getKey();
      TimelineEntry val = entry.getValue();

      if (timelineInterval.overlaps(interval)) {
        retVal.add(
            new TimelineObjectHolder<VersionType, ObjectType>(
                timelineInterval,
                val.getVersion(),
                val.getPartitionHolder()
            )
        );
      }
    }

    // this trimming thing is very stupid idea. but it infested over all kind of modules, let it in that way.
    if (retVal.isEmpty()) {
      return retVal;
    }

    if (retVal.size() == 1) {
      TimelineObjectHolder<VersionType, ObjectType> singleEntry = retVal.get(0);
      return Arrays.asList(
          new TimelineObjectHolder<VersionType, ObjectType>(
              interval.overlap(singleEntry.getInterval()),
              singleEntry.getVersion(),
              singleEntry.getObject()
          )
      );
    }

    TimelineObjectHolder<VersionType, ObjectType> firstEntry = retVal.get(0);
    Interval firstInterval = firstEntry.getInterval();
    if (interval.overlaps(firstInterval) && interval.getStartMillis() > firstInterval.getStartMillis()) {
      retVal.set(
          0,
          new TimelineObjectHolder<VersionType, ObjectType>(
              new Interval(interval.getStartMillis(), firstInterval.getEndMillis(), interval.getChronology()),
              firstEntry.getVersion(),
              firstEntry.getObject()
          )
      );
    }

    TimelineObjectHolder<VersionType, ObjectType> lastEntry = retVal.get(retVal.size() - 1);
    Interval lastInterval = lastEntry.getInterval();
    if (interval.overlaps(lastInterval) && interval.getEndMillis() < lastInterval.getEndMillis()) {
      retVal.set(
          retVal.size() - 1,
          new TimelineObjectHolder<VersionType, ObjectType>(
              new Interval(lastInterval.getStartMillis(), interval.getEndMillis(), lastInterval.getChronology()),
              lastEntry.getVersion(),
              lastEntry.getObject()
          )
      );
    }

    return retVal;
  }

  public class TimelineEntry
  {
    private final Interval trueInterval;
    private final VersionType version;
    private final PartitionHolder<ObjectType> partitionHolder;

    public TimelineEntry(Interval trueInterval, VersionType version, PartitionHolder<ObjectType> partitionHolder)
    {
      this.trueInterval = trueInterval;
      this.version = version;
      this.partitionHolder = partitionHolder;
    }

    public Interval getTrueInterval()
    {
      return trueInterval;
    }

    public VersionType getVersion()
    {
      return version;
    }

    public PartitionHolder<ObjectType> getPartitionHolder()
    {
      return partitionHolder;
    }
  }
}
