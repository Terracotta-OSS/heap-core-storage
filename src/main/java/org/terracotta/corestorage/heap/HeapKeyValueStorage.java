/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package org.terracotta.corestorage.heap;

import org.terracotta.corestorage.KeyValueStorage;
import org.terracotta.corestorage.KeyValueStorageMutationListener;
import org.terracotta.corestorage.Retriever;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class HeapKeyValueStorage<K, V> implements KeyValueStorage<K, V> {

  private final ConcurrentMap<K, V> store = new ConcurrentHashMap<K, V>();

  private final ReadWriteLock[] locks;
  private final int segmentShift;
  private final int segmentMask;
  private final Collection<KeyValueStorageMutationListener<? super K, ? super V>> mutationListeners;

  public HeapKeyValueStorage() {
    this(null);
  }

  public HeapKeyValueStorage(final List<? extends KeyValueStorageMutationListener<? super K, ? super V>> mutationListeners) {
    this(mutationListeners, 512);
  }

  public HeapKeyValueStorage(final List<? extends KeyValueStorageMutationListener<? super K, ? super V>> mutationListeners, final int concurrency) {
    int sshift = 0;
    int ssize = 1;
    while (ssize < concurrency) {
      ++sshift;
      ssize <<= 1;
    }
    segmentShift = 32 - sshift;
    segmentMask = ssize - 1;
    this.locks = new ReadWriteLock[concurrency];
    for (int i = 0, locksLength = locks.length; i < locksLength; i++) {
      locks[i] = new ReentrantReadWriteLock();
    }
    if (mutationListeners == null || mutationListeners.isEmpty()) {
      this.mutationListeners = Collections.emptyList();
    } else {
      this.mutationListeners = Collections.unmodifiableList(new ArrayList<KeyValueStorageMutationListener<? super K, ? super V>>(mutationListeners));
    }
  }

  @Override
  public Set<K> keySet() {
    return store.keySet();
  }

  @Override
  public Collection<V> values() {
    return store.values();
  }

  @Override
  public long size() {
    return store.size();
  }

  @Override
  public void put(final K key, final V value) {
    put(key, value, (byte) 0);
  }

  public void put(final K key, final V value, byte metadata) {
    final Lock lock = getLockFor(key).writeLock();
    lock.lock();
    try {
      store.put(key, value);
      notifyAdd(key, value, metadata);
    } finally {
      lock.unlock();
    }
  }
  
  @Override
  public V get(final K key) {
    final Lock lock = getLockFor(key).readLock();
    lock.lock();
    try {
      return store.get(key);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean remove(final K key) {
    final Lock lock = getLockFor(key).writeLock();
    lock.lock();
    try {
      final V previous = store.remove(key);
      if (previous != null) {
        notifyRemove(key, previous);
        return true;
      } else {
        return false;
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void removeAll(final Collection<K> keys) {
    for (K key : keys) {
      remove(key);
    }
  }

  @Override
  public boolean containsKey(final K key) {
    final Lock lock = getLockFor(key).readLock();
    lock.lock();
    try {
      return store.containsKey(key);
    } finally {
      lock.unlock();
    }
  }

  private void notifyAdd(final K key, final V value, byte metadata) {
    for (KeyValueStorageMutationListener<? super K, ? super V> mutationListener : mutationListeners) {
      mutationListener.added(new HeapRetriever<K>(key), new HeapRetriever<V>(value), metadata);
    }
  }

  private void notifyRemove(final K key, final V value) {
    for (KeyValueStorageMutationListener<? super K, ? super V> mutationListener : mutationListeners) {
      mutationListener.removed(new HeapRetriever<K>(key), new HeapRetriever<V>(value));
    }
  }

  private ReadWriteLock getLockFor(K key) {
    return locks[(spread(key.hashCode()) >>> segmentShift) & segmentMask];
  }

  private static int spread(int hash) {
    int h = hash;
    h += (h << 15) ^ 0xffffcd7d;
    h ^= (h >>> 10);
    h += (h << 3);
    h ^= (h >>> 6);
    h += (h << 2) + (h << 14);
    return h ^ (h >>> 16);
  }

  @Override
  public void clear() {
    store.clear();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    final HeapKeyValueStorage that = (HeapKeyValueStorage)o;

    if (segmentMask != that.segmentMask) return false;
    if (segmentShift != that.segmentShift) return false;
    if (!mutationListeners.equals(that.mutationListeners)) return false;
    if (!store.equals(that.store)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = store.hashCode();
    result = 31 * result + segmentShift;
    result = 31 * result + segmentMask;
    result = 31 * result + mutationListeners.hashCode();
    return result;
  }
}
class HeapRetriever<T> implements Retriever<T> {

  private final T value;

  HeapRetriever(final T value) {
    this.value = value;
  }

  @Override
  public T retrieve() {
    return value;
  }
}
