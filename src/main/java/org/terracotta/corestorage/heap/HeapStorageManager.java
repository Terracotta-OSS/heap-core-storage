/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package org.terracotta.corestorage.heap;

import org.terracotta.corestorage.KeyValueStorage;
import org.terracotta.corestorage.KeyValueStorageConfig;
import org.terracotta.corestorage.StorageManager;
import org.terracotta.corestorage.monitoring.MonitoredResource;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

public class HeapStorageManager implements StorageManager {

  // no data
  // -> create fresh
  // -> fail
  // data exists
  // -> use
  // -> fail
  // -> delete

  private final HeapKeyValueStorageFactory factory = new HeapKeyValueStorageFactory();
  private final Map<String, String> storageProperties = new ConcurrentHashMap<String, String>();
  private final Map<String, KeyValueStorageConfig<?, ?>> configs;

  private final ConcurrentMap<String, MapHolder> maps = new ConcurrentHashMap<String, MapHolder>();
  private volatile Status status;


  public HeapStorageManager() {
    this(Collections.<String, KeyValueStorageConfig<?, ?>>emptyMap());
  }

  public HeapStorageManager(Map<String, KeyValueStorageConfig<?, ?>> configs) {
    this.configs = new ConcurrentHashMap<String, KeyValueStorageConfig<?, ?>>(configs);
  }

  @Override
  public Map<String, String> getProperties() {
    return storageProperties;
  }

  @Override
  public Future<Void> start() {
    final FutureTask<Void> future = new FutureTask<Void>(new Runnable() {
      @Override
      public void run() {
        for (Map.Entry<String, KeyValueStorageConfig<?, ?>> mapConfigEntry : configs.entrySet()) {
          final KeyValueStorage<?, ?> map = factory.create(mapConfigEntry.getValue());
          final String mapAlias = mapConfigEntry.getKey();
          registerMap(mapAlias, map, mapConfigEntry.getValue().getKeyClass(), mapConfigEntry.getValue().getValueClass());
        }
        status = Status.STARTED;
      }
    }, null);
    new Thread(future).start();
    return future;
  }

  @Override
  public void shutdown() {
    status = Status.STOPPED;
    for (String alias : maps.keySet()) {
      unregisterMap(alias);
    }
  }

  @Override
  public <K, V> KeyValueStorage<K, V> createKeyValueStorage(String alias, KeyValueStorageConfig<K, V> config) throws IllegalStateException {
    checkIsStarted();
    KeyValueStorage<K, V> storage = factory.create(config);
    if (maps.putIfAbsent(alias, new MapHolder(storage, config.getKeyClass(), config.getValueClass())) != null) {
      throw new IllegalStateException("Duplicated map for alias: " + alias);
    } else {
      return storage;
    }
  }

  @Override
  public void destroyKeyValueStorage(String name) {
    checkIsStarted();
    unregisterMap(name);
  }

  @Override
  public <K, V> KeyValueStorage<K, V> getKeyValueStorage(String alias, Class<K> keyClass, Class<V> valueClass) {
    checkIsStarted();
    final MapHolder mapHolder = maps.get(alias);
    return mapHolder == null ? null : mapHolder.getMap(keyClass, valueClass);
  }

  @Override
  public void begin() {
    checkIsStarted();
  }

  @Override
  public void commit() {
    checkIsStarted();
  }

  protected <K, V> void registerMap(final String mapAlias, final KeyValueStorage<?, ?> map, final Class<K> keyClass, final Class<V> valueClass) {
    if (maps.putIfAbsent(mapAlias, new MapHolder(map, keyClass, valueClass)) != null) {
      throw new IllegalStateException("Duplicated map for alias: " + mapAlias);
    }
  }

  private void unregisterMap(final String name) {
    maps.remove(name);
  }

  private void checkIsStarted() {
    if (status != Status.STARTED) {
      throw new IllegalStateException("We're " + status + " which is not started");
    }
  }

  @Override
  public Collection<MonitoredResource> getMonitoredResources() {
    return Collections.singleton(factory.getHeapResource());
  }

  private static enum Status {INITIALIZED, STARTED, STOPPED}

  private static class MapHolder<K, V> {

    private final KeyValueStorage<K, V> map;
    private final Class<K> keyClass;
    private final Class<V> valueClass;

    private MapHolder(final KeyValueStorage<K, V> map, final Class<K> keyClass, final Class<V> valueClass) {
      this.map = map;
      this.keyClass = keyClass;
      this.valueClass = valueClass;
    }

    public KeyValueStorage<K, V> getMap(final Class<?> keyClass, final Class<?> valueClass) {
      if ((keyClass != this.keyClass) || (valueClass != this.valueClass)) {
        throw new IllegalArgumentException("Classes don't match!");
      }
      return map;
    }
  }
}
