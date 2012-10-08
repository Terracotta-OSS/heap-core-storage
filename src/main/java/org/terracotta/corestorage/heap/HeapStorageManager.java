/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.terracotta.corestorage.heap;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import org.terracotta.corestorage.KeyValueStorage;
import org.terracotta.corestorage.KeyValueStorageConfig;
import org.terracotta.corestorage.KeyValueStorageFactory;
import org.terracotta.corestorage.StorageManager;
import org.terracotta.corestorage.StorageManagerConfiguration;

/**
 *
 * @author cdennis
 */
public class HeapStorageManager implements StorageManager {

  // no data
  // -> create fresh
  // -> fail
  // data exists
  // -> use
  // -> fail
  // -> delete

  private final KeyValueStorageFactory factory;
  private final StorageManagerConfiguration           configuration;

  private final ConcurrentMap<String, MapHolder> maps = new ConcurrentHashMap<String, MapHolder>();
  private volatile Status status;


  public HeapStorageManager(KeyValueStorageFactory factory) {
    this(factory, new GBManagerConfigurationDummy());
  }

  protected HeapStorageManager(KeyValueStorageFactory factory, StorageManagerConfiguration configuration) {
    this.factory = factory;
    this.configuration = configuration;
  }

  @Override
  public StorageManagerConfiguration getConfiguration() {
    return this.configuration;
  }

  @Override
  public Future<Void> start() {
    final FutureTask<Void> future = new FutureTask<Void>(new Runnable() {
      @Override
      public void run() {
        for (Map.Entry<String, KeyValueStorageConfig<?, ?>> mapConfigEntry : configuration.mapConfig().entrySet()) {
          final KeyValueStorage<?, ?> map = factory.createMap(mapConfigEntry.getValue());
          final String mapAlias = mapConfigEntry.getKey();
          registerMap(mapAlias, map, mapConfigEntry.getValue().getKeyClass(), mapConfigEntry.getValue().getValueClass());
        }
        status = Status.STARTED;
      }
    }, null);
    new Thread(future).start();
    return future;
  }

  public void shutdown() {
    status = Status.STOPPED;
    for (String alias : maps.keySet()) {
      unregisterMap(alias);
    }
  }

  public <K, V> void attachMap(String alias, KeyValueStorage<K, V> map, Class<K> keyClass, Class<V> valueClass) throws IllegalStateException {
    checkIsStarted();
    registerMap(alias, map, keyClass, valueClass);
  }

  public void detachMap(String name) {
    checkIsStarted();
    unregisterMap(name);
  }

  public <K, V> KeyValueStorage<K, V> getMap(String alias, Class<K> keyClass, Class<V> valueClass) {
    checkIsStarted();
    final MapHolder mapHolder = maps.get(alias);
    return mapHolder == null ? null : mapHolder.getMap(keyClass, valueClass);
  }

  public void begin() {
    checkIsStarted();
  }

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

  private static class GBManagerConfigurationDummy implements StorageManagerConfiguration {

    private final HashMap<String, KeyValueStorageConfig<?, ?>> stringGBMapConfigHashMap = new HashMap<String, KeyValueStorageConfig<?, ?>>();

    @Override
    public Collection<Object> sharedConfig() {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public Map<String, KeyValueStorageConfig<?, ?>> mapConfig() {
      return stringGBMapConfigHashMap;
    }
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
