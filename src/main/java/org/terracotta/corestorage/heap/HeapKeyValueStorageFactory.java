/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package org.terracotta.corestorage.heap;

import java.util.List;
import java.util.concurrent.Callable;
import org.terracotta.corestorage.KeyValueStorage;
import org.terracotta.corestorage.KeyValueStorageConfig;
import org.terracotta.corestorage.KeyValueStorageFactory;
import org.terracotta.corestorage.KeyValueStorageMutationListener;
import org.terracotta.corestorage.monitoring.MonitoredResource;

public class HeapKeyValueStorageFactory implements KeyValueStorageFactory {

  private final HeapMonitoredResource resource = new HeapMonitoredResource();
  
  @Override
  public <K, V> KeyValueStorage<K, V> create(final KeyValueStorageConfig<K, V> config) {

    List<? extends KeyValueStorageMutationListener<? super K, ? super V>> mutationListeners = null;

    if(config != null) {
      mutationListeners = config.getMutationListeners();
    }

    return new HeapKeyValueStorage<K, V>(mutationListeners);
  }
  
  public MonitoredResource getHeapResource() {
    return resource;
  }

  static class HeapMonitoredResource implements MonitoredResource {

    private final Runtime runtime = Runtime.getRuntime();
    
    @Override
    public Type getType() {
      return Type.HEAP;
    }

    @Override
    public long getUsed() {
      long total;
      long free;
      do {
        total = runtime.totalMemory();
        free = runtime.freeMemory();
      } while (total != runtime.totalMemory());
      return total - free;
    }

    @Override
    public long getReserved() {
      return getUsed();
    }

    @Override
    public long getTotal() {
      return runtime.maxMemory();
    }

    @Override
    public void addUsedThreshold(long value, Callable<?> action) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void addReservedThreshold(long value, Callable<?> action) {
      throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void removeThreshold(Callable<?> action) throws IllegalArgumentException {
      throw new UnsupportedOperationException("Not supported yet.");
    }
  }
}
