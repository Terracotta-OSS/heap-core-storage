/* 
 * The contents of this file are subject to the Terracotta Public License Version
 * 2.0 (the "License"); You may not use this file except in compliance with the
 * License. You may obtain a copy of the License at 
 *
 *      http://terracotta.org/legal/terracotta-public-license.
 *
 * Software distributed under the License is distributed on an "AS IS" basis,
 * WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License for
 * the specific language governing rights and limitations under the License.
 *
 * The Covered Software is Heap Core Storage Implementation.
 *
 * The Initial Developer of the Covered Software is 
 *      Terracotta, Inc., a Software AG company
 */
package org.terracotta.corestorage.heap;

import java.util.List;
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
    public long getVital() {
      return getUsed();
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
    public Runnable addUsedThreshold(Direction direction, long value, Runnable action) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Runnable removeUsedThreshold(Direction direction, long value) {
      return null;
    }

    @Override
    public Runnable addReservedThreshold(Direction direction, long value, Runnable action) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Runnable removeReservedThreshold(Direction direction, long value) {
      return null;
    }

  }
}
