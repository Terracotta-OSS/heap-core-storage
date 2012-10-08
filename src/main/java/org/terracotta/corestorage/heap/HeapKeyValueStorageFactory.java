/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.terracotta.corestorage.heap;

import java.util.List;
import org.terracotta.corestorage.KeyValueStorage;
import org.terracotta.corestorage.KeyValueStorageConfig;
import org.terracotta.corestorage.KeyValueStorageFactory;
import org.terracotta.corestorage.KeyValueStorageMutationListener;

/**
 *
 * @author cdennis
 */
public class HeapKeyValueStorageFactory implements KeyValueStorageFactory {

  @Override
  public <K, V> KeyValueStorage<K, V> create(final KeyValueStorageConfig<K, V> config, final Object ... configs) {

    List<? extends KeyValueStorageMutationListener<? super K, ? super V>> mutationListeners = null;

    if(config != null) {
      mutationListeners = config.getMutationListeners();
    }

    return new HeapKeyValueStorage<K, V>(mutationListeners);
  }
}
