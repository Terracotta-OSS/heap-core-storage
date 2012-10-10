/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.terracotta.corestorage.heap;

import org.terracotta.corestorage.KeyValueStorageConfig;
import org.terracotta.corestorage.KeyValueStorageMutationListener;
import org.terracotta.corestorage.Serializer;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author cdennis
 */
public class KeyValueStorageConfigImpl<K, V> implements KeyValueStorageConfig<K, V> {

  private final Class<K> keyClass;
  private final Class<V> valueClass;
  private final List<KeyValueStorageMutationListener<? super K, ? super V>> mutationListeners = new ArrayList<KeyValueStorageMutationListener<? super K, ? super V>>();
  private Serializer<K> keySerializer;
  private Serializer<V> valueSerializer;

  public KeyValueStorageConfigImpl(final Class<K> keyClass, final Class<V> valueClass) {
    this.keyClass = keyClass;
    this.valueClass = valueClass;
  }

  @Override
  public Class<K> getKeyClass() {
    return keyClass;
  }

  @Override
  public Class<V> getValueClass() {
    return valueClass;
  }

  @Override
  public void addListener(final KeyValueStorageMutationListener<? super K, ? super V> listener) {
    mutationListeners.add(listener);
  }

  @Override
  public List<KeyValueStorageMutationListener<? super K, ? super V>> getMutationListeners() {
    return mutationListeners;
  }

  @Override
  public void setKeySerializer(Serializer<K> serializer) {
    keySerializer = serializer;
  }

  @Override
  public void setValueSerializer(Serializer<V> serializer) {
    valueSerializer = serializer;
  }

  @Override
  public Serializer<K> getKeySerializer() {
    return keySerializer;
  }

  @Override
  public Serializer<V> getValueSerializer() {
    return valueSerializer;
  }
}
