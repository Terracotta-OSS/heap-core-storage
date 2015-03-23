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

import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;
import org.terracotta.corestorage.KeyValueStorage;
import org.terracotta.corestorage.KeyValueStorageMutationListener;
import org.terracotta.corestorage.Retriever;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author Alex Snaps
 */
public class HeapKeyValueStorageTest {

  @Test
  public void testPutStoresValue() {
    KeyValueStorage<Integer, String> map = new HeapKeyValueStorage<Integer, String>();
    final int maxKey = 1000;
    for(int i = 0; i < maxKey; i++) {
      map.put(i, Integer.toHexString(i));
    }
    for(int i = 0; i < maxKey; i++) {
      assertThat(map.get(i), equalTo(Integer.toHexString(i)));
    }
  }

  @Test
  public void testNotifiesListenersOnAddAndRemove() {

    final CountingMapStorageMutationListener<Integer, String> mapMutationListener = new CountingMapStorageMutationListener<Integer, String>();
    KeyValueStorage<Integer, String> map = new HeapKeyValueStorage<Integer, String>(Collections.singletonList(mapMutationListener));
    final int maxKey = 1000;
    for(int i = 0; i < maxKey; i++) {
      map.put(i, Integer.toHexString(i));
    }
    assertThat(mapMutationListener.added.get(), is((long) maxKey));
    assertThat(mapMutationListener.removed.get(), is(0L));

    for(int i = 500; i < 500 + maxKey; i++) {
      map.remove(i);
    }

    assertThat(mapMutationListener.added.get(), is((long) maxKey));
    assertThat(mapMutationListener.removed.get(), is(500L));

  }

  private static class CountingMapStorageMutationListener<K, V> implements KeyValueStorageMutationListener<K, V> {

    final AtomicLong added = new AtomicLong();
    final AtomicLong removed = new AtomicLong();

    @Override
    public void removed(final Retriever<? extends K> key) {
      removed.incrementAndGet();
    }

    @Override
    public void added(final Retriever<? extends K> key, final Retriever<? extends V> value, final byte metadata) {
      added.incrementAndGet();
    }
  }
}
