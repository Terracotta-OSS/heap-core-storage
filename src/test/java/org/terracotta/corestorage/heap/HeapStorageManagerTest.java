/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.terracotta.corestorage.heap;

import org.junit.Test;
import org.terracotta.corestorage.ImmutableKeyValueStorageConfig;
import org.terracotta.corestorage.KeyValueStorage;
import org.terracotta.corestorage.KeyValueStorageConfig;
import org.terracotta.corestorage.KeyValueStorageMutationListener;
import org.terracotta.corestorage.Retriever;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.terracotta.corestorage.ImmutableKeyValueStorageConfig.builder;

/**
 * @author Alex Snaps
 */
public class HeapStorageManagerTest {

  @Test(expected = IllegalStateException.class)
  public void testCantCreateMapIfNotStarted() {
    new HeapStorageManager().createKeyValueStorage("whatever!", builder(Object.class, Object.class).build());
  }

  @Test(expected = IllegalStateException.class)
  public void testCantAccessMapIfNotStarted() {
    new HeapStorageManager().getKeyValueStorage("whatever!", Object.class, Object.class);
  }

  @Test
  public void testReturnNullWhenNotAttached() throws ExecutionException, InterruptedException {
    HeapStorageManager manager = new HeapStorageManager();
    manager.start().get();
    assertThat(manager.getKeyValueStorage("whatever!", Object.class, Object.class), nullValue());
  }

  @Test
  public void testReturnsMapWhenConfigured() throws ExecutionException, InterruptedException {
    HeapStorageManager manager = new HeapStorageManager(Collections.<String, KeyValueStorageConfig<?, ?>>singletonMap("foo", builder(String.class, String.class).build()));
    manager.start().get();
    assertThat(manager.getKeyValueStorage("foo", String.class, String.class), notNullValue());
  }

  @Test
  public void testThrowsWhenStopped() throws ExecutionException, InterruptedException {
    HeapStorageManager manager = new HeapStorageManager(Collections.<String, KeyValueStorageConfig<?, ?>>singletonMap("foo", builder(String.class, String.class).build()));
    manager.start().get();
    assertThat(manager.getKeyValueStorage("foo", String.class, String.class), notNullValue());
    manager.close();
    try {
      manager.getKeyValueStorage("foo", String.class, String.class);
      fail();
    } catch (IllegalStateException e) {
      // expected!
    }
  }

  @Test
  public void testThrowsWhenTypeNotAssignable() throws ExecutionException, InterruptedException {
    HeapStorageManager manager = new HeapStorageManager(Collections.<String, KeyValueStorageConfig<?, ?>>singletonMap("foo", builder(Long.class, Integer.class).build()));
    manager.start().get();
    assertThat(manager.getKeyValueStorage("foo", Long.class, Integer.class), notNullValue());
    try {
      assertThat(manager.getKeyValueStorage("foo", Number.class, Number.class), notNullValue());
      fail();
    } catch (IllegalArgumentException e) {
      // expected!
    }
    try {
      manager.getKeyValueStorage("foo", Integer.class, Long.class);
      fail();
    } catch (IllegalArgumentException e) {
      // expected!
    }
  }

  @Test
  public void testMutationsListenersGetWiredFromConfig() throws ExecutionException, InterruptedException {
    final AtomicBoolean invoked = new AtomicBoolean();
    final KeyValueStorageConfig<Long, Integer> config = builder(Long.class, Integer.class).listener(new KeyValueStorageMutationListener<Long, Integer>() {
      @Override
      public void removed(final Retriever<? extends Long> key) {
        invoked.set(true);
      }

      @Override
      public void added(final Retriever<? extends Long> key, final Retriever<? extends Integer> value, final byte metadata) {
        invoked.set(true);
      }
    }).build();
    HeapStorageManager manager = new HeapStorageManager(Collections.<String, KeyValueStorageConfig<?, ?>>singletonMap("foo", config));
    manager.start().get();
    final KeyValueStorage<Long, Integer> map = manager.getKeyValueStorage("foo", Long.class, Integer.class);
    assertThat(map, notNullValue());
    assertThat(invoked.get(), is(false));
    map.put(1L, 1);
    assertThat(invoked.get(), is(true));
  }

  @Test
  public void testGetKeyValueStorage() throws Exception {
    HeapStorageManager manager = new HeapStorageManager();
    manager.start().get();
    KeyValueStorage<Object, Object> map = manager.createKeyValueStorage("foo", builder(Object.class, Object.class).build());
    assertThat(manager.getKeyValueStorage("foo", Object.class, Object.class), sameInstance(map));
  }
}
