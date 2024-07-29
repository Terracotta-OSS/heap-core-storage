/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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

import org.hamcrest.core.IsNull;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import org.terracotta.corestorage.KeyValueStorage;
import org.terracotta.corestorage.KeyValueStorageConfig;
import org.terracotta.corestorage.KeyValueStorageFactory;
import org.terracotta.corestorage.KeyValueStorageMutationListener;
import org.terracotta.corestorage.Retriever;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.terracotta.corestorage.ImmutableKeyValueStorageConfig.builder;

/**
 * @author Alex Snaps
 */
public class HeapKeyValueStorageFactoryTest {
  @Test
  public void testCreateMap() throws Exception {
    KeyValueStorageFactory factory = new HeapKeyValueStorageFactory();
    assertThat(factory.create(null), IsNull.notNullValue());
  }

  @Test
  public void testUsesRegisteredMutationListeners() {
    final AtomicBoolean invoked = new AtomicBoolean();
    KeyValueStorageFactory factory = new HeapKeyValueStorageFactory();
    final KeyValueStorageConfig<Integer, String> mapConfig = builder(Integer.class, String.class).listener(new KeyValueStorageMutationListener<Integer, String>() {
      @Override
      public void removed(final Retriever<? extends Integer> key) {
        invoked.set(true);
      }

      @Override
      public void added(final Retriever<? extends Integer> key,  final Retriever<? extends String> value,final byte metadata) {
        invoked.set(true);
      }
    }).build();
    final KeyValueStorage<Integer,String> map = factory.create(mapConfig);
    assertThat(map, IsNull.notNullValue());
    assertThat(invoked.get(), is(false));
    map.put(1, "one");
    assertThat(invoked.get(), is(true));
  }
}
