package org.terracotta.corestorage.heap;

import org.hamcrest.core.IsNull;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.terracotta.corestorage.KeyValueStorage;
import org.terracotta.corestorage.KeyValueStorageConfig;
import org.terracotta.corestorage.KeyValueStorageFactory;
import org.terracotta.corestorage.KeyValueStorageMutationListener;
import org.terracotta.corestorage.Retriever;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

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
    final KeyValueStorageConfig<Integer, String> mapConfig = new KeyValueStorageConfigImpl<Integer, String>(Integer.class, String.class);
    mapConfig.addListener(new KeyValueStorageMutationListener<Integer, String>() {
      @Override
      public void removed(final Retriever<? extends Integer> key, final Retriever<? extends String> value, final Map<? extends Enum, Object> metadata) {
        invoked.set(true);
      }

      @Override
      public void added(final Retriever<? extends Integer> key, final Retriever<? extends String> value, final Map<? extends Enum, Object> metadata) {
        invoked.set(true);
      }
    });
    final KeyValueStorage<Integer,String> map = factory.create(mapConfig);
    assertThat(map, IsNull.notNullValue());
    assertThat(invoked.get(), is(false));
    map.put(1, "one");
    assertThat(invoked.get(), is(true));
  }
}
