package org.terracotta.corestorage.heap;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.terracotta.corestorage.KeyValueStorage;
import org.terracotta.corestorage.KeyValueStorageConfig;
import org.terracotta.corestorage.KeyValueStorageMutationListener;
import org.terracotta.corestorage.Retriever;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author Alex Snaps
 */
public class HeapStorageManagerTest {

  @Test(expected = IllegalStateException.class)
  public void testCantAttachMapIfNotStarted() {
    new HeapStorageManager(new HeapKeyValueStorageFactory()).attachMap("whatever!", new HeapKeyValueStorage<Object, Object>(), Object.class, Object.class);
  }

  @Test(expected = IllegalStateException.class)
  public void testCantAccessMapIfNotStarted() {
    new HeapStorageManager(new HeapKeyValueStorageFactory()).getMap("whatever!", Object.class, Object.class);
  }

  @Test
  public void testReturnNullWhenNotAttached() throws ExecutionException, InterruptedException {
    HeapStorageManager manager = new HeapStorageManager(new HeapKeyValueStorageFactory());
    manager.start().get();
    assertThat(manager.getMap("whatever!", Object.class, Object.class), nullValue());
  }

  @Test
  public void testReturnsMapWhenConfigured() throws ExecutionException, InterruptedException {
    HeapStorageManager manager = new HeapStorageManager(new HeapKeyValueStorageFactory());
    manager.getConfiguration().mapConfig().put("foo", new KeyValueStorageConfigImpl<String, String>(String.class, String.class));
    manager.start().get();
    assertThat(manager.getMap("foo", String.class, String.class), notNullValue());
  }

  @Test
  public void testThrowsWhenStopped() throws ExecutionException, InterruptedException {
    HeapStorageManager manager = new HeapStorageManager(new HeapKeyValueStorageFactory());
    manager.getConfiguration().mapConfig().put("foo", new KeyValueStorageConfigImpl<String, String>(String.class, String.class));
    manager.start().get();
    assertThat(manager.getMap("foo", String.class, String.class), notNullValue());
    manager.shutdown();
    try {
      manager.getMap("foo", String.class, String.class);
      fail();
    } catch (IllegalStateException e) {
      // expected!
    }
  }

  @Test
  public void testThrowsWhenTypeNotAssignable() throws ExecutionException, InterruptedException {
    HeapStorageManager manager = new HeapStorageManager(new HeapKeyValueStorageFactory());
    manager.getConfiguration().mapConfig().put("foo", new KeyValueStorageConfigImpl<Long, Integer>(Long.class, Integer.class));
    manager.start().get();
    assertThat(manager.getMap("foo", Long.class, Integer.class), notNullValue());
    try {
      assertThat(manager.getMap("foo", Number.class, Number.class), notNullValue());
      fail();
    } catch (IllegalArgumentException e) {
      // expected!
    }
    try {
      manager.getMap("foo", Integer.class, Long.class);
      fail();
    } catch (IllegalArgumentException e) {
      // expected!
    }
  }

  @Test
  public void testMutationsListenersGetWiredFromConfig() throws ExecutionException, InterruptedException {
    HeapStorageManager manager = new HeapStorageManager(new HeapKeyValueStorageFactory());
    final AtomicBoolean invoked = new AtomicBoolean();
    final KeyValueStorageConfig<Long, Integer> config = new KeyValueStorageConfigImpl<Long, Integer>(Long.class, Integer.class);
    config.addListener(new KeyValueStorageMutationListener<Long, Integer>() {
      @Override
      public void removed(final Retriever<? extends Long> key, final Retriever<? extends Integer> value, final Map<? extends Enum, Object> metadata) {
        invoked.set(true);
      }

      @Override
      public void added(final Retriever<? extends Long> key, final Retriever<? extends Integer> value, final Map<? extends Enum, Object> metadata) {
        invoked.set(true);
      }
    });
    manager.getConfiguration().mapConfig().put("foo", config);
    manager.start().get();
    final KeyValueStorage<Long, Integer> map = manager.getMap("foo", Long.class, Integer.class);
    assertThat(map, notNullValue());
    assertThat(invoked.get(), is(false));
    map.put(1L, 1);
    assertThat(invoked.get(), is(true));
  }
}
