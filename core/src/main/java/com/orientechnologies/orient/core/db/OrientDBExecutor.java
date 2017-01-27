package com.orientechnologies.orient.core.db;

import java.io.Closeable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by tglman on 23/01/17.
 */
public interface OrientDBExecutor extends Closeable {

  static OrientDBExecutor databaseExecutor(String url, String user, String password) {
    return new OrientDBExecutorImpl(ODatabasePool.open(url, user, password),
        Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()));
  }

  static OrientDBExecutor databaseExecutor(ODatabasePool pool) {
    return databaseExecutor(pool, Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()));
  }

  static OrientDBExecutor databaseExecutor(ODatabasePool pool, ExecutorService executor) {
    return new OrientDBExecutorImpl(pool, executor);
  }

  <T> Future<T> submit(ODatabaseCallable<T> callable);

  default void submit(ODatabaseRunnable runnable) {
    this.<Void>submit((database) -> {
      runnable.run(database);
      return (Void) null;
    });
  }

  void close();
}
