package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;

import java.io.Closeable;

/**
 * Created by tglman on 27/06/16.
 */
public interface ODatabasePool extends AutoCloseable {

  static ODatabasePool open(String url, String user, String password) {
    return new ODatabasePoolWrapper(url, user, password);
  }

  static ODatabasePool open(String url) {
    return new ODatabasePoolWrapper(url);
  }

  ODatabaseDocument acquire();

  void close();
}
