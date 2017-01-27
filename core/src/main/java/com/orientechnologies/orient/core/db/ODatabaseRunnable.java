package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;

/**
 * Created by tglman on 27/01/17.
 */
public interface ODatabaseRunnable {
  void run(ODatabaseDocument database);
}
