package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;

/**
 * Created by tglman on 27/01/17.
 */
public class ODatabasePoolPublic implements AutoCloseable {

  private ODatabasePool pool;

  /**
   * This may have additiona paramerter for the pool size ecc, maybe it just get a OrientDBConfig
   *
   * @param orientdb
   * @param database
   * @param user
   * @param password
   */
  public ODatabasePoolPublic(OrientDBPublic orientdb, String database, String user, String password) {
    this.pool = orientdb.openPool(database, user, password);
  }

  public ODatabaseDocument acquire() {
    return this.pool.acquire();
  }

  public void close() {
    this.pool.close();
  }

}
