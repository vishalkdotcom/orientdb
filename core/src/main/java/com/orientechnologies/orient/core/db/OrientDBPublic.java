package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;

/**
 * Created by tglman on 27/01/17.
 */
public class OrientDBPublic implements AutoCloseable {

  private OrientDB orientdb;

  public OrientDBPublic(String url, OrientDBConfig config) {
    this.orientdb = OrientDB.fromUrl(url, config);
  }

  public ODatabaseDocument open(String name, String user, String password) {
    return orientdb.open(name, user, password);
  }

  public boolean exists(String name, String user, String password) {
    return orientdb.exists(name, user, password);
  }

  public void create(String name, String user, String password, OrientDB.DatabaseType type) {
    orientdb.create(name, user, password, type);
  }

  protected ODatabasePool openPool(String name, String user, String password) {
    return orientdb.openPool(name, user, password);
  }

  @Override
  public void close() {
    orientdb.close();
  }

}
