package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import org.junit.Test;

/**
 * Created by tglman on 27/01/17.
 */
public class OrientDBPublicTest {

  @Test
  public void simpleTest() {
    try (OrientDBPublic orientdb = new OrientDBPublic("embedded:", OrientDBConfig.defaultConfig())) {
      if (!orientdb.exists("test", "", ""))
        orientdb.create("test", "", "", OrientDB.DatabaseType.MEMORY);
      try (ODatabaseDocument database = orientdb.open("test", "admin", "admin")) {

      }
    }

  }

  @Test
  public void simplePoolTest() {
    try (OrientDBPublic orientdb = new OrientDBPublic("embedded:", OrientDBConfig.defaultConfig())) {
      if (!orientdb.exists("test", "", ""))
        orientdb.create("test", "", "", OrientDB.DatabaseType.MEMORY);
      try (ODatabasePoolPublic pool = new ODatabasePoolPublic(orientdb, "test", "admin", "admin")) {
        try (ODatabaseDocument database = pool.acquire()) {

        }
      }
    }

  }

}
