package com.orientechnologies.orient.core.db;

import org.junit.Test;

/**
 * Created by tglman on 27/01/17.
 */
public class ODatabaseExecutorTest {

  @Test
  public void simpleExecuterTest() {

    try (OrientDB orient = OrientDB.fromUrl("embedded:", OrientDBConfig.defaultConfig())) {
      if (!orient.exists("test", "", ""))
        orient.create("test", "", "", OrientDB.DatabaseType.MEMORY);

      try (ODatabasePool pool = orient.openPool("test", "admin", "admin")) {

        //Be aware the close will wait for the end of execution.
        try (OrientDBExecutor executor = OrientDBExecutor.databaseExecutor(pool)) {
          executor.submit((ODatabaseRunnable) (database) -> database.save(database.newVertex()));
          executor.submit((ODatabaseRunnable) (database) -> database.save(database.newVertex()));
          executor.submit((ODatabaseRunnable) (database) -> database.save(database.newVertex()));
          executor.submit((ODatabaseRunnable) (database) -> database.save(database.newVertex()));
        }
      }

    }

  }

}
