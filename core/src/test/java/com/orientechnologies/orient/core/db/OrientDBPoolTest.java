package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import org.junit.Test;

/**
 * Created by tglman on 27/01/17.
 */
public class OrientDBPoolTest {

  @Test
  public void simplePoolFromUrlUsrPwd() {

    try (ODatabasePool pool = ODatabasePool.open("embedded:/test", "admin", "admin")) {
      try (ODatabaseDocument database = pool.acquire()) {

      }
    }

  }

  @Test
  public void simplePoolFromUrl() {
    //This url is a bit strange now, if you try to create a database PLOCAL will give you an error
    //For a plocal database you need a proper path before the firs /, example: "embedded:./test&user=admin&password=admin&type=PLOCAL" note the "."
    try (ODatabasePool pool = ODatabasePool.open("embedded:/test&user=admin&password=admin&type=MEMORY")) {
      try (ODatabaseDocument database = pool.acquire()) {

      }
    }

  }

}