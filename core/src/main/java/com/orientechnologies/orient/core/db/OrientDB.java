/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;

import java.util.ArrayList;
import java.util.List;

/**
 * OrientDB management environment, it allow to connect to an environment and manipulate databases or open sessions.
 * <p>
 * Usage example:
 * <p>
 * remote ex:
 * <code>
 * OrientDB orientDb = new OrientDB("remote:localhost","root","root");
 * if(orientDb.createIfNotExists("test",ODatabaseType.MEMORY)){
 * ODatabaseDocument session = orientDb.open("test","admin","admin");
 * session.createClass("MyClass");
 * session.close();
 * }
 * ODatabaseDocument session = orientDb.open("test","writer","writer");
 * //...
 * session.close();
 * orientDb.close();
 * </code>
 * <p>
 * embedded ex:
 * <code>
 * OrientDB orientDb = new OrientDB("embedded:./databases/",null,null);
 * orientDb.create("test",ODatabaseType.PLOCAL);
 * ODatabaseDocument session = orientDb.open("test","admin","admin");
 * //...
 * session.close();
 * orientDb.close();
 * </code>
 * <p>
 * database manipulation ex:
 * <p>
 * <code>
 * OrientDB orientDb = ...
 * if(!orientDb.exists("one")){
 * orientDb.create("one",ODatabaseType.PLOCAL);
 * }
 * if(orientDb.exists("two")){
 * orientDb.drop("two");
 * }
 * List&ltString&gt databases = orientDb.list();
 * assertEquals(databases.size(),1);
 * assertEquals(databases.get("0"),"one");
 * </code>
 * <p>
 * <p>
 * <p>
 * Created by tglman on 08/02/17.
 */
public class OrientDB implements AutoCloseable {

  private OrientDBInternal internal;
  private String           serverUser;
  private String           serverPassword;

  /**
   * Create a new OrientDb instance for a specific environment
   * <p/>
   * possible kind of urls 'embedded','remote', for the case of remote and distributed can be specified multiple nodes
   * using comma.
   * <p>
   * remote ex:
   * <code>
   * OrientDB orientDb = new OrientDB("remote:localhost");
   * ODatabaseDocument session = orientDb.open("test","admin","admin");
   * //...
   * session.close();
   * orientDb.close();
   * </code>
   * <p>
   * embedded ex:
   * <code>
   * OrientDB orientDb = new OrientDB("embedded:./databases/");
   * ODatabaseDocument session = orientDb.open("test","admin","admin");
   * //...
   * session.close();
   * orientDb.close();
   * </code>
   *
   * @param url           the url for the specific factory.
   * @param configuration configuration for the specific factory for the list of option {@see OGlobalConfiguration}.
   *
   * @return the new Orient Factory.
   */
  public OrientDB(String url, OrientDBConfig configuration) {
    this(url, null, null, configuration);
  }

  /**
   * Create a new OrientDb instance for a specific environment
   * <p/>
   * possible kind of urls 'embedded','remote', for the case of remote and distributed can be specified multiple nodes
   * using comma.
   * <p>
   * remote ex:
   * <code>
   * OrientDB orientDb = new OrientDB("remote:localhost","root","root");
   * orientDb.create("test",ODatabaseType.PLOCAL);
   * ODatabaseDocument session = orientDb.open("test","admin","admin");
   * //...
   * session.close();
   * orientDb.close();
   * </code>
   * <p>
   * embedded ex:
   * <code>
   * OrientDB orientDb = new OrientDB("embedded:./databases/",null,null);
   * orientDb.create("test",ODatabaseType.MEMORY);
   * ODatabaseDocument session = orientDb.open("test","admin","admin");
   * //...
   * session.close();
   * orientDb.close();
   * </code>
   *
   * @param url            the url for the specific factory.
   * @param serverUser     the server user allowed to manipulate databases.
   * @param serverPassword relative to the server user.
   * @param configuration  configuration for the specific factory for the list of option {@see OGlobalConfiguration}.
   *
   * @return the new Orient Factory.
   */
  public OrientDB(String url, String serverUser, String serverPassword, OrientDBConfig configuration) {
    String what = url.substring(0, url.indexOf(':'));
    if ("embedded".equals(what))
      internal = OrientDBInternal.embedded(url.substring(url.indexOf(':') + 1), configuration);
    else if ("remote".equals(what))
      internal = OrientDBInternal.remote(url.substring(url.indexOf(':') + 1).split(","), configuration);

    this.serverUser = serverUser;
    this.serverPassword = serverPassword;
  }

  OrientDB(OrientDBInternal internal) {
    this.internal = internal;
    this.serverUser = null;
    this.serverPassword = null;
  }

  /**
   * Open a database
   *
   * @param database the database to open
   * @param user     username of a database user or a server user allowed to open the database
   * @param password related to the specified username
   *
   * @return the opened database
   */
  public ODatabaseDocument open(String database, String user, String password) {
    return internal.open(database, user, password);
  }

  /**
   * Create a new database
   *
   * @param database database name
   * @param type     can be plocal or memory
   */
  public void create(String database, ODatabaseType type) {
    this.internal.create(database, serverUser, serverPassword, type);
  }

  /**
   * Create a new database if not exists
   *
   * @param database database name
   * @param type     can be plocal or memory
   *
   * @return true if the database has been created, false if already exists
   */
  public boolean createIfNotExists(String database, ODatabaseType type) {
    if (!this.internal.exists(database, serverUser, serverPassword)) {
      this.internal.create(database, serverUser, serverPassword, type);
      return true;
    }
    return false;
  }

  /**
   * Drop a database
   *
   * @param database database name
   */
  public void drop(String database) {
    this.internal.drop(database, serverUser, serverPassword);
  }

  /**
   * Check if a database exists
   *
   * @param database database name to check
   *
   * @return boolean true if exist false otherwise.
   */
  public boolean exists(String database) {
    return this.internal.exists(database, serverUser, serverPassword);
  }

  /**
   * List exiting databases in the current environment
   *
   * @return a list of existing databases.
   */
  public List<String> list() {
    return new ArrayList<>(this.internal.listDatabases(serverUser, serverPassword));
  }

  /**
   * Close the current OrientDB context with all related databases and pools.
   */
  @Override
  public void close() {
    this.internal.close();
  }

  ODatabasePoolInternal openPool(String database, String user, String password) {
    return this.internal.openPool(database, user, password);
  }

  OrientDBInternal getInternal() {
    return internal;
  }
}
