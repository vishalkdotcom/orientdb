package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.cache.OCommandCacheHook;
import com.orientechnologies.orient.core.cache.OLocalRecordCache;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.metadata.OMetadataDefault;
import com.orientechnologies.orient.core.metadata.security.*;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializerFactory;
import com.orientechnologies.orient.core.serialization.serializer.record.string.ORecordSerializerSchemaAware2CSV;
import com.orientechnologies.orient.core.storage.OStorage;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Created by tglman on 27/06/16.
 */
public class ODatabaseDocumentEmbedded extends ODatabaseDocumentTx {

  public ODatabaseDocumentEmbedded(final OStorage storage) {
    activateOnCurrentThread();

    try {
      status = STATUS.CLOSED;

      // OVERWRITE THE URL
      url = storage.getURL();
      this.storage = storage;
      this.componentsFactory = storage.getComponentsFactory();

      unmodifiableHooks = Collections.unmodifiableMap(hooks);

      localCache = new OLocalRecordCache();

      init();

      databaseOwner = this;
    } catch (Exception t) {
      ODatabaseRecordThreadLocal.INSTANCE.remove();

      throw OException.wrapException(new ODatabaseException("Error on opening database "), t);
    }

    setSerializer(defaultSerializer);
  }

  public <DB extends ODatabase> DB open(final String iUserName, final String iUserPassword) {
    throw new UnsupportedOperationException("Use OrientDBFactory");
  }

  public void internalOpen(final String iUserName, final String iUserPassword){
    internalOpen(iUserName, iUserPassword,true);
  }
  
  private void internalOpen(final String iUserName, final String iUserPassword,boolean checkPassword) {
    boolean failure = true;
    setupThreadOwner();
    activateOnCurrentThread();
    try {

      if (user != null && !user.getName().equals(iUserName))
        initialized = false;

      status = STATUS.OPEN;

      initAtFirstOpen();

      final OSecurity security = metadata.getSecurity();
      
      if (user == null || user.getVersion() != security.getVersion() || !user.getName().equalsIgnoreCase(iUserName)) {
        final OUser usr;
        if (checkPassword) {
          usr = metadata.getSecurity().authenticate(iUserName, iUserPassword);
        } else {
            usr = metadata.getSecurity().getUser(iUserName);
        }
        if (usr != null)
          user = new OImmutableUser(security.getVersion(), usr);
        else
          user = null;

        checkSecurity(ORule.ResourceGeneric.DATABASE, ORole.PERMISSION_READ);
      }

      // WAKE UP LISTENERS
      callOnOpenListeners();

      failure = false;
    } catch (OException e) {
      close();
      throw e;
    } catch (Exception e) {
      close();
      throw OException.wrapException(new ODatabaseException("Cannot open database url=" + getURL()), e);
    } finally {
      if (failure)
        owner.set(null);
    }
  }

  /**
   * Opens a database using an authentication token received as an argument.
   *
   * @param iToken Authentication token
   * @return The Database instance itself giving a "fluent interface". Useful to call multiple methods in chain.
   */
  @Deprecated
  public <DB extends ODatabase> DB open(final OToken iToken) {
    throw new UnsupportedOperationException("Deprecated Method");
  }

  @Override
  public <DB extends ODatabase> DB create() {
    throw new UnsupportedOperationException("Deprecated Method");
  }

  /**
   * {@inheritDoc}
   */
  public <DB extends ODatabase> DB internalCreate() {
    this.status = STATUS.OPEN;
    // THIS IF SHOULDN'T BE NEEDED, CREATE HAPPEN ONLY IN EMBEDDED

    metadata = new OMetadataDefault(this);
    installHooksEmbedded();
    // CREATE THE DEFAULT SCHEMA WITH DEFAULT USER
    OSharedContext shared = getStorage().getResource(OSharedContext.class.getName(), new Callable<OSharedContext>() {
      @Override
      public OSharedContext call() throws Exception {
        OSharedContext shared = new OSharedContext(getStorage());
        return shared;
      }
    });
    metadata.init(shared);
    shared.create(this);

    registerHook(new OCommandCacheHook(this), ORecordHook.HOOK_POSITION.REGULAR);
    registerHook(new OSecurityTrackerHook(metadata.getSecurity(), this), ORecordHook.HOOK_POSITION.LAST);

    // WAKE UP DB LIFECYCLE LISTENER
    for (Iterator<ODatabaseLifecycleListener> it = Orient.instance().getDbLifecycleListeners(); it.hasNext(); )
      it.next().onCreate(getDatabaseOwner());

    // WAKE UP LISTENERS
    for (ODatabaseListener listener : browseListeners())
      try {
        listener.onCreate(this);
      } catch (Throwable ignore) {
      }

    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public <DB extends ODatabase> DB create(String incrementalBackupPath) {
    throw new UnsupportedOperationException("use OrientDBFactory");
  }

  @Override
  public <DB extends ODatabase> DB create(final Map<OGlobalConfiguration, Object> iInitialSettings) {
    throw new UnsupportedOperationException("use OrientDBFactory");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void drop() {
    throw new UnsupportedOperationException("use OrientDBFactory");
  }

  /**
   * Returns a copy of current database if it's open. The returned instance can be used by another thread without affecting current
   * instance. The database copy is not set in thread local.
   */
  public ODatabaseDocumentTx copy() {
    ODatabaseDocumentEmbedded database = new ODatabaseDocumentEmbedded(storage);
    database.internalOpen(getUser().getName(), null, false);
    return database;
  }

  @Override
  public boolean exists() {
    throw new UnsupportedOperationException("use OrientDBFactory");
  }

  @Override
  public void close() {
    checkIfActive();

    localCache.shutdown();

    if (isClosed()) {
      status = STATUS.CLOSED;
      return;
    }

    try {
      commit(true);
    } catch (Exception e) {
      OLogManager.instance().error(this, "Exception during commit of active transaction", e);
    }

    if (status != STATUS.OPEN)
      return;

    callOnCloseListeners();

    if (currentIntent != null) {
      currentIntent.end(this);
      currentIntent = null;
    }
    sharedContext = null;
    status = STATUS.CLOSED;

    localCache.clear();

    if (!keepStorageOpen && storage != null)
      storage.close();

    ODatabaseRecordThreadLocal.INSTANCE.remove();
    clearOwner();
  }

  @Override
  public boolean isClosed() {
    return status == STATUS.CLOSED || storage.isClosed();
  }

  private void initAtFirstOpen() {
    if (initialized)
      return;

    ORecordSerializerFactory serializerFactory = ORecordSerializerFactory.instance();
    String serializeName = getStorage().getConfiguration().getRecordSerializer();
    if (serializeName == null)
      serializeName = ORecordSerializerSchemaAware2CSV.NAME;
    serializer = serializerFactory.getFormat(serializeName);
    if (serializer == null)
      throw new ODatabaseException("RecordSerializer with name '" + serializeName + "' not found ");
    if (getStorage().getConfiguration().getRecordSerializerVersion() > serializer.getMinSupportedVersion())
      throw new ODatabaseException("Persistent record serializer version is not support by the current implementation");

    localCache.startup();

    loadMetadata();

    if (metadata.getIndexManager().autoRecreateIndexesAfterCrash()) {
      metadata.getIndexManager().recreateIndexes();
    }

    installHooksEmbedded();
    registerHook(new OCommandCacheHook(this), ORecordHook.HOOK_POSITION.REGULAR);
    registerHook(new OSecurityTrackerHook(metadata.getSecurity(), this), ORecordHook.HOOK_POSITION.LAST);

    user = null;

    initialized = true;
  }

}

