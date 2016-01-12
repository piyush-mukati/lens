package org.apache.lens.client;


import java.util.concurrent.ConcurrentHashMap;

import org.apache.lens.api.LensSessionHandle;

import com.google.common.base.Preconditions;

public class LensClientSessionResolverImpl implements LensClientSessionResolver {
  private LensConnection connection;
  final ConcurrentHashMap<String, LensSessionHandle> sessionStore;

  public LensClientSessionResolverImpl() {
    this.sessionStore = new ConcurrentHashMap<String, LensSessionHandle>();
  }

  @Override
  public LensSessionHandle getSession(LensClientUserContext context) {
    if (!sessionStore.containsKey(context.getUserName())) {
      Preconditions.checkNotNull(connection, this.getClass() + " is not initialised");
      final LensSessionHandle sessionHandle = connection.createSession(context.getUserName(), context.getPassWord());
      sessionStore.put(context.getUserName(), sessionHandle);
    }
    return sessionStore.get(context.getUserName());
  }

  @Override
  public void init(LensConnection connection) {
    Preconditions.checkNotNull(connection, "connection can not be null");
    this.connection = connection;
  }

  @Override
  public void close() {
   for (LensSessionHandle session : sessionStore.values()){
    connection.closeSession(session);
   }

  }


}
