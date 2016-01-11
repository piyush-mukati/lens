package org.apache.lens.client;


import java.util.concurrent.ConcurrentHashMap;

import org.apache.lens.api.LensSessionHandle;

public class LensClientSessionResolverImpl implements  LensClientSessionResolver{
  final LensConnection connection;
  final ConcurrentHashMap<String,LensSessionHandle> sessionStore;
  public LensClientSessionResolverImpl(LensConnection connection){
    this.connection=connection;
    this.sessionStore=new ConcurrentHashMap<String,LensSessionHandle>();
  }

  @Override
  public LensSessionHandle getSession(LensClientUserContext context) {
    if(!sessionStore.containsKey(context.getUserName())){
      sessionStore.put(context.getUserName(), connection.createSession(context.getUserName(), context.getPassWord()));
     }
    return  sessionStore.get(context.getUserName());
  }

}
