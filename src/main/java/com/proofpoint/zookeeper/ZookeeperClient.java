package com.proofpoint.zookeeper;

import com.google.common.base.Predicate;
import com.google.inject.Inject;
import com.proofpoint.concurrent.events.EventQueue;
import com.proofpoint.crossprocess.CrossProcessLock;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;

/**
 * A wrapper around ZooKeeper that makes ZK more manageable and adds some higher level features
 */
public class ZookeeperClient implements ZookeeperClientHelper
{
    private final AtomicReference<ZookeeperClientErrorHandler> errorHandler;

    private final AtomicBoolean started;
    private final RetryPolicy retryPolicy;
    private final EventQueue<ZookeeperEvent> eventQueue;
    private final CreateMode createMode;
    private final boolean inBackground;
    private final Object key;
    private final boolean watched;
    private final int dataVersion;
    private final Object context;
    private final ConnectionState state;
    private final Map<EventQueue.EventListener<ZookeeperEvent>, EventQueue.EventListener<ZookeeperEvent>> externalToInternalListenerMap = new ConcurrentHashMap<EventQueue.EventListener<ZookeeperEvent>, EventQueue.EventListener<ZookeeperEvent>>();
    private final ZookeeperClientConfig config;
    private final Watcher overrideWatcher;

    @Inject
    public ZookeeperClient(ZookeeperClientConfig config)
            throws IOException
    {
        this
        (
            null,
            config,
            newRetryPolicy(config),
            new AtomicBoolean(false),
            new EventQueue<ZookeeperEvent>(0),
            // no wait for events - process immediately
            CreateMode.PERSISTENT,
            false,
            null,
            false,
            -1,
            null,
            new AtomicReference<ZookeeperClientErrorHandler>(null),
            null
        );
    }

    private static RetryPolicy newRetryPolicy(ZookeeperClientConfig config)
    {
        RetryPolicy policy = RetryPolicies.exponentialBackoffRetry(config.getMaxConnectionLossRetries(), config.getConnectionLossSleepInMs(), TimeUnit.MILLISECONDS);
        Map<Class<? extends Exception>, RetryPolicy> map = new HashMap<Class<? extends Exception>, RetryPolicy>();
        map.put(KeeperException.ConnectionLossException.class, policy);
        return RetryPolicies.retryByException(RetryPolicies.TRY_ONCE_THEN_FAIL, map);
    }

    /**
     * Change/set the error handler for this client
     *
     * @param errorHandler handler
     */
    @Inject(optional = true)
    public void setErrorHandler(ZookeeperClientErrorHandler errorHandler)
    {
        this.errorHandler.set(errorHandler);
    }

    /**
     * Returns a copy of the ZK session info
     *
     * @return session info
     * @throws Exception errors
     */
    public ZookeeperSessionID getSessionInfo()
            throws Exception
    {
        RetryHandler.Call<ZookeeperSessionID> backgroundCall = new RetryHandler.Call<ZookeeperSessionID>() {
            @Override
            public ZookeeperSessionID call(ZooKeeper client, RetryHandler<ZookeeperSessionID> zookeeperSessionIDRetryHandler)
                    throws Exception
            {
                ZookeeperSessionID sessionID = new ZookeeperSessionID();
                sessionID.setPassword(client.getSessionPasswd());
                sessionID.setSessionId(client.getSessionId());
                return sessionID;
            }
        };
        return RetryHandler.makeAndStart(this, retryPolicy, backgroundCall);
    }

    private ZookeeperClient(
            ConnectionState state,
            ZookeeperClientConfig config,
            RetryPolicy retryPolicy,
            AtomicBoolean started,
            EventQueue<ZookeeperEvent> eventQueue,
            CreateMode createMode,
            boolean inBackground,
            Object key,
            boolean watched,
            int dataVersion,
            Object context,
            AtomicReference<ZookeeperClientErrorHandler> errorHandler,
            Watcher overrideWatcher
    )
    {
        this.state = (state == null) ? new ConnectionState(this, config) : state;
        this.config = config;
        this.retryPolicy = retryPolicy;
        this.started = started;
        this.eventQueue = eventQueue;
        this.createMode = createMode;
        this.inBackground = inBackground;
        this.key = key;
        this.watched = watched;
        this.dataVersion = dataVersion;
        this.context = context;
        this.errorHandler = errorHandler;
        this.overrideWatcher = overrideWatcher;
    }

    /**
     * Builder-style method that returns a view of the client with the given retry policy
     *
     * @param retryPolicy new retry policy
     * @return new instance with the given policy
     */
    public ZookeeperClient setRetryPolicy(RetryPolicy retryPolicy)
    {
        return new ZookeeperClient(state, config, retryPolicy, started, eventQueue, createMode, inBackground, key, watched, dataVersion, context, errorHandler, overrideWatcher);
    }

    @Override
    public ZookeeperClientHelper withCreateMode(CreateMode createMode)
    {
        return new ZookeeperClient(state, config, retryPolicy, started, eventQueue, createMode, inBackground, key, watched, dataVersion, context, errorHandler, overrideWatcher);
    }

    @Override
    public ZookeeperClientHelper inBackground(Object key)
    {
        return new ZookeeperClient(state, config, retryPolicy, started, eventQueue, createMode, true, key, watched, dataVersion, context, errorHandler, overrideWatcher);
    }

    @Override
    public ZookeeperClientHelper watched()
    {
        return new ZookeeperClient(state, config, retryPolicy, started, eventQueue, createMode, inBackground, key, true, dataVersion, context, errorHandler, overrideWatcher);
    }

    @Override
    public ZookeeperClientHelper withContext(Object contextArg)
    {
        return new ZookeeperClient(state, config, retryPolicy, started, eventQueue, createMode, inBackground, key, watched, dataVersion, contextArg, errorHandler, overrideWatcher);
    }

    @Override
    public ZookeeperClientHelper dataVersion(int version)
    {
        return new ZookeeperClient(state, config, retryPolicy, started, eventQueue, createMode, inBackground, key, watched, version, context, errorHandler, overrideWatcher);
    }

    @Override
    public ZookeeperClientHelper usingWatcher(Watcher watcher)
    {
        return new ZookeeperClient(state, config, retryPolicy, started, eventQueue, createMode, inBackground, key, watched, dataVersion, context, errorHandler, watcher);
    }

    @PreDestroy
    public void closeForShutdown()
    {
        state.close();
    }

    /**
     * Make this client a NOP zombie
     */
    public void setZombie()
    {
        state.setZombie();
    }

    /**
     * Add an event listener that listens for events starting with the given path and/or use the given key. i.e. if the
     * event's path starts with <code>basePath</code> or the event's key is equal to <code>backgroundKey</code> then the
     * event will be sent to the listener
     *
     * @param listener listener
     * @param basePath base path - can be null
     * @param backgroundKey background key (see {@link #inBackground(Object)}) - can be null
     */
    public void addListener(final EventQueue.EventListener<ZookeeperEvent> listener, final String basePath, final Object backgroundKey)
    {
        addListener(listener, new Predicate<ZookeeperEvent>()
        {
            @Override
            public boolean apply(ZookeeperEvent event)
            {
                boolean applies = true;
                do {
                    if ((basePath != null) && (event.getPath() != null)) {
                        if (!event.getPath().startsWith(basePath)) {
                            applies = false;
                            break;
                        }
                    }

                    if ((backgroundKey != null) && (event.getKey() != null)) {
                        if (!event.getKey().equals(backgroundKey)) {
                            applies = false;
                            break;
                        }
                    }
                } while (false);

                return applies;
            }
        });
    }

    /**
     * Add an event queue listener
     *
     * @param listener the listener
     * @param predicate functor that decides which events to pass through to the listener
     */
    public void addListener(final EventQueue.EventListener<ZookeeperEvent> listener, final Predicate<ZookeeperEvent> predicate)
    {
        EventQueue.EventListener<ZookeeperEvent> internalListener = new EventQueue.EventListener<com.proofpoint.zookeeper.ZookeeperEvent>()
        {
            @Override
            public void eventProcessed(ZookeeperEvent event)
                    throws Exception
            {
                if (predicate.apply(event)) {
                    listener.eventProcessed(event);
                }
            }
        };
        externalToInternalListenerMap.put(listener, internalListener);
        eventQueue.addListener(internalListener);
    }

    /**
     * Remove an event queue listener
     *
     * @param listener the listener to remove
     */
    public void removeListener(EventQueue.EventListener<ZookeeperEvent> listener)
    {
        EventQueue.EventListener<ZookeeperEvent> internalListener = externalToInternalListenerMap.remove(listener);
        if (internalListener != null) {
            eventQueue.removeListener(internalListener);
        }
    }

    @Override
    public List<String> getChildren(final String path)
            throws Exception
    {
        if (inBackground) {
            RetryHandler.Call<Void> backgroundCall = new RetryHandler.Call<Void>() {
                @Override
                public Void call(ZooKeeper client, final RetryHandler<Void> retryHandler)
                        throws Exception
                {
                    if ( overrideWatcher != null ) {
                        client.getChildren(path, overrideWatcher, new RetryChildrenCallback(retryHandler), context);
                    }
                    else {
                        client.getChildren(path, watched, new RetryChildrenCallback(retryHandler), context);
                    }
                    return null;
                }
            };
            RetryHandler.makeAndStart(this, retryPolicy, backgroundCall);
            return null;
        }

        RetryHandler.Call<List<String>> backgroundCall = new RetryHandler.Call<List<String>>() {
            @Override
            public List<String> call(ZooKeeper client, RetryHandler<List<String>> listRetryHandler)
                    throws Exception
            {
                return (overrideWatcher != null) ? client.getChildren(path, overrideWatcher) : client.getChildren(path, watched);
            }
        };
        return RetryHandler.makeAndStart(this, retryPolicy, backgroundCall);
    }

    @Override
    public DataAndStat getDataAndStat(final String path)
            throws Exception
    {
        if (inBackground) {
            RetryHandler.Call<Void> backgroundCall = new RetryHandler.Call<Void>() {
                @Override
                public Void call(ZooKeeper client, final RetryHandler<Void> retryHandler)
                        throws Exception
                {
                    if ( overrideWatcher != null ) {
                        client.getData(path, overrideWatcher, new RetryDataCallback(retryHandler), context);
                    }
                    else {
                        client.getData(path, watched, new RetryDataCallback(retryHandler), context);
                    }
                    return null;
                }
            };
            RetryHandler.makeAndStart(this, retryPolicy, backgroundCall);
            return null;
        }

        RetryHandler.Call<DataAndStat> backgroundCall = new RetryHandler.Call<DataAndStat>() {
            @Override
            public DataAndStat call(ZooKeeper client, RetryHandler<DataAndStat> dataAndStatRetryHandler)
                    throws Exception
            {
                final Stat stat = new Stat();
                final byte[] data = (overrideWatcher != null) ? client.getData(path, overrideWatcher, stat) : client.getData(path, watched, stat);
                return new DataAndStat()
                {
                    @Override
                    public Stat getStat()
                    {
                        return stat;
                    }

                    @Override
                    public byte[] getData()
                    {
                        return data;
                    }
                };
            }
        };
        return RetryHandler.makeAndStart(this, retryPolicy, backgroundCall);
    }

    @Override
    public byte[] getData(final String path)
            throws Exception
    {
        DataAndStat dataAndStat = getDataAndStat(path);
        return (dataAndStat != null) ? dataAndStat.getData() : null;
    }

    private void preconditionNotWatched()
    {
        if ( watched || (overrideWatcher != null) ) {
            throw new IllegalArgumentException("Watchers do not apply to this method");
        }
    }

    private void preconditionNotInBackground()
    {
        if ( inBackground ) {
            throw new IllegalArgumentException("Cannot be in background");
        }
    }

    @Override
    public String create(final String path, final byte data[])
            throws Exception
    {
        preconditionNotWatched();

        if (inBackground) {
            RetryHandler.Call<Void> backgroundCall = new RetryHandler.Call<Void>()
            {
                @Override
                public Void call(ZooKeeper client, final RetryHandler retryHandler)
                {
                    client.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode, new RetryStringCallback(retryHandler), context);
                    return null;
                }
            };
            RetryHandler.makeAndStart(this, retryPolicy, backgroundCall);

            return null;
        }

        RetryHandler.Call<String> backgroundCall = new RetryHandler.Call<String>() {
            @Override
            public String call(ZooKeeper client, RetryHandler<String> stringRetryHandler)
                    throws Exception
            {
                return client.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
            }
        };
        return RetryHandler.makeAndStart(this, retryPolicy, backgroundCall);
    }

    @Override
    public Stat exists(final String path)
            throws Exception
    {
        if (inBackground) {
            RetryHandler.Call<Void> backgroundCall = new RetryHandler.Call<Void>()
            {
                @Override
                public Void call(ZooKeeper client, final RetryHandler retryHandler)
                {
                    if ( overrideWatcher != null ) {
                        client.exists(path, overrideWatcher, new RetryStatCallback(retryHandler), context);
                    }
                    else {
                        client.exists(path, watched, new RetryStatCallback(retryHandler), context);
                    }
                    return null;
                }
            };
            RetryHandler.makeAndStart(this, retryPolicy, backgroundCall);
            return null;
        }

        RetryHandler.Call<Stat> backgroundCall = new RetryHandler.Call<Stat>() {
            @Override
            public Stat call(ZooKeeper client, RetryHandler<Stat> statRetryHandler)
                    throws Exception
            {
                return (overrideWatcher != null) ? client.exists(path, overrideWatcher) : client.exists(path, watched);
            }
        };
        return RetryHandler.makeAndStart(this, retryPolicy, backgroundCall);
    }

    @Override
    public void sync(final String path)
            throws Exception
    {
        preconditionNotWatched();
        preconditionInBackground();

        RetryHandler.Call<Void> backgroundCall = new RetryHandler.Call<Void>()
        {
            @Override
            public Void call(ZooKeeper client, final RetryHandler retryHandler)
            {
                client.sync(
                        path,
                        new AsyncCallback.VoidCallback()
                        {
                            @Override
                            public void processResult(int rc, String path, Object ctx)
                            {
                                if ( retryHandler.okToContinue(rc) )
                                {
                                    eventQueue.postEvent(new ZookeeperEvent(ZookeeperEvent.Type.SYNC, rc, path, ctx, null, null, null, null, key));
                                }
                            }
                        },
                        context
                );
                return null;
            }
        };
        RetryHandler.makeAndStart(this, retryPolicy, backgroundCall);
    }

    private void preconditionInBackground()
            throws Exception
    {
        if (!inBackground) {
            throw new Exception("Must be called inBackground()");
        }
    }

    @Override
    public Stat setData(final String path, final byte data[])
            throws Exception
    {
        preconditionNotWatched();

        if (inBackground) {
            RetryHandler.Call<Void> backgroundCall = new RetryHandler.Call<Void>()
            {
                @Override
                public Void call(ZooKeeper client, final RetryHandler retryHandler)
                {
                    client.setData(
                            path,
                            data,
                            dataVersion,
                            new AsyncCallback.StatCallback()
                            {
                                @Override
                                public void processResult(int rc, String path, Object ctx, Stat stat)
                                {
                                    if ( retryHandler.okToContinue(rc) )
                                    {
                                        eventQueue.postEvent(new ZookeeperEvent(ZookeeperEvent.Type.SET_DATA, rc, path, ctx, null, stat, null, null, key));
                                    }
                                }
                            },
                            context
                    );
                    return null;
                }
            };
            RetryHandler.makeAndStart(this, retryPolicy, backgroundCall);
            return null;
        }

        RetryHandler.Call<Stat> backgroundCall = new RetryHandler.Call<Stat>() {
            @Override
            public Stat call(ZooKeeper client, RetryHandler<Stat> statRetryHandler)
                    throws Exception
            {
                return client.setData(path, data, dataVersion);
            }
        };
        return RetryHandler.makeAndStart(this, retryPolicy, backgroundCall);
    }

    @Override
    public void delete(final String path)
            throws Exception
    {
        preconditionNotWatched();

        if (inBackground) {
            RetryHandler.Call<Void> backgroundCall = new RetryHandler.Call<Void>()
            {
                @Override
                public Void call(ZooKeeper client, final RetryHandler retryHandler)
                {
                    client.delete(
                            path,
                            dataVersion,
                            new AsyncCallback.VoidCallback()
                            {
                                @Override
                                public void processResult(int rc, String path, Object ctx)
                                {
                                    if ( retryHandler.okToContinue(rc) )
                                    {
                                        eventQueue.postEvent(new ZookeeperEvent(ZookeeperEvent.Type.DELETE, rc, path, ctx, null, null, null, null, key));
                                    }
                                }
                            },
                            context
                    );
                    return null;
                }
            };
            RetryHandler.makeAndStart(this, retryPolicy, backgroundCall);
        }
        else {
            RetryHandler.Call<Void> backgroundCall = new RetryHandler.Call<Void>() {
                @Override
                public Void call(ZooKeeper client, RetryHandler<Void> voidRetryHandler)
                        throws Exception
                {
                    client.delete(path, dataVersion);
                    return null;
                }
            };
            RetryHandler.makeAndStart(this, retryPolicy, backgroundCall);
        }
    }

    /**
     * Return the children of the given path sorted by sequence number
     *
     * @param path the path
     * @return sorted list of children
     * @throws Exception errors
     */
    public List<String> getSortedChildren(final String path)
            throws Exception
    {
        preconditionNotWatched();
        preconditionNotInBackground();

        RetryHandler.Call<List<String>> backgroundCall = new RetryHandler.Call<List<String>>() {
            @Override
            public List<String> call(ZooKeeper client, RetryHandler<List<String>> listRetryHandler)
                    throws Exception
            {
                return ZookeeperUtils.getSortedChildren(client, path);
            }
        };
        return RetryHandler.makeAndStart(this, retryPolicy, backgroundCall);
    }

    /**
     * Make sure all the nodes in the path are created. NOTE: Unlike File.mkdirs(), Zookeeper doesn't distinguish
     * between directories and files. So, every node in the path is created. The data for each node is an empty blob
     *
     * @param path path to ensure
     * @throws Exception errors
     */
    public void mkdirs(final String path)
            throws Exception
    {
        preconditionNotWatched();
        preconditionNotInBackground();

        RetryHandler.Call<Void> backgroundCall = new RetryHandler.Call<Void>() {
            @Override
            public Void call(ZooKeeper client, RetryHandler<Void> voidRetryHandler)
                    throws Exception
            {
                ZookeeperUtils.mkdirs(client, path);
                return null;
            }
        };
        RetryHandler.makeAndStart(this, retryPolicy, backgroundCall);
    }

    /**
     * Given a parent path and a child node, create a combined full path
     *
     * @param parent the parent
     * @param child the child
     * @return full path
     */
    public String makePath(String parent, String child)
    {
        preconditionNotWatched();

        return ZookeeperUtils.makePath(parent, child);
    }

    /**
     * Given a parent path and a child node, create a combined full path
     *
     * @param parent the parent
     * @param child the child
     * @return full path
     */
    public static String staticMakePath(String parent, String child)
    {
        return ZookeeperUtils.makePath(parent, child);
    }

    ConnectionState getState()
    {
        return state;
    }

    CrossProcessLock newLock(final String path)
            throws Exception
    {
        return new CrossProcessLock()
        {
            @Override
            public void lock()
            {
                getLock().lock();
            }

            @Override
            public boolean isLocked()
                    throws Exception
            {
                return getLock().isLocked();
            }

            @Override
            public boolean tryLock()
            {
                return getLock().tryLock();
            }

            @Override
            public void unlock()
            {
                getLock().unlock();
            }

            @Override
            public void lockInterruptibly()
                    throws InterruptedException
            {
                getLock().lockInterruptibly();
            }

            @Override
            public boolean tryLock(long time, TimeUnit unit)
                    throws InterruptedException
            {
                return getLock().tryLock(time, unit);
            }

            @Override
            public Condition newCondition()
            {
                return getLock().newCondition();
            }

            private CrossProcessLock getLock()
            {
                final Object        thisRef = this;
                RetryHandler.Call<CrossProcessLock> backgroundCall = new RetryHandler.Call<CrossProcessLock>() {
                    @Override
                    public CrossProcessLock call(ZooKeeper client, RetryHandler<CrossProcessLock> crossProcessLockRetryHandler)
                            throws Exception
                    {
                        synchronized(thisRef) {
                            if (lock == null) {
                                lock = new CrossProcessLockImp(client, path);
                            }
                            return lock;
                        }
                    }
                };
                try
                {
                    return RetryHandler.makeAndStart(ZookeeperClient.this, retryPolicy, backgroundCall);
                }
                catch ( Exception e )
                {
                    throw new RuntimeException(e);
                }
            }

            private CrossProcessLock lock = null;
        };
    }

    void errorConnectionLost()
    {
        ZookeeperClientErrorHandler localErrorHandler = errorHandler.get();
        if (localErrorHandler != null) {
            localErrorHandler.connectionLost(this);
        }
    }

    void     postEvent(ZookeeperEvent event)
    {
        eventQueue.postEvent(event);
    }

    private class RetryChildrenCallback
            implements AsyncCallback.Children2Callback
    {
        private final RetryHandler<Void> retryHandler;

        public RetryChildrenCallback(RetryHandler<Void> retryHandler)
        {
            this.retryHandler = retryHandler;
        }

        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat)
        {
            if ( retryHandler.okToContinue(rc) ) {
                eventQueue.postEvent(new ZookeeperEvent(ZookeeperEvent.Type.GET_CHILDREN, rc, path, ctx, null, stat, null, children, key));
            }
        }
    }

    private class RetryDataCallback
            implements AsyncCallback.DataCallback
    {
        private final RetryHandler<Void> retryHandler;

        public RetryDataCallback(RetryHandler<Void> retryHandler)
        {
            this.retryHandler = retryHandler;
        }

        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat)
        {
            if ( retryHandler.okToContinue(rc) ) {
                eventQueue.postEvent(new ZookeeperEvent(ZookeeperEvent.Type.GET_DATA, rc, path, ctx, data, stat, null, null, key));
            }
        }
    }

    private class RetryStringCallback
            implements AsyncCallback.StringCallback
    {
        private final RetryHandler retryHandler;

        public RetryStringCallback(RetryHandler retryHandler)
        {
            this.retryHandler = retryHandler;
        }

        @Override
        public void processResult(int rc, String path, Object ctx, String name)
        {
            if ( retryHandler.okToContinue(rc) ) {
                eventQueue.postEvent(new ZookeeperEvent(ZookeeperEvent.Type.CREATE, rc, path, ctx, null, null, name, null, key));
            }
        }
    }

    private class RetryStatCallback
            implements AsyncCallback.StatCallback
    {
        private final RetryHandler retryHandler;

        public RetryStatCallback(RetryHandler retryHandler)
        {
            this.retryHandler = retryHandler;
        }

        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat)
        {
            if ( retryHandler.okToContinue(rc) ) {
                eventQueue.postEvent(new ZookeeperEvent(ZookeeperEvent.Type.EXISTS, rc, path, ctx, null, stat, null, null, key));
            }
        }
    }
}
