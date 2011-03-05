package com.proofpoint.zookeeper;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.proofpoint.log.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;

class ConnectionState implements Watcher
{
    private final Logger log = Logger.get(getClass());

    private final ZookeeperClient client;
    private final ZookeeperClientConfig config;

    // guarded by synchronization on ensureConnected() and ensureStarted()
    private ZooKeeper      zookeeper = null;

    // guarded by synchronization on ensureConnected() and ensureStarted()
    private boolean        isConnected = false;

    // guarded by synchronization
    private boolean        isZombie = false;

    // guarded by synchronization
    private long           connectionStartTicks;

    ConnectionState(ZookeeperClient client, ZookeeperClientConfig config)
    {
        this.client = client;
        this.config = config;
    }

    synchronized boolean isZombie()
    {
        return isZombie;
    }

    synchronized void setZombie()
    {
        isZombie = true;
    }

    synchronized void close()
    {
        if ( zookeeper != null ) {
            try {
                zookeeper.close();
            }
            catch ( InterruptedException e ) {
                Thread.currentThread().interrupt();
                log.warn(e, "Interrupted while closing zookeeper");
            }
        }
        isConnected = false;
        clear();
        notifyAll();
    }

    private synchronized void clear()
    {
        zookeeper = null;
        connectionStartTicks = 0;
    }

    synchronized ZooKeeper ensureCreated()
            throws IOException
    {
        if ( (zookeeper == null) && !isZombie ) {
            ZookeeperSessionID session = readSessionId();
            zookeeper = internalCreate(session);
        }
        return zookeeper;
    }

    synchronized boolean ensureConnected()
            throws IOException, InterruptedException
    {
        if ( isZombie ) {
            return false;
        }
        ensureCreated();

        boolean     needsWriteSessionId = false;
        if ( !isConnected ) {
            long waitTicks;
            if ( connectionStartTicks == 0 ) {
                waitTicks = config.getConnectionTimeoutInMs();
                connectionStartTicks = System.currentTimeMillis();
            }
            else {
                long        elapsed = System.currentTimeMillis() - connectionStartTicks;
                waitTicks = config.getConnectionTimeoutInMs() - elapsed;
            }
            if ( waitTicks > 0 ) {
                wait(waitTicks);
            }
            needsWriteSessionId = isConnected;
        }

        if ( needsWriteSessionId ) {
            writeSessionId();
        }
        return isConnected;
    }

    private ZooKeeper internalCreate(ZookeeperSessionID session)
            throws IOException
    {
        ZooKeeper keeper;
        //noinspection LoopStatementThatDoesntLoop
        do {
            if (session != null) {
                try {
                    keeper = new ZooKeeper(config.getConnectionString(), config.getSessionTimeoutInMs(), this, session.getSessionId(), session.getPassword());
                    break;
                }
                catch (IOException e) {
                    log.warn(e, "Could not create Zookeeper with session: %s" + session);
                }
            }

            keeper = new ZooKeeper(config.getConnectionString(), config.getSessionTimeoutInMs(), this);
        } while (false);

        return keeper;
    }

    private ZookeeperSessionID readSessionId()
    {
        if ( config.getSessionStorePath() != null ) {
            File sessionIdFile = new File(config.getSessionStorePath());
            if (sessionIdFile.exists()) {
                try {
                    String sessionSpec = Files.toString(sessionIdFile, Charsets.UTF_8);
                    ObjectMapper mapper = new ObjectMapper();
                    return mapper.readValue(sessionSpec, ZookeeperSessionID.class);
                }
                catch (IOException e) {
                    log.warn(e, "Could not read session file: %s", config.getSessionStorePath());
                }
            }
        }
        return null;
    }

    private void writeSessionId()
    {
        if ( config.getSessionStorePath() != null ) {
            ZookeeperSessionID session = new ZookeeperSessionID();
            session.setPassword(zookeeper.getSessionPasswd());
            session.setSessionId(zookeeper.getSessionId());
            ObjectMapper mapper = new ObjectMapper();
            try {
                String sessionSpec = mapper.writeValueAsString(session);
                Files.write(sessionSpec, new File(config.getSessionStorePath()), Charsets.UTF_8);
            }
            catch (IOException e) {
                log.warn(e, "Couldn't write session info to: %s", config.getSessionStorePath());
            }
        }
    }

    @Override
    public void process(WatchedEvent event)
    {
        boolean     localIsConnected;
        synchronized(this) {
            boolean     needsNotify = false;
            if (event.getType() == Watcher.Event.EventType.None) {
                boolean     wasConnected = isConnected;
                isConnected = (event.getState() == Event.KeeperState.SyncConnected);
                needsNotify = isConnected && !wasConnected;

                if ( event.getState() == Event.KeeperState.Expired ) {
                    File sessionIdFile = new File(config.getSessionStorePath());
                    if ( !sessionIdFile.delete() ) {
                        log.error("Could not delete session ID file: " + config.getSessionStorePath());
                    }
                    needsNotify = true;
                }
                else if ( event.getState() == Event.KeeperState.Disconnected ) {
                    needsNotify = true;
                }
            }

            if ( needsNotify ) {
                if ( !isConnected && (zookeeper != null) ) {
                    try {
                        zookeeper.close();
                    }
                    catch ( InterruptedException e ) {
                        Thread.currentThread().interrupt();
                        log.error(e, "Interrupted trying to close Zookeeper. Ignoring at this level.");
                    }
                    clear();
                }

                notifyAll();
            }

            localIsConnected = isConnected;
        }

        if ( localIsConnected ) {
            client.postEvent(new ZookeeperEvent(getTypeFromWatched(event), 0, event.getPath(), null, null, null, null, null, null));
        }
    }

    private ZookeeperEvent.Type getTypeFromWatched(WatchedEvent event)
    {
        switch (event.getType()) {
            case None: {
                return ZookeeperEvent.Type.WATCHED_NONE;
            }

            case NodeCreated: {
                return ZookeeperEvent.Type.WATCHED_NODE_CREATED;
            }

            case NodeDeleted: {
                return ZookeeperEvent.Type.WATCHED_NODE_DELETED;
            }

            case NodeDataChanged: {
                return ZookeeperEvent.Type.WATCHED_NODE_DATA_CHANGED;
            }

            case NodeChildrenChanged: {
                return ZookeeperEvent.Type.WATCHED_NODE_CHILDREN_CHANGED;
            }
        }
        return ZookeeperEvent.Type.WATCHED_NONE;
    }
}
