/*
 * Copyright 2010 Proofpoint, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.proofpoint.zookeeper;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.proofpoint.zookeeper.events.EventQueue;
import com.proofpoint.log.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.collect.Sets.difference;

/**
 * Mechanism for watching changes on a path
 */
public class ChildDataWatcher implements EventQueue.EventListener<ZookeeperEvent>, Watcher
{
    private final static Logger log = Logger.get(ChildDataWatcher.class);

    private final Map<String, Stat> elements = Maps.newHashMap();

    private final AtomicBoolean isStarted = new AtomicBoolean(false);
    private final String path;
    private final ZookeeperClient client;

    private final ChildDataListener listener;
    private final Executor executor;

    private final Object backgroundKey = new Object();

    private final Stat SENTINEL = new Stat();

    /**
     * @param client the client
     * @param path path to watch
     * @param listener event vector
     * @param executor thread to execute listener in
     */
    public ChildDataWatcher(ZookeeperClient client, String path, ChildDataListener listener, Executor executor)
    {
        this.path = path;
        this.client = client;
        this.listener = listener;
        this.executor = executor;
    }

    @PostConstruct
    public void start()
            throws Exception
    {
        if (isStarted.compareAndSet(false, true)) {
            log.debug("start(): getting children: %s", path);
            client.addListener(this, path, backgroundKey);
            client.usingWatcher(this).inBackground(backgroundKey).getChildren(path);
        }
    }

    @PreDestroy
    public void stop()
    {
        isStarted.set(false);
        client.removeListener(this);
    }

    @Override
    public void process(WatchedEvent event)
    {
        try {
            switch ( event.getType() ) {
                case NodeChildrenChanged: {
                    log.debug("Getting children: %s", path);
                    client.usingWatcher(this).inBackground(backgroundKey).getChildren(path);
                    break;
                }

                case NodeDataChanged: {
                    String child = event.getPath().substring(path.length() + 1);

                    log.debug("Getting data: %s", event.getPath());
                    client.usingWatcher(this).inBackground(backgroundKey).withContext(child).getData(event.getPath());
                    break;
                }
            }
        }
        catch ( Exception e ) {
            log.error(e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void eventProcessed(ZookeeperEvent event)
            throws Exception
    {
        if (isOurEvent(event)) {
            switch (event.getType()) {
                case DELETE: {
                    processDelete(event);
                    break;
                }

                case WATCHED_NODE_CHILDREN_CHANGED: {
                    log.debug("Getting children: %s", path);
                    client.usingWatcher(this).inBackground(backgroundKey).getChildren(path);
                    break;
                }

                case WATCHED_NODE_DATA_CHANGED: {
                    String child = event.getPath().substring(path.length() + 1);

                    log.debug("Getting data: %s", event.getPath());
                    client.usingWatcher(this).inBackground(backgroundKey).withContext(child).getData(event.getPath());
                    break;
                }

                case GET_CHILDREN: {
                    processGetChildren(event);
                    break;
                }

                case GET_DATA: {
                    processGetData(event);
                    break;
                }
            }
        }
    }

    private void processDelete(ZookeeperEvent event)
    {
        String  deletePath = event.getPath();
        if ( deletePath.startsWith(path) ) {
            deletePath = deletePath.substring(path.length());
            if ( deletePath.startsWith("/") ) {
                deletePath = deletePath.substring(1);
            }
        }

        log.debug("Deleting: %s", deletePath);
        synchronized (elements) {
            elements.remove(deletePath);
        }
        notifyRemoved(deletePath);
    }

    private void processGetData(ZookeeperEvent event)
            throws Exception
    {
        log.debug("ChildDataCallback: [rc = %d] %s, %s", event.getResultCode().intValue(), path, event.getStat());
        if (isStarted.get() && event.getResultCode() == KeeperException.Code.OK) { // check for case where the child disappeared since we got the childrenChanged notification

            String child = event.getContext().toString();
            Stat currentStat;

            synchronized (elements) {
                currentStat = elements.get(child);
                elements.put(child, event.getStat());
            }

            if (currentStat == SENTINEL) {
                notifyAdded(child, event.getData());
            }
            else {
                notifyUpdated(child, event.getData(), event.getStat().getVersion());
            }
        }
    }

    private void processGetChildren(ZookeeperEvent event)
            throws Exception
    {
        log.debug("ChildrenListCallBack: [rc = %d], %s, %s", event.getResultCode().intValue(), event.getPath(), event.getChildren());
        if (isStarted.get() && event.getResultCode() == KeeperException.Code.OK) {
            synchronized (elements) {
                Set<String> added = Sets.newHashSet(difference(Sets.newHashSet(event.getChildren()), elements.keySet()));
                Set<String> removed = Sets.newHashSet(difference(elements.keySet(), Sets.newHashSet(event.getChildren())));

                for (String child : added) {
                    String fullPath = path + "/" + child;
                    elements.put(child,
                            SENTINEL); // mark this child as tentative -- notifications will be sent once we get the data

                    log.debug("Getting data: %s", fullPath);
                    client.usingWatcher(this).inBackground(backgroundKey).withContext(child).getData(fullPath);
                }

                // removed
                for (String child : removed) {
                    // don't notify if the node was removed before we got a chance to get the data
                    // For all intents and purposes, the node never existed
                    if (elements.remove(child) != SENTINEL) {
                        notifyRemoved(child);
                    }
                }
            }
        }
    }

    private boolean isOurEvent(ZookeeperEvent event)
    {
        if (event.getPath() != null) {
            if (event.getPath().startsWith(path)) {
                return true;
            }
        }

        return (event.getKey() != null) && backgroundKey.equals(event.getKey());
    }

    private void notifyAdded(final String child, final byte[] bytes)
    {
        log.debug("Notify added: %s", child);
        if (isStarted.get()) {
            executor.execute(new Runnable()
            {
                @Override
                public void run()
                {
                    try {
                        listener.added(child, bytes);
                    }
                    catch (Exception e) {
                        log.error(e, "From added() listener");
                    }
                }
            });
        }
    }

    private void notifyUpdated(final String child, final byte[] bytes, final int version)
    {
        log.debug("Notify updated: %s (version %d)", child, version);
        if (isStarted.get()) {
            executor.execute(new Runnable()
            {
                @Override
                public void run()
                {
                    try {
                        listener.updated(child, bytes, version);
                    }
                    catch (Exception e) {
                        log.error(e, "From updated() listener");
                    }
                }
            });
        }
    }

    private void notifyRemoved(final String child)
    {
        if (isStarted.get()) {
            log.debug("Notify removed: %s", child);
            executor.execute(new Runnable()
            {
                @Override
                public void run()
                {
                    try {
                        listener.removed(child);
                    }
                    catch (Exception e) {
                        log.error(e, "From removed() listener");
                    }
                }
            });
        }
    }
}