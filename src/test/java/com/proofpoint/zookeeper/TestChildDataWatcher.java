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
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TestChildDataWatcher
{
    private ZookeeperTestServerInstance server;
    private ZookeeperClient client;

    @BeforeMethod
    public void     setup()
            throws Exception
    {
        server = new ZookeeperTestServerInstance();
        ZookeeperClientConfig config = new ZookeeperClientConfig();
        config.setConnectionString(server.getConnectString());
        client = new ZookeeperClient(config);
    }

    @AfterMethod
    public void     teardown()
            throws InterruptedException
    {
        client.closeForShutdown();
        server.close();
    }

    @Test
    public void     testMultipleEvents()
            throws Exception
    {
        client.mkdirs("/top");
        client.create("/top/one", "one".getBytes());
        client.create("/top/two", "two".getBytes());

        final CountDownLatch            addlatch = new CountDownLatch(2);
        final CountDownLatch            mutatelatch = new CountDownLatch(2);
        final Set<String>               events = Sets.newTreeSet();
        ChildDataListener               listener = new ChildDataListener()
        {
            @Override
            public void added(String child, byte[] data)
                    throws Exception
            {
                synchronized(events)
                {
                    events.add("Add: " + child);
                }
                addlatch.countDown();
            }

            @Override
            public void updated(String child, byte[] data, int version)
                    throws Exception
            {
                synchronized(events)
                {
                    events.add("Update: " + child);
                }
                mutatelatch.countDown();
            }

            @Override
            public void removed(String child)
                    throws Exception
            {
                synchronized(events)
                {
                    events.add("Remove: " + child);
                }
                mutatelatch.countDown();
            }
        };

        ChildDataWatcher        watcher = new ChildDataWatcher(client, "/top", listener, Executors.newSingleThreadExecutor());
        try
        {
            watcher.start();

            Assert.assertTrue(addlatch.await(5, TimeUnit.SECONDS));

            client.setData("/top/one", "test 1".getBytes());
            client.setData("/top/one", "test 2".getBytes());
            client.delete("/top/one");

            Assert.assertTrue(mutatelatch.await(5, TimeUnit.SECONDS));

            Set<String>     expected = Sets.newTreeSet();
            expected.add("Add: one");
            expected.add("Add: two");
            expected.add("Update: one");
            expected.add("Remove: one");
            Assert.assertEquals(events, expected);
        }
        finally
        {
            watcher.stop();
        }
    }

    @Test
    public void     testRemoveLocally()
            throws Exception
    {
        client.mkdirs("/top");
        client.create("/top/one", "one".getBytes());
        client.create("/top/two", "two".getBytes());

        final BlockingQueue<String>     queue = new ArrayBlockingQueue<String>(1);
        ChildDataListener               listener = new ChildDataListener()
        {
            @Override
            public void added(String child, byte[] data)
                    throws Exception
            {
            }

            @Override
            public void updated(String child, byte[] data, int version)
                    throws Exception
            {
            }

            @Override
            public void removed(String child)
                    throws Exception
            {
                queue.add(child);
            }
        };
        ChildDataWatcher        watcher = new ChildDataWatcher(client, "/top", listener, Executors.newSingleThreadExecutor());
        try
        {
            watcher.start();

            client.delete("/top/one");
            String  value = queue.poll(5, TimeUnit.SECONDS);
            Assert.assertEquals(value, "one");
        }
        finally
        {
            watcher.stop();
        }
    }

    @Test
    public void     testInitialState()
            throws Exception
    {
        client.mkdirs("/top");
        client.create("/top/one", "one".getBytes());
        client.create("/top/two", "two".getBytes());
        client.create("/top/three", "three".getBytes());

        final Map<String, String>   added = Maps.newHashMap();
        ChildDataListener           listener = new ChildDataListener()
        {
            @Override
            public void added(String child, byte[] data)
                    throws Exception
            {
                synchronized(added)
                {
                    added.put(child, new String(data));
                    added.notifyAll();
                }
            }

            @Override
            public void updated(String child, byte[] data, int version)
                    throws Exception
            {
            }

            @Override
            public void removed(String child)
                    throws Exception
            {
            }
        };
        ChildDataWatcher        watcher = new ChildDataWatcher(client, "/top", listener, Executors.newSingleThreadExecutor());
        try
        {
            watcher.start();

            long        startTicks = System.currentTimeMillis();
            //noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized(added)
            {
                while ( added.size() < 3 )
                {
                    if ( (System.currentTimeMillis() - startTicks) > 5000 )
                    {
                        break;
                    }
                    added.wait(1000);
                }

                Assert.assertEquals(added.size(), 3);
                Assert.assertEquals(added.get("one"), "one");
                Assert.assertEquals(added.get("two"), "two");
                Assert.assertEquals(added.get("three"), "three");
            }
        }
        finally
        {
            watcher.stop();
        }
    }
}
