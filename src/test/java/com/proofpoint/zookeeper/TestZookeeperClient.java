package com.proofpoint.zookeeper;

import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import com.proofpoint.zookeeper.events.EventQueue;
import com.proofpoint.configuration.ConfigurationFactory;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.fail;

public class TestZookeeperClient
{
    //    @Test TODO - this doesn't work due to this bug: https://issues.apache.org/jira/browse/ZOOKEEPER-832
    public void testMultiServer()
            throws Exception
    {
        ZookeeperTestServerInstance testServer1 = new ZookeeperTestServerInstance(10000);
        ZookeeperTestServerInstance testServer2 = new ZookeeperTestServerInstance(10001);
        String connectionString = testServer1.getConnectString() + "\\," + testServer2.getConnectString();

        Map<String, String> props = new HashMap<String, String>();
        props.put("zookeeper.connection-string", connectionString);
        ConfigurationFactory factory = new ConfigurationFactory(props);
        ZookeeperClient client = new ZookeeperClient(factory.build(ZookeeperClientConfig.class));

        for (int i = 0; i < 2; ++i) {
            try {
                client.exists("/one/two");
            }
            catch (Exception e) {
                fail("Connection Failed", e);
            }

            if (i == 0) {
                testServer1.close();
            }
        }

        client.closeForShutdown();

        testServer2.close();
    }

    @Test
    public void testNewSessionFromSaved()
            throws Exception
    {
        ZookeeperTestServerInstance server = new ZookeeperTestServerInstance();
        try {
            ZookeeperClientConfig config = new ZookeeperClientConfig();
            config.setSessionStorePath(server.getTempDirectory().newFile().getPath());
            config.setConnectionString(server.getConnectString());

            ZookeeperClient client1 = new ZookeeperClient(config);
            ZookeeperSessionID initialSession = client1.getSessionInfo();
            client1.closeForShutdown();

            ZookeeperClient client2 = new ZookeeperClient(config);
            ZookeeperSessionID nextSession = client2.getSessionInfo();
            client2.closeForShutdown();

            Assert.assertNotSame(initialSession, nextSession);
        }
        finally {
            server.close();
        }
    }

    @Test
    public void testRestartSession()
            throws Exception
    {
        ZookeeperClient client1 = null;
        ZookeeperClient client2 = null;
        ZookeeperTestServerInstance server = new ZookeeperTestServerInstance();
        try {
            ZookeeperClientConfig config = new ZookeeperClientConfig();
            config.setSessionStorePath(server.getTempDirectory().newFile().getPath());
            config.setConnectionString(server.getConnectString());

            client1 = new ZookeeperClient(config);
            ZookeeperSessionID initialSession = client1.getSessionInfo();

            client2 = new ZookeeperClient(config);    // should resume session
            ZookeeperSessionID nextSession = client2.getSessionInfo();

            Assert.assertEquals(initialSession, nextSession);
        }
        finally {
            if (client1 != null) {
                client1.closeForShutdown();
            }
            if (client2 != null) {
                client2.closeForShutdown();
            }

            server.close();
        }
    }

    @Test
    public void testReconnect()
            throws Exception
    {
        ZookeeperTestServerInstance testServer = new ZookeeperTestServerInstance();
        int testServerPort = testServer.getPort();

        Map<String, String> props = Maps.newHashMap();
        props.put("zookeeper.connection-string", testServer.getConnectString());
        ConfigurationFactory factory = new ConfigurationFactory(props);
        ZookeeperClientConfig zookeeperClientConfig = factory.build(ZookeeperClientConfig.class);
        zookeeperClientConfig.setConnectionTimeoutInMs(1000);
        ZookeeperClient client = new ZookeeperClient(zookeeperClientConfig);
        client.exists("/"); // blocks until connection is achieved

        testServer.close();

        testServer = new ZookeeperTestServerInstance(testServerPort);
        client.exists("/");  // blocks until connection is achieved

        testServer.close();
    }

    @Test
    public void testRetries()
            throws Exception
    {
        ZookeeperTestServerInstance testServer = new ZookeeperTestServerInstance();

        final AtomicInteger retryCount = new AtomicInteger(0);
        Map<String, String> props = Maps.newHashMap();
        props.put("zookeeper.connection-string", testServer.getConnectString());
        ConfigurationFactory factory = new ConfigurationFactory(props);
        ZookeeperClientConfig zookeeperClientConfig = factory.build(ZookeeperClientConfig.class);
        zookeeperClientConfig.setConnectionTimeoutInMs(1000);

        ZookeeperClient client = new ZookeeperClient(zookeeperClientConfig).setRetryPolicy(new RetryPolicy()
        {
            @Override
            public boolean shouldRetry(Exception e, int retries)
                    throws Exception
            {
                retryCount.incrementAndGet();
                return retries < 1;
            }
        }
        );
        client.exists("/"); // blocks until connection is achieved

        final AtomicReference<CountDownLatch> latch = new AtomicReference<CountDownLatch>(new CountDownLatch(1));
        client.setErrorHandler(new ZookeeperClientErrorHandler()
        {
            @Override
            public void connectionLost(ZookeeperClient client)
            {
                latch.get().countDown();
            }
        });

        testServer.close(); // next client call causes a retry

        try {
            client.exists("/");
            fail();
        }
        catch (Exception e) {
            // correct
        }
        assertEquals(retryCount.get(), 2);
        Assert.assertTrue(latch.get().await(10, TimeUnit.SECONDS));

        latch.set(new CountDownLatch(1));
        retryCount.set(0);

        // now try with a background call that should succeed

        testServer = new ZookeeperTestServerInstance();
        zookeeperClientConfig.setConnectionString(testServer.getConnectString());

        client.inBackground("").exists("/");
        assertEquals(retryCount.get(), 0);
        assertEquals(latch.get().getCount(), 1);

        client.closeForShutdown();
    }

    @Test
    public void testConnection()
            throws Exception
    {
        ZookeeperTestServerInstance testServer = new ZookeeperTestServerInstance(4532);

        Map<String, String> props = Maps.newHashMap();
        props.put("zookeeper.connection-string", testServer.getConnectString());
        ConfigurationFactory factory = new ConfigurationFactory(props);
        ZookeeperClient client = new ZookeeperClient(factory.build(ZookeeperClientConfig.class));

        try {
            client.exists("/one/two");
        }
        catch (Exception e) {
            fail("Connection Failed", e);
        }

        client.closeForShutdown();

        testServer.close();
    }

    @Test
    public void testMessaging()
            throws Exception
    {
        ZookeeperTestServerInstance testServer = new ZookeeperTestServerInstance(4532);

        Map<String, String> props = Maps.newHashMap();
        props.put("zookeeper.connection-string", testServer.getConnectString());
        ConfigurationFactory factory = new ConfigurationFactory(props);
        ZookeeperClient client = new ZookeeperClient(factory.build(ZookeeperClientConfig.class));

        final String path = "/one";
        final CountDownLatch latch = new CountDownLatch(1);
        client.addListener(
                new EventQueue.EventListener<com.proofpoint.zookeeper.ZookeeperEvent>()
                {
                    @Override
                    public void eventProcessed(ZookeeperEvent event)
                            throws Exception
                    {
                        if (event.getType() == ZookeeperEvent.Type.CREATE) {
                            assertEquals(event.getPath(), path);
                            latch.countDown();
                        }
                    }
                },
                new Predicate<ZookeeperEvent>()
                {
                    @Override
                    public boolean apply(ZookeeperEvent event)
                    {
                        return isOurEvent(event, path);
                    }
                }
        );

        client.inBackground(null).create(path, new byte[0]);

        latch.await(30, TimeUnit.SECONDS);
        assertEquals(latch.getCount(), 0);

        client.closeForShutdown();

        testServer.close();
    }

    @Test
    public void testMessagingMultiClient()
            throws Exception
    {
        ZookeeperTestServerInstance testServer = new ZookeeperTestServerInstance(4325);

        Map<String, String> props = Maps.newHashMap();
        props.put("zookeeper.connection-string", testServer.getConnectString());
        ConfigurationFactory factory = new ConfigurationFactory(props);
        ZookeeperClient client1 = new ZookeeperClient(factory.build(ZookeeperClientConfig.class));
        ZookeeperClient client2 = new ZookeeperClient(factory.build(ZookeeperClientConfig.class));

        final String path = "/one";
        final CountDownLatch latch = new CountDownLatch(1);
        client2.addListener(
                new EventQueue.EventListener<com.proofpoint.zookeeper.ZookeeperEvent>()
                {
                    @Override
                    public void eventProcessed(ZookeeperEvent event)
                            throws Exception
                    {
                        if (event.getType() == ZookeeperEvent.Type.EXISTS) {
                            assertEquals(event.getPath(), path);
                            latch.countDown();
                        }
                    }
                },
                new Predicate<ZookeeperEvent>()
                {
                    @Override
                    public boolean apply(ZookeeperEvent event)
                    {
                        return isOurEvent(event, path);
                    }
                }
        );

        client2.inBackground(null).exists(path);
        client1.create(path, new byte[0]);

        latch.await(30, TimeUnit.SECONDS);
        assertEquals(latch.getCount(), 0);

        client1.closeForShutdown();
        client2.closeForShutdown();

        testServer.close();
    }

    @Test
    public void testVersionMismatch()
            throws Exception
    {
        ZookeeperTestServerInstance testServer = new ZookeeperTestServerInstance();

        Map<String, String> props = Maps.newHashMap();
        props.put("zookeeper.connection-string", testServer.getConnectString());
        ConfigurationFactory factory = new ConfigurationFactory(props);
        ZookeeperClient client = new ZookeeperClient(factory.build(ZookeeperClientConfig.class));

        final String path = "/one";

        client.create(path, new byte[0]);
        Stat preChangeStat = client.exists(path);
        assertEquals(preChangeStat.getVersion(), 0);
        client.setData(path, "test".getBytes());
        Stat postChangeStat = client.exists(path);
        assertNotSame(preChangeStat.getVersion(), postChangeStat.getVersion());
        try {
            client.dataVersion(preChangeStat.getVersion()).setData(path, "something".getBytes());
            fail();
        }
        catch (KeeperException.BadVersionException e) {
            // sucess
        }
        catch (Exception e) {
            fail("", e);
        }

        client.closeForShutdown();

        testServer.close();
    }

    private boolean isOurEvent(ZookeeperEvent event, String path)
    {
        if (event.getPath() != null) {
            if (event.getPath().startsWith(path)) {
                return true;
            }
        }

        return false;
    }
}
