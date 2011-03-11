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

import ch.qos.logback.classic.Level;
import com.proofpoint.zookeeper.io.TempLocalDirectory;
import com.proofpoint.log.Logger;
import org.apache.zookeeper.ClientCnxn;
import org.apache.zookeeper.server.FinalRequestProcessor;
import org.apache.zookeeper.server.NIOServerCnxn;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

/**
 * manages an internally running ZooKeeper server and clients to that server. FOR TESTING PURPOSES ONLY
 */
public class ZookeeperTestServerInstance
{
    private static final Logger log = Logger.get(ZookeeperTestServerInstance.class);

    static
    {
        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(FinalRequestProcessor.class)).setLevel(Level.ERROR);
        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ClientCnxn.class)).setLevel(Level.ERROR);
        ((ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ZookeeperEvent.class)).setLevel(Level.ERROR);
    }

    private final ZooKeeperServer server;
    private final int port;
    private final NIOServerCnxn.Factory factory;
    private final TempLocalDirectory tempLocalDirectory;

    private static final int TIME_IN_MS = 2000;

    /**
     * Create the server using a default port
     *
     * @throws Exception errors
     */
    public ZookeeperTestServerInstance()
            throws Exception
    {
        this(getRandomPort());
    }

    private static int getRandomPort()
    {
        ServerSocket server = null;
        try {
            server = new ServerSocket(0);
            return server.getLocalPort();
        }
        catch ( IOException e ) {
            throw new Error(e);
        }
        finally {
            if ( server != null ) {
                try {
                    server.close();
                }
                catch ( IOException ignore ) {
                    // ignore
                }
            }
        }
    }

    /**
     * Return the port being used
     *
     * @return port
     */
    public int getPort()
    {
        return port;
    }

    /**
     * Create the server using the given port
     *
     * @param port the port
     *
     * @throws Exception errors
     */
    public ZookeeperTestServerInstance(int port)
            throws Exception
    {
        this.port = port;

        tempLocalDirectory = new TempLocalDirectory();
        tempLocalDirectory.cleanupPrevious();
        File tempDirectory = tempLocalDirectory.newDirectory();

        log.info("Test Zookeeper port: %d path: %s", port, tempDirectory);

        File logDir = new File(tempDirectory, "log");
        File dataDir = new File(tempDirectory, "data");

        try
        {
            server = new ZooKeeperServer(dataDir, logDir, TIME_IN_MS);
            factory = new NIOServerCnxn.Factory(new InetSocketAddress(port));
            factory.startup(server);
        }
        catch ( BindException e )
        {
            log.debug("Address is in use: %d", port);
            throw e;
        }
    }

    /**
     * Close the server and any open clients
     *
     * @throws InterruptedException thread interruption
     */
    public void close()
            throws InterruptedException
    {
        try
        {
            server.shutdown();
            factory.shutdown();
        }
        finally
        {
            tempLocalDirectory.cleanup();
        }
    }

    /**
     * Returns the temp directory instance
     *
     * @return temp directory
     */
    public TempLocalDirectory getTempDirectory()
    {
        return tempLocalDirectory;
    }

    /**
     * Returns the connection string to use
     *
     * @return connection string
     */
    public String getConnectString()
    {
        return "localhost:" + port;
    }
}
