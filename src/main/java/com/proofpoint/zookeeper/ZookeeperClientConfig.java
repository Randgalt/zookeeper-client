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

import com.proofpoint.configuration.Config;
import com.proofpoint.configuration.ConfigDescription;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Null;

public class ZookeeperClientConfig
{
    private String connectionString;
    private int sessionTimeoutInMs = 60000;
    private int connectionTimeoutInMs = 10000;
    private int maxConnectionLossRetries = 2;
    private int connectionLossSleepInMs = 1000;
    private String sessionStorePath;

    @Null
    public String getSessionStorePath()
    {
        return sessionStorePath;
    }

    @Config("zookeeper.session-id-local-path")
    @ConfigDescription("File to store the Zookeeper session ID in. This is optional - specify only if session re-use is needed.")
    public void setSessionStorePath(String sessionStorePath)
    {
        this.sessionStorePath = sessionStorePath;
    }

    @NotNull
    public String getConnectionString()
    {
        return connectionString;
    }

    @Config("zookeeper.connection-string")
    @ConfigDescription("Zookeeper connection string")
    public void setConnectionString(String connectionString)
    {
        this.connectionString = connectionString;
    }

    public int getSessionTimeoutInMs()
    {
        return sessionTimeoutInMs;
    }

    @Config("zookeeper.session-timeout-in-ms")
    @ConfigDescription("Zookeeper session timeout in ms")
    public void setSessionTimeoutInMs(int sessionTimeoutInMs)
    {
        this.sessionTimeoutInMs = sessionTimeoutInMs;
    }

    public int getConnectionTimeoutInMs()
    {
        return connectionTimeoutInMs;
    }

    @Config("zookeeper.connection-timeout-in-ms")
    @ConfigDescription("Zookeeper connection timeout in ms")
    public void setConnectionTimeoutInMs(int connectionTimeoutInMs)
    {
        this.connectionTimeoutInMs = connectionTimeoutInMs;
    }

    public int getMaxConnectionLossRetries()
    {
        return maxConnectionLossRetries;
    }

    @Config("zookeeper.connection-max-retries")
    @ConfigDescription("Max number of times to retry connecting to the ZK cluster before failing")
    public void setMaxConnectionLossRetries(int maxConnectionLossRetries)
    {
        this.maxConnectionLossRetries = maxConnectionLossRetries;
    }

    public int getConnectionLossSleepInMs()
    {
        return connectionLossSleepInMs;
    }

    @Config("zookeeper.connection-loss-sleep")
    @ConfigDescription("Amount of time in ms to sleep between connection loss retries")
    public void setConnectionLossSleepInMs(int connectionLossSleepInMs)
    {
        this.connectionLossSleepInMs = connectionLossSleepInMs;
    }
}
