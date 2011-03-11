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

import java.util.Arrays;

/**
 * Wrapper around a ZK session
 */
public class ZookeeperSessionID
{
    private long sessionId;
    private byte[] password;

    public long getSessionId()
    {
        return sessionId;
    }

    public void setSessionId(long sessionId)
    {
        this.sessionId = sessionId;
    }

    public byte[] getPassword()
    {
        return password;
    }

    public void setPassword(byte[] password)
    {
        this.password = password;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ZookeeperSessionID sessionID = (ZookeeperSessionID) o;

        if (sessionId != sessionID.sessionId) {
            return false;
        }
        //noinspection RedundantIfStatement
        if (!Arrays.equals(password, sessionID.password)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (int) (sessionId ^ (sessionId >>> 32));
        result = 31 * result + Arrays.hashCode(password);
        return result;
    }
}
