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

import com.google.common.base.Preconditions;
import com.proofpoint.log.Logger;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

class RetryHandler<T>
{
    private static final Logger log = Logger.get(RetryHandler.class);

    private final ZookeeperClient client;
    private final RetryPolicy policy;
    private final Call<T> proc;

    private int retries = 0;

    interface Call<T>
    {
        public T call(ZooKeeper client, RetryHandler<T> retryHandler) throws Exception;
    }

    static <T> T makeAndStart(ZookeeperClient client, RetryPolicy policy, Call<T> proc)
            throws Exception
    {
        return new RetryHandler<T>(client, policy, proc).start();
    }

    RetryHandler(ZookeeperClient client, RetryPolicy policy, Call<T> proc)
    {
        Preconditions.checkNotNull(client);
        Preconditions.checkNotNull(policy);
        Preconditions.checkNotNull(proc);

        this.client = client;
        this.policy = policy;
        this.proc = proc;
    }

    T start()
            throws Exception
    {
        if ( client.getState().isZombie() ) {
            return null;
        }

        while ( !client.getState().ensureConnected() ) {
            if ( !shouldRetry(KeeperException.Code.CONNECTIONLOSS) ) {
                throw new KeeperException.ConnectionLossException();
            }
        }

        for(;;) {
            try {
                return proc.call(client.getState().ensureCreated(), this);
            }
            catch ( KeeperException e ) {
                if ( !shouldRetry(e.code()) ) {
                    throw e;
                }
            }
        }
    }

    boolean okToContinue(int rc)
    {
        KeeperException.Code code = KeeperException.Code.get(rc);
        if ( shouldRetry(code) ) {
            try {
                proc.call(client.getState().ensureCreated(), this);
            }
            catch ( Exception e ) {
                log.error(e, "for call %s", proc.getClass().getName());
            }
            return false;
        }
        return true;
    }

    private boolean shouldRetry(KeeperException.Code code)
    {
        if ( (code == KeeperException.Code.CONNECTIONLOSS) || (code == KeeperException.Code.OPERATIONTIMEOUT) ) {
            try {
                //noinspection ThrowableResultOfMethodCallIgnored
                if (policy.shouldRetry(KeeperException.create(code), retries++)) {
                    client.getState().close();
                    return true;
                }
                else {
                    log.info("Connection lost on retries for call %s", proc.getClass().getName());
                    client.errorConnectionLost();
                    client.getState().close();
                }
            }
            catch ( Exception e ) {
                log.error(e, "for call %s", proc.getClass().getName());
            }
        }
        return false;
    }
}
