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
package com.proofpoint.zookeeper.events;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TestQuantizedExecutor
{
    @Test
    public void     testRunImmediately() throws InterruptedException
    {
        final AtomicInteger     runCount = new AtomicInteger(0);
        Runnable                task = new Runnable()
        {
            @Override
            public void run()
            {
                runCount.incrementAndGet();
            }
        };
        QuantizedExecutor       executor = new QuantizedExecutor(10 * 60 * 1000, task);
        executor.runNowIf();
        Thread.sleep(1000);
        Assert.assertEquals(runCount.get(), 1);
    }

    @Test
    public void     testCoalescing() throws InterruptedException
    {
        final CountDownLatch    runlatch = new CountDownLatch(1);
        final CountDownLatch    testlatch = new CountDownLatch(1);
        final AtomicInteger     runCount = new AtomicInteger(0);
        Runnable                task = new Runnable()
        {
            @Override
            public void run()
            {
                runCount.incrementAndGet();

                runlatch.countDown();

                try
                {
                    testlatch.await();
                }
                catch ( InterruptedException e )
                {
                    Thread.currentThread().interrupt();
                }
            }
        };
        QuantizedExecutor       executor = new QuantizedExecutor(1, task);

        for ( int i = 0; i < 1000; ++i )
        {
            executor.makeRunnable();
        }

        runlatch.await();

        Assert.assertEquals(runCount.get(), 1);

        for ( int i = 0; i < 1000; ++i )
        {
            executor.makeRunnable();
        }

        testlatch.countDown();

        Thread.sleep(1000);

        Assert.assertEquals(runCount.get(), 2);
    }

    @Test
    public void     testSerializedExecution() throws InterruptedException
    {
        final CountDownLatch    runlatch = new CountDownLatch(1);
        final CountDownLatch    testlatch = new CountDownLatch(1);
        final AtomicBoolean     isRunning = new AtomicBoolean(false);
        final AtomicInteger     runCount = new AtomicInteger(0);
        Runnable                task = new Runnable()
        {
            @Override
            public void run()
            {
                if ( !isRunning.compareAndSet(false, true) )
                {
                    Assert.fail();
                }
                runCount.incrementAndGet();

                try
                {
                    runlatch.countDown();
                    try
                    {
                        testlatch.await();
                    }
                    catch ( InterruptedException e )
                    {
                        Thread.currentThread().interrupt();
                    }
                }
                finally
                {
                    isRunning.set(false);
                }
            }
        };
        QuantizedExecutor executor = new QuantizedExecutor(1, task);
        executor.makeRunnable();
        executor.makeRunnable();
        executor.makeRunnable();
        executor.makeRunnable();
        runlatch.await();
        executor.makeRunnable();
        executor.makeRunnable();
        executor.makeRunnable();
        executor.makeRunnable();
        testlatch.countDown();
        Thread.sleep(1000);

        Assert.assertFalse(isRunning.get());
        Assert.assertEquals(runCount.get(), 2);
    }
}
