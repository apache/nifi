/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.engine;

import org.apache.nifi.nar.NarThreadContextClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public final class FlowEngine extends ScheduledThreadPoolExecutor {

    private static final Logger logger = LoggerFactory.getLogger(FlowEngine.class);

    /**
     * Creates a new instance of FlowEngine
     *
     * @param corePoolSize the maximum number of threads available to tasks running in the engine.
     * @param threadNamePrefix for naming the thread
     */
    public FlowEngine(int corePoolSize, final String threadNamePrefix) {
        this(corePoolSize, threadNamePrefix, false);
    }

    /**
     * Creates a new instance of FlowEngine
     *
     * @param corePoolSize the maximum number of threads available to tasks running in the engine.
     * @param threadNamePrefix for thread naming
     * @param daemon if true, the thread pool will be populated with daemon threads, otherwise the threads will not be marked as daemon.
     */
    public FlowEngine(int corePoolSize, final String threadNamePrefix, final boolean daemon) {
        super(corePoolSize);

        final AtomicInteger threadIndex = new AtomicInteger(0);
        final ThreadFactory defaultThreadFactory = getThreadFactory();
        setThreadFactory(new ThreadFactory() {
            @Override
            public Thread newThread(final Runnable r) {
                final Thread t = defaultThreadFactory.newThread(r);
                if (daemon) {
                    t.setDaemon(true);
                }
                t.setName(threadNamePrefix + " Thread-" + threadIndex.incrementAndGet());
                return t;
            }
        });
    }

    /**
     * Hook method called by the running thread whenever a runnable task is given to the thread to run.
     *
     * @param thread thread
     * @param runnable runnable
     */
    @Override
    protected void beforeExecute(final Thread thread, final Runnable runnable) {
        // Ensure classloader is correct
        thread.setContextClassLoader(NarThreadContextClassLoader.getInstance());
        super.beforeExecute(thread, runnable);
    }

    /**
     * Hook method called by the thread that executed the given runnable after execution of the runnable completed. Logs the fact of completion and any errors that might have occurred.
     *
     * @param runnable runnable
     * @param throwable throwable
     */
    @Override
    protected void afterExecute(final Runnable runnable, final Throwable throwable) {
        super.afterExecute(runnable, throwable);
        if (runnable instanceof FutureTask<?>) {
            final FutureTask<?> task = (FutureTask<?>) runnable;
            try {
                if (task.isDone()) {
                    if (task.isCancelled()) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("A flow controller execution task '{}' has been cancelled.", runnable);
                        }
                    } else {
                        task.get(); //to raise any exceptions that might have occurred.
                        logger.debug("A Flow Controller execution task '{}' has completed.", runnable);
                    }
                }
            } catch (final CancellationException ce) {
                if (logger.isDebugEnabled()) {
                    logger.debug("A flow controller execution task '{}' has been cancelled.", runnable);
                }
            } catch (final InterruptedException ie) {
                if (logger.isDebugEnabled()) {
                    logger.debug("A flow controller execution task has been interrupted.", ie);
                }
            } catch (final ExecutionException ee) {
                logger.error("A flow controller task execution stopped abnormally", ee);
            }
        } else {
            logger.debug("A flow controller execution task '{}' has finished.", runnable);
        }
    }

    /**
     * Hook method called whenever the engine is terminated.
     */
    @Override
    protected void terminated() {
        super.terminated();
    }
}
