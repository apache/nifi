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
package org.apache.nifi.nar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class NarAutoLoaderExternalSourceTask implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(NarAutoLoaderExternalSourceTask.class);

    private final NarAutoLoaderExternalSource externalSource;
    private final long pollTimeInMs;
    private volatile boolean stopped = false;

    NarAutoLoaderExternalSourceTask(final NarAutoLoaderExternalSource externalSource, final long pollTimeInMs) {
        this.externalSource = externalSource;
        this.pollTimeInMs = pollTimeInMs;
    }

    @Override
    public void run() {
        while (!stopped) {
            try {
                LOGGER.debug("Starting acquiring from external source");
                externalSource.acquire();
                LOGGER.debug("Finished acquiring from external source");
                Thread.sleep(pollTimeInMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.warn("NAR autoloader external source task is interrupted");
                stopped = true;
            } catch (Throwable e) {
                LOGGER.error("Error during reaching the external source", e);
            }
        }
    }

    void stop() {
        LOGGER.info("Task is stopped");
        stopped = true;
        externalSource.stop();
    }
}
