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

package org.apache.nifi.amqp.processors;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Connection;

public class AMQPResource<T extends AMQPWorker> implements Closeable {
    private final Connection connection;
    private final T worker;

    public AMQPResource(final Connection connection, final T worker) {
        this.connection = connection;
        this.worker = worker;
    }

    public Connection getConnection() {
        return connection;
    }

    public T getWorker() {
        return worker;
    }

    @Override
    public void close() throws IOException {
        IOException ioe = null;

        try {
            worker.close();
        } catch (final IOException e) {
            ioe = e;
        } catch (final TimeoutException e) {
            ioe = new IOException(e);
        }

        try {
            connection.close();
        } catch (final IOException e) {
            if (ioe == null) {
                ioe = e;
            } else {
                ioe.addSuppressed(e);
            }
        }

        if (ioe != null) {
            throw ioe;
        }
    }

}
