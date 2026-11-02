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
package org.apache.nifi.cdc.mongodb.processors;

import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.apache.nifi.cdc.mongodb.event.EventMapper;
import org.bson.BsonDocument;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * A change stream cursor that hands out prepared events. Like the real cursor it reports the position it has read
 * up to, which advances as events are taken, and it can be told to fail in the middle of a batch.
 */
class FakeChangeStreamCursor implements MongoChangeStreamCursor<ChangeStreamDocument<BsonDocument>> {

    private final Deque<ChangeStreamDocument<BsonDocument>> events = new ArrayDeque<>();
    private BsonDocument resumeToken;
    private int failAfterEvents = -1;
    private int served;
    private boolean closed;

    FakeChangeStreamCursor(final String initialResumeToken, final List<ChangeStreamDocument<BsonDocument>> events) {
        this.resumeToken = EventMapper.resumeToken(initialResumeToken);
        this.events.addAll(events);
    }

    FakeChangeStreamCursor failAfter(final int events) {
        this.failAfterEvents = events;
        return this;
    }

    boolean isClosed() {
        return closed;
    }

    @Override
    public ChangeStreamDocument<BsonDocument> tryNext() {
        if (served == failAfterEvents) {
            throw new MongoException("Injected change stream failure");
        }
        final ChangeStreamDocument<BsonDocument> event = events.poll();
        if (event == null) {
            return null;
        }
        served++;
        resumeToken = event.getResumeToken();
        return event;
    }

    @Override
    public BsonDocument getResumeToken() {
        return resumeToken;
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public boolean hasNext() {
        return !events.isEmpty();
    }

    @Override
    public ChangeStreamDocument<BsonDocument> next() {
        final ChangeStreamDocument<BsonDocument> event = tryNext();
        if (event == null) {
            throw new NoSuchElementException();
        }
        return event;
    }

    @Override
    public int available() {
        return events.size();
    }

    @Override
    public ServerCursor getServerCursor() {
        return null;
    }

    @Override
    public ServerAddress getServerAddress() {
        return new ServerAddress();
    }
}
