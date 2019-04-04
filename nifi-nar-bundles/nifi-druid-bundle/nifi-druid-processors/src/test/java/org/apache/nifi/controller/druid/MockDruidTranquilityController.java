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
package org.apache.nifi.controller.druid;

import com.metamx.tranquility.beam.Beam;
import com.metamx.tranquility.tranquilizer.MessageDroppedException;
import com.metamx.tranquility.tranquilizer.Tranquilizer;
import com.metamx.tranquility.typeclass.Timestamper;
import com.twitter.finagle.Status;
import com.twitter.util.Awaitable;
import com.twitter.util.Duration;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;
import com.twitter.util.TimeoutException;
import com.twitter.util.Try;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.query.aggregation.AggregatorFactory;
import org.apache.curator.framework.CuratorFramework;
import scala.Function1;
import scala.Option;
import scala.runtime.BoxedUnit;

import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockDruidTranquilityController extends DruidTranquilityController {

    private final Tranquilizer t;
    private final CuratorFramework cf;
    private int numCalls = 0;

    public MockDruidTranquilityController() {
        this(-1, -1);
    }

    /**
     * Creates a mock/stub Druid controller for testing. The failAfterN parameter must be higher than the dropAfterN parameter in order for messages to be dropped.
     *
     * @param dropAfterN The number of records after which to start calling the "dropped" callback, namely onFailure(MessageDroppedException)
     * @param failAfterN The number of records after which to start calling the "failure" callback, namely onFailure(Exception)
     */
    public MockDruidTranquilityController(final int dropAfterN, final int failAfterN) {
        t = mock(Tranquilizer.class);
        final Future<BoxedUnit> future = new Future<BoxedUnit>() {

            FutureEventListener<? super BoxedUnit> listener;

            @Override
            public Future<BoxedUnit> addEventListener(FutureEventListener<? super BoxedUnit> listener) {
                this.listener = listener;
                numCalls++;
                if (dropAfterN >= 0 && numCalls > failAfterN) {
                    listener.onFailure(new Exception());
                } else if (dropAfterN >= 0 && numCalls > dropAfterN) {
                    listener.onFailure(MessageDroppedException.Instance());
                } else {
                    listener.onSuccess(BoxedUnit.UNIT);
                }
                return this;
            }

            @Override
            public Awaitable<BoxedUnit> ready(Duration timeout, CanAwait permit) throws InterruptedException, TimeoutException {
                return null;
            }

            @Override
            public BoxedUnit result(Duration timeout, CanAwait permit) throws Exception {
                return null;
            }

            @Override
            public boolean isReady(CanAwait permit) {
                return true;
            }

            @Override
            public Future<BoxedUnit> respond(Function1<Try<BoxedUnit>, BoxedUnit> k) {
                return null;
            }

            @Override
            public Option<Try<BoxedUnit>> poll() {
                return null;
            }

            @Override
            public void raise(Throwable interrupt) {

            }

            @Override
            public <B> Future<B> transform(Function1<Try<BoxedUnit>, Future<B>> f) {
                return null;
            }
        };
        when(t.send(anyObject())).thenReturn(future);
        when(t.status()).thenReturn(new Status() {
        });
        cf = mock(CuratorFramework.class);
    }

    @Override
    public Tranquilizer getTranquilizer() {
        return t;
    }

    @Override
    CuratorFramework getCurator(String zkConnectString) {
        return cf;
    }

    @SuppressWarnings("unchecked")
    @Override
    Tranquilizer<Map<String, Object>> buildTranquilizer(int maxBatchSize, int maxPendingBatches, int lingerMillis, Beam<Map<String, Object>> beam) {
        return t;
    }

    @SuppressWarnings("unchecked")
    @Override
    Beam<Map<String, Object>> buildBeam(String dataSource, String indexService, String discoveryPath, int clusterPartitions, int clusterReplication,
                                        String segmentGranularity, String queryGranularity, String windowPeriod, String indexRetryPeriod, List<String> dimensions,
                                        List<AggregatorFactory> aggregator, Timestamper<Map<String, Object>> timestamper, TimestampSpec timestampSpec) {
        return mock(Beam.class);
    }

    @Override
    public String getTransitUri() {
        return "";
    }

}
