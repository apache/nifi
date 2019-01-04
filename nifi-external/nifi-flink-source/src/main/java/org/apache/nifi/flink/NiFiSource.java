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
package org.apache.nifi.flink;

import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;
import org.apache.nifi.remote.protocol.DataPacket;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * The <code>NiFiSource</code> is a Reliable Receiver that provides a way to
 * pull data from Apache NiFi so that it can be processed by Flink.
 * The NiFi Receiver connects to NiFi instance provided in the config and
 * requests data from the OutputPort that is named. In NiFi, when an OutputPort
 * is added to the root process group, it acts as a queue of data for remote
 * clients. This receiver is then able to pull that data from NiFi reliably.
 * </p>
 *
 * <p>
 * It is important to note that if pulling data from a NiFi cluster, the URL
 * that should be used is that of the NiFi Cluster Manager. The Receiver will
 * automatically handle determining the nodes in that cluster and pull from
 * those nodes as appropriate.
 * </p>
 *
 * <p>
 * In order to use the NiFiReceiver, you will need to first build a
 * {@link SiteToSiteClientConfig} to provide to the constructor. This can be
 * achieved by using the {@link SiteToSiteClient.Builder}. Below is an example
 * snippet of driver code to pull data from NiFi that is running on
 * localhost:8080. This example assumes that NiFi exposes and OutputPort on the
 * root group named "Data For Flink". Additionally, it assumes that the data
 * that it will receive from this OutputPort is text data, as it will map the
 * byte array received from NiFi to a UTF-8 Encoded string.
 * </p>
 *
 * <code>
 * <pre>
 * {@code
 * StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
 *
 * SiteToSiteClientConfig clientConfig = new SiteToSiteClient.Builder()
 *   .url("http://localhost:8080/nifi")
 * 	 .portName("Data for Flink")
 * 	 .requestBatchCount(5)
 * 	 .buildConfig();
 *
 * SourceFunction<NiFiDataPacket> nifiSource = new NiFiSource(clientConfig);
 * DataStream<NiFiDataPacket> streamSource = env.addSource(nifiSource).setParallelism(2);
 *
 * DataStream<String> dataStream = streamSource.map(new MapFunction<NiFiDataPacket, String>() {
 *   @Override
 * 	 public String map(NiFiDataPacket value) throws Exception {
 *     return new String(value.getContent(), Charset.defaultCharset());
 * 	 }
 * });
 *
 * dataStream.print();
 * env.execute();
 * }
 * </pre>
 * </code>
 */
public class NiFiSource extends RichParallelSourceFunction<NiFiDataPacket> implements StoppableFunction {

    private static final long serialVersionUID = -5248908768058221188L;

    private static final long DEFAULT_WAIT_TIME_MS = 1000;

    // ------------------------------------------------------------------------

    private final SiteToSiteClientConfig clientConfig;

    private final long waitTimeMs;

    private transient SiteToSiteClient client;

    private volatile boolean isRunning = true;

    /**
     * Constructs a new NiFiReceiver using the given client config and the default wait time of 1000 ms.
     *
     * @param clientConfig the configuration for building a NiFi SiteToSiteClient
     */
    public NiFiSource(SiteToSiteClientConfig clientConfig) {
        this(clientConfig, DEFAULT_WAIT_TIME_MS);
    }

    /**
     * Constructs a new NiFiReceiver using the given client config and wait time.
     *
     * @param clientConfig the configuration for building a NiFi SiteToSiteClient
     * @param waitTimeMs the amount of time to wait (in milliseconds) if no data is available to pull from NiFi
     */
    public NiFiSource(SiteToSiteClientConfig clientConfig, long waitTimeMs) {
        this.clientConfig = clientConfig;
        this.waitTimeMs = waitTimeMs;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        client = new SiteToSiteClient.Builder().fromConfig(clientConfig).build();
    }

    @Override
    public void run(SourceContext<NiFiDataPacket> ctx) throws Exception {
        while (isRunning) {
            final Transaction transaction = client.createTransaction(TransferDirection.RECEIVE);
            if (transaction == null) {
                try {
                    Thread.sleep(waitTimeMs);
                } catch (InterruptedException ignored) {

                }
                continue;
            }

            DataPacket dataPacket = transaction.receive();
            if (dataPacket == null) {
                transaction.confirm();
                transaction.complete();
                try {
                    Thread.sleep(waitTimeMs);
                } catch (InterruptedException ignored) {

                }
                continue;
            }

            final List<NiFiDataPacket> niFiDataPackets = new ArrayList<>();
            do {
                // Read the data into a byte array and wrap it along with the attributes
                // into a NiFiDataPacket.
                final InputStream inStream = dataPacket.getData();
                final byte[] data = new byte[(int) dataPacket.getSize()];
                StreamUtils.fillBuffer(inStream, data);

                final Map<String, String> attributes = dataPacket.getAttributes();

                niFiDataPackets.add(new StandardNiFiDataPacket(data, attributes));
                dataPacket = transaction.receive();
            } while (dataPacket != null);

            // Confirm transaction to verify the data
            transaction.confirm();

            for (NiFiDataPacket dp : niFiDataPackets) {
                ctx.collect(dp);
            }

            transaction.complete();
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public void close() throws Exception {
        super.close();
        client.close();
    }

    @Override
    public void stop() {
        this.isRunning = false;
    }
}
