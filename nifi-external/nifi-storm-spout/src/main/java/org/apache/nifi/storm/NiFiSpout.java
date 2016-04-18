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
package org.apache.nifi.storm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;
import org.apache.nifi.remote.protocol.DataPacket;
import org.apache.nifi.stream.io.StreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * <p>
 * The <code>NiFiSpout</code> provides a way to pull data from Apache NiFi so
 * that it can be processed by Apache Storm. The NiFi Spout connects to a NiFi
 * instance provided in the config and requests data from the OutputPort that
 * is named. In NiFi, when an OutputPort is added to the root process group,
 * it acts as a queue of data for remote clients. This spout is then able to
 * pull that data from NiFi reliably.
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
 * In order to use the NiFiSpout, you will need to first build a
 * {@link SiteToSiteClientConfig} to provide to the constructor. This can be
 * achieved by using the {@link SiteToSiteClient.Builder}. Below is an example
 * snippet of driver code to pull data from NiFi that is running on
 * localhost:8080. This example assumes that NiFi exposes an OutputPort on the
 * root group named "Data For Storm". Additionally, it assumes that the data
 * that it will receive from this OutputPort is text data, as it will map the
 * byte array received from NiFi to a UTF-8 Encoded string.
 * </p>
 *
 * <code>
 * <pre>
 * {@code
 *
 * // Build a Site-To-Site client config
 * SiteToSiteClientConfig clientConfig = new SiteToSiteClient.Builder()
 *   .url("http://localhost:8080/nifi")
 *   .portName("Data for Storm")
 *   .buildConfig();
 *
 * // Build a topology starting with a NiFiSpout
 * TopologyBuilder builder = new TopologyBuilder();
 * builder.setSpout("nifi", new NiFiSpout(clientConfig));
 *
 * // Add a bolt that prints the attributes and content
 * builder.setBolt("print", new BaseBasicBolt() {
 *   @Override
 *   public void execute(Tuple tuple, BasicOutputCollector collector) {
 *      NiFiDataPacket dp = (NiFiDataPacket) tuple.getValueByField("nifiDataPacket");
 *      System.out.println("Attributes: " + dp.getAttributes());
 *      System.out.println("Content: " + new String(dp.getContent()));
 *   }
 *
 *   @Override
 *   public void declareOutputFields(OutputFieldsDeclarer declarer) {}
 *
 * }).shuffleGrouping("nifi");
 *
 * // Submit the topology running in local mode
 * Config conf = new Config();
 * LocalCluster cluster = new LocalCluster();
 * cluster.submitTopology("test", conf, builder.createTopology());
 *
 * Utils.sleep(90000);
 * cluster.shutdown();
 * }
 * </pre>
 * </code>
 */
public class NiFiSpout extends BaseRichSpout {

    private static final long serialVersionUID = 3067274587595578836L;

    public static final Logger LOGGER = LoggerFactory.getLogger(NiFiSpout.class);

    public static final String NIFI_DATA_PACKET = "nifiDataPacket";

    private NiFiSpoutReceiver spoutReceiver;
    private LinkedBlockingQueue<NiFiDataPacket> queue;
    private SpoutOutputCollector spoutOutputCollector;

    private final SiteToSiteClientConfig clientConfig;
    private final List<String> attributeNames;

    /**
     * @param clientConfig
     *              configuration used to build the SiteToSiteClient
     */
    public NiFiSpout(SiteToSiteClientConfig clientConfig) {
        this(clientConfig, null);
    }

    /**
     *
     * @param clientConfig
     *              configuration used to build the SiteToSiteClient
     * @param attributeNames
     *              names of FlowFile attributes to be added as values to each tuple, in addition
     *                  to the nifiDataPacket value on all tuples
     *
     */
    public NiFiSpout(SiteToSiteClientConfig clientConfig, List<String> attributeNames) {
        this.clientConfig = clientConfig;
        this.attributeNames = (attributeNames == null ? new ArrayList<String>() : attributeNames);
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        this.queue = new LinkedBlockingQueue<>(1000);

        this.spoutReceiver = new NiFiSpoutReceiver();
        this.spoutReceiver.setDaemon(true);
        this.spoutReceiver.setName("NiFi Spout Receiver");
        this.spoutReceiver.start();
    }

    @Override
    public void nextTuple() {
        NiFiDataPacket data = queue.poll();
        if (data == null) {
            Utils.sleep(50);
        } else {
            // always start with the data packet
            Values values = new Values(data);

            // add additional values based on the specified attribute names
            for (String attributeName : attributeNames) {
                if (data.getAttributes().containsKey(attributeName)) {
                    values.add(data.getAttributes().get(attributeName));
                }
            }

            spoutOutputCollector.emit(values);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        final List<String> fieldNames = new ArrayList<>();
        fieldNames.add(NIFI_DATA_PACKET);
        fieldNames.addAll(attributeNames);
        outputFieldsDeclarer.declare(new Fields(fieldNames));
    }

    @Override
    public void close() {
        super.close();
        spoutReceiver.shutdown();
    }

    class NiFiSpoutReceiver extends Thread {

        private boolean shutdown = false;

        public synchronized void shutdown() {
            this.shutdown = true;
        }

        @Override
        public void run() {
            try {
                final SiteToSiteClient client = new SiteToSiteClient.Builder().fromConfig(clientConfig).build();
                try {
                    while (!shutdown) {
                        final Transaction transaction = client.createTransaction(TransferDirection.RECEIVE);
                        DataPacket dataPacket = transaction.receive();
                        if (dataPacket == null) {
                            transaction.confirm();
                            transaction.complete();

                            // no data available. Wait a bit and try again
                            try {
                                Thread.sleep(1000L);
                            } catch (InterruptedException e) {
                            }

                            continue;
                        }

                        final List<NiFiDataPacket> dataPackets = new ArrayList<>();
                        do {
                            // Read the data into a byte array and wrap it along with the attributes
                            // into a NiFiDataPacket.
                            final InputStream inStream = dataPacket.getData();
                            final byte[] data = new byte[(int) dataPacket.getSize()];
                            StreamUtils.fillBuffer(inStream, data);

                            final Map<String, String> attributes = dataPacket.getAttributes();
                            final NiFiDataPacket niFiDataPacket = new StandardNiFiDataPacket(data, attributes);

                            dataPackets.add(niFiDataPacket);
                            dataPacket = transaction.receive();
                        } while (dataPacket != null);

                        // Confirm transaction to verify the data
                        transaction.confirm();

                        for (NiFiDataPacket dp : dataPackets) {
                            queue.offer(dp);
                        }

                        transaction.complete();
                    }
                } finally {
                    try {
                        client.close();
                    } catch (final IOException ioe) {
                        LOGGER.error("Failed to close client", ioe);
                    }
                }
            } catch (final IOException ioe) {
                LOGGER.error("Failed to receive data from NiFi", ioe);
            }
        }
    }
}
