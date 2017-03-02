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
package org.apache.nifi.spark;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;
import org.apache.nifi.remote.protocol.DataPacket;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

/**
 * <p>
 * The <code>NiFiReceiver</code> is a Reliable Receiver that provides a way to
 * pull data from Apache NiFi so that it can be processed by Spark Streaming.
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
 * root group named "Data For Spark". Additionally, it assumes that the data
 * that it will receive from this OutputPort is text data, as it will map the
 * byte array received from NiFi to a UTF-8 Encoded string.
 * </p>
 *
 * <code>
 * <pre>
 * {@code
 * Pattern SPACE = Pattern.compile(" ");
 *
 * // Build a Site-to-site client config
 * SiteToSiteClientConfig config = new SiteToSiteClient.Builder()
 *   .setUrl("http://localhost:8080/nifi")
 *   .setPortName("Data For Spark")
 *   .buildConfig();
 *
 * SparkConf sparkConf = new SparkConf().setAppName("NiFi-Spark Streaming example");
 * JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(1000L));
 *
 * // Create a JavaReceiverInputDStream using a NiFi receiver so that we can pull data from
 * // specified Port
 * JavaReceiverInputDStream<NiFiDataPacket> packetStream =
 *     ssc.receiverStream(new NiFiReceiver(clientConfig, StorageLevel.MEMORY_ONLY()));
 *
 * // Map the data from NiFi to text, ignoring the attributes
 * JavaDStream<String> text = packetStream.map(new Function<NiFiDataPacket, String>() {
 *   public String call(final NiFiDataPacket dataPacket) throws Exception {
 *     return new String(dataPacket.getContent(), StandardCharsets.UTF_8);
 *   }
 * });
 *
 * // Split the words by spaces
 * JavaDStream<String> words = text.flatMap(new FlatMapFunction<String, String>() {
 *   public Iterable<String> call(final String text) throws Exception {
 *     return Arrays.asList(SPACE.split(text));
 *   }
 * });
 *
 * // Map each word to the number 1, then aggregate by key
 * JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
 *   new PairFunction<String, String, Integer>() {
 *     public Tuple2<String, Integer> call(String s) {
 *       return new Tuple2<String, Integer>(s, 1);
 *     }
 *   }).reduceByKey(new Function2<Integer, Integer, Integer>() {
 *     public Integer call(Integer i1, Integer i2) {
 *       return i1 + i2;
 *     }
 *    }
 *  );
 *
 * // print the results
 * wordCounts.print();
 * ssc.start();
 * ssc.awaitTermination();
 * }
 * </pre>
 * </code>
 */
public class NiFiReceiver extends Receiver<NiFiDataPacket> {

    private static final long serialVersionUID = 3067274587595578836L;
    private final SiteToSiteClientConfig clientConfig;

    public NiFiReceiver(final SiteToSiteClientConfig clientConfig, final StorageLevel storageLevel) {
        super(storageLevel);
        this.clientConfig = clientConfig;
    }

    @Override
    public void onStart() {
        final Thread thread = new Thread(new ReceiveRunnable());
        thread.setDaemon(true);
        thread.setName("NiFi Receiver");
        thread.start();
    }

    @Override
    public void onStop() {
    }

    class ReceiveRunnable implements Runnable {

        public ReceiveRunnable() {
        }

        @Override
        public void run() {
            try {
                final SiteToSiteClient client = new SiteToSiteClient.Builder().fromConfig(clientConfig).build();
                try {
                    while (!isStopped()) {
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
                            final NiFiDataPacket NiFiDataPacket = new StandardNiFiDataPacket(data, attributes);
                            dataPackets.add(NiFiDataPacket);
                            dataPacket = transaction.receive();
                        } while (dataPacket != null);

                        // Confirm transaction to verify the data
                        transaction.confirm();

                        store(dataPackets.iterator());

                        transaction.complete();
                    }
                } finally {
                    try {
                        client.close();
                    } catch (final IOException ioe) {
                        reportError("Failed to close client", ioe);
                    }
                }
            } catch (final IOException ioe) {
                restart("Failed to receive data from NiFi", ioe);
            }
        }
    }
}
