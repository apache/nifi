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

import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Utils;

import java.io.Serializable;

/**
 * Example topology that pulls data from a NiFi Output Port named 'Data for Storm' and writes the same
 * data back to a NiFi Input Port named 'Data from Storm'.
 */
public class NiFiStormTopology {

    public static void main( String[] args ) {
        // Build a Site-To-Site client config for pulling data
        final SiteToSiteClientConfig inputConfig = new SiteToSiteClient.Builder()
                .url("http://localhost:8080/nifi")
                .portName("Data for Storm")
                .buildConfig();

        // Build a Site-To-Site client config for pushing data
        final SiteToSiteClientConfig outputConfig = new SiteToSiteClient.Builder()
                .url("http://localhost:8080/nifi")
                .portName("Data from Storm")
                .buildConfig();

        final int tickFrequencySeconds = 5;
        final NiFiDataPacketBuilder niFiDataPacketBuilder = new SimpleNiFiDataPacketBuilder();
        final NiFiBolt niFiBolt = new NiFiBolt(outputConfig, niFiDataPacketBuilder, tickFrequencySeconds)
                //.withBatchSize(1)
                ;

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("nifiInput", new NiFiSpout(inputConfig));
        builder.setBolt("nifiOutput", niFiBolt).shuffleGrouping("nifiInput");

        // Submit the topology running in local mode
        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());

        Utils.sleep(90000);
        cluster.shutdown();
    }

    /**
     * Simple builder that returns the incoming data packet.
     */
    static class SimpleNiFiDataPacketBuilder implements NiFiDataPacketBuilder, Serializable {

        private static final long serialVersionUID = 3067274587595578836L;

        @Override
        public NiFiDataPacket createNiFiDataPacket(Tuple tuple) {
            return (NiFiDataPacket) tuple.getValueByField(NiFiSpout.NIFI_DATA_PACKET);
        }
    }

}
