package org.apache.nifi.remote.protocol.raw;

import org.apache.nifi.remote.PeerDescription;
import org.apache.nifi.remote.RemoteResourceManager;
import org.apache.nifi.remote.client.KeystoreType;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.cluster.ClusterNodeInformation;
import org.apache.nifi.remote.cluster.NodeInformant;
import org.apache.nifi.remote.cluster.NodeInformation;
import org.apache.nifi.remote.protocol.socket.SocketFlowFileServerProtocol;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class AbstractS2SMessageSequenceTest {

    protected static final NodeInformation NODE_1 = new NodeInformation("nifi1", 8081, null, 8080, true, 100);
    protected static final NodeInformation NODE_2 = new NodeInformation("nifi2", 8082, null, 8080, true, 200);
    // Simulating a node not having s2s port configured.
    protected static final NodeInformation NODE_3 = new NodeInformation("nifi3", null, null, 8080, true, 300);
    protected static final NodeInformation NODE_4 = new NodeInformation("nifi4", 8084, null, 8080, true, 400);

    protected static final NodeInformant NODE_INFORMANT = mock(NodeInformant.class);

    protected static final PeerDescription CLIENT_PEER = new PeerDescription("client", 54321, true);

    protected final Logger log = LoggerFactory.getLogger(getClass());

    @BeforeClass
    public static void setup() {
        RemoteResourceManager.setServerProtocolImplementation(SocketFlowFileServerProtocol.RESOURCE_NAME, NIOSocketFlowFileServerProtocol.class);

        final ClusterNodeInformation nodeInformation = mock(ClusterNodeInformation.class);
        when(NODE_INFORMANT.getNodeInformation()).thenReturn(nodeInformation);
        when(nodeInformation.getNodeInformation()).thenReturn(Arrays.asList(NODE_1, NODE_2, NODE_3, NODE_4));
    }

    SiteToSiteClient createClient(Integer apiPort, String portName) {
        return new SiteToSiteClient.Builder()
            .url("http://localhost:" + apiPort + "/nifi")
            .portName(portName)
            .truststoreFilename(this.getClass().getResource("/client/truststore.jks").getFile())
            .truststorePass("password")
            .truststoreType(KeystoreType.JKS)
            .keystoreFilename(this.getClass().getResource("/client/keystore.jks").getFile())
            .keystorePass("password")
            .keystoreType(KeystoreType.JKS)
            .build();
    }
}
