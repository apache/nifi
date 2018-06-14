package org.apache.nifi.controller.queue.clustered.client;

import org.apache.nifi.controller.MockFlowFileRecord;
import org.apache.nifi.controller.queue.clustered.protocol.LoadBalanceProtocolConstants;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.flowfile.FlowFile;
import org.junit.Test;
import org.mockito.Mockito;
import org.omg.IOP.Codec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;
import java.util.zip.Checksum;

import static org.junit.Assert.assertArrayEquals;

public class TestSocketLoadBalanceTransaction {

    @Test
    public void testSunnyCase() throws IOException {
        final LoadBalanceFlowFileCodec codec = new StandardLoadBalanceFlowFileCodec();

        final InputStream clientIn = new ByteArrayInputStream(new byte[] {LoadBalanceProtocolConstants.CONFIRM_CHECKSUM, LoadBalanceProtocolConstants.CONFIRM_COMPLETE_TRANSACTION});

        final ByteArrayOutputStream clientOut = new ByteArrayOutputStream();
        final DataOutputStream dos = new DataOutputStream(clientOut);

        final Socket socket = Mockito.mock(Socket.class);
        Mockito.when(socket.getOutputStream()).thenReturn(dos);
        Mockito.when(socket.getInputStream()).thenReturn(clientIn);

        final SocketLoadBalanceTransaction transaction = new SocketLoadBalanceTransaction(socket, codec, "unit-test-connection", "Unit Test");

        final FlowFileRecord flowFile = new MockFlowFileRecord(5);
        transaction.send(flowFile, new ByteArrayInputStream("hello".getBytes()));

        final FlowFileRecord flowFile2 = new MockFlowFileRecord(8);
        transaction.send(flowFile2, new ByteArrayInputStream("good-bye".getBytes()));

        transaction.complete();

        final byte[] dataSent = clientOut.toByteArray();

        final Checksum expectedChecksum = new CRC32();
        final ByteArrayOutputStream expectedOut = new ByteArrayOutputStream();
        final DataOutputStream expectedDos = new DataOutputStream(new CheckedOutputStream(expectedOut, expectedChecksum));

        expectedDos.writeUTF("unit-test-connection");

        expectedDos.write(LoadBalanceProtocolConstants.MORE_FLOWFILES);
        expectedDos.writeInt(1); // 1 attribute
        expectedDos.writeInt(4); // length of attribute
        expectedDos.write("uuid".getBytes());
        expectedDos.writeInt(flowFile.getAttribute("uuid").length());
        expectedDos.write(flowFile.getAttribute("uuid").getBytes());
        expectedDos.writeLong(flowFile.getLineageStartDate()); // lineage start date
        expectedDos.writeLong(flowFile.getEntryDate()); // entry date
        expectedDos.write(LoadBalanceProtocolConstants.DATA_FRAME_FOLLOWS);
        expectedDos.writeShort(5);
        expectedDos.write("hello".getBytes());
        expectedDos.write(LoadBalanceProtocolConstants.NO_DATA_FRAME);

        expectedDos.write(LoadBalanceProtocolConstants.MORE_FLOWFILES);
        expectedDos.writeInt(1); // 1 attribute
        expectedDos.writeInt(4); // length of attribute
        expectedDos.write("uuid".getBytes());
        expectedDos.writeInt(flowFile2.getAttribute("uuid").length());
        expectedDos.write(flowFile2.getAttribute("uuid").getBytes());
        expectedDos.writeLong(flowFile2.getLineageStartDate()); // lineage start date
        expectedDos.writeLong(flowFile2.getEntryDate()); // entry date
        expectedDos.write(LoadBalanceProtocolConstants.DATA_FRAME_FOLLOWS);
        expectedDos.writeShort(8);
        expectedDos.write("good-bye".getBytes());
        expectedDos.write(LoadBalanceProtocolConstants.NO_DATA_FRAME);

        expectedDos.write(LoadBalanceProtocolConstants.NO_MORE_FLOWFILES);
        expectedDos.writeLong(expectedChecksum.getValue());
        expectedDos.write(LoadBalanceProtocolConstants.COMPLETE_TRANSACTION);

        final byte[] expectedSent = expectedOut.toByteArray();
        assertArrayEquals(expectedSent, dataSent);
    }

    @Test
    public void testEmptyTransaction() throws IOException {
        final LoadBalanceFlowFileCodec codec = new StandardLoadBalanceFlowFileCodec();

        final InputStream clientIn = new ByteArrayInputStream(new byte[] {LoadBalanceProtocolConstants.CONFIRM_CHECKSUM, LoadBalanceProtocolConstants.CONFIRM_COMPLETE_TRANSACTION});

        final ByteArrayOutputStream clientOut = new ByteArrayOutputStream();
        final DataOutputStream dos = new DataOutputStream(clientOut);

        final Socket socket = Mockito.mock(Socket.class);
        Mockito.when(socket.getOutputStream()).thenReturn(dos);
        Mockito.when(socket.getInputStream()).thenReturn(clientIn);

        final SocketLoadBalanceTransaction transaction = new SocketLoadBalanceTransaction(socket, codec, "unit-test-connection", "Unit Test");
        transaction.complete();

        final Checksum expectedChecksum = new CRC32();
        final ByteArrayOutputStream expectedOut = new ByteArrayOutputStream();
        final DataOutputStream expectedDos = new DataOutputStream(new CheckedOutputStream(expectedOut, expectedChecksum));

        expectedDos.writeUTF("unit-test-connection");

        expectedDos.write(LoadBalanceProtocolConstants.NO_MORE_FLOWFILES);
        expectedDos.writeLong(expectedChecksum.getValue());
        expectedDos.write(LoadBalanceProtocolConstants.COMPLETE_TRANSACTION);

        final byte[] expectedSent = expectedOut.toByteArray();
        assertArrayEquals(expectedSent, clientOut.toByteArray());
    }
}
