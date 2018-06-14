package org.apache.nifi.controller.queue.clustered.client;

import org.apache.nifi.controller.repository.FlowFileRecord;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class StandardLoadBalanceFlowFileCodec implements LoadBalanceFlowFileCodec {

    @Override
    public void encode(final FlowFileRecord flowFile, final OutputStream destination) throws IOException {
        final DataOutputStream out = new DataOutputStream(destination);

        out.writeInt(flowFile.getAttributes().size());
        for (final Map.Entry<String, String> entry : flowFile.getAttributes().entrySet()) {
            writeString(entry.getKey(), out);
            writeString(entry.getValue(), out);
        }

        out.writeLong(flowFile.getLineageStartDate());
        out.writeLong(flowFile.getEntryDate());
    }

    private void writeString(final String value, final DataOutputStream out) throws IOException {
        final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

}
