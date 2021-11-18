package org.apache.nifi.event.transport.netty.channel;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketAddress;

public class NetworkRecordReader implements Closeable {
    private final RecordReaderFactory readerFactory;
    private SocketAddress senderAddress;
    private RecordReader recordReader;
    private final InputStream recordStream;
    private final ComponentLog logger;

    public NetworkRecordReader(final SocketAddress sender, final InputStream recordStream, final RecordReaderFactory factory, final ComponentLog logger) {
        this.senderAddress = sender;
        this.readerFactory = factory;
        this.logger = logger;
        this.recordStream = recordStream;
    }

    public SocketAddress getSender() {
        return senderAddress;
    }

    public RecordReader getRecordReader() throws SchemaNotFoundException, MalformedRecordException, IOException {
        if (recordReader == null) {
            recordReader = readerFactory.createRecordReader(null, recordStream, logger);
        }
        return recordReader;
    }

    @Override
    public void close() throws IOException {
        if (recordReader != null) {
            recordReader.close();
        }
    }
}
