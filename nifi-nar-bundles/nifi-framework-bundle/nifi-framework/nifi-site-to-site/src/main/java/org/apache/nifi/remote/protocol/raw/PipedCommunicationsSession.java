package org.apache.nifi.remote.protocol.raw;

import org.apache.nifi.remote.io.StandardCommunicationsSession;
import org.apache.nifi.remote.io.StandardInput;
import org.apache.nifi.remote.io.StandardOutput;
import org.apache.nifi.remote.protocol.CommunicationsSession;

import java.io.InputStream;
import java.io.OutputStream;

// TODO: standard? No, need to implement timeout.
public class PipedCommunicationsSession extends StandardCommunicationsSession implements CommunicationsSession {

    PipedCommunicationsSession(InputStream inputStream, OutputStream outputStream) {
        super(new StandardInput(inputStream, false), new StandardOutput(outputStream));
    }

}
