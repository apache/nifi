package org.apache.nifi.nio;

import java.io.IOException;

@FunctionalInterface
public interface IORunnable {
    void run() throws IOException;
}
