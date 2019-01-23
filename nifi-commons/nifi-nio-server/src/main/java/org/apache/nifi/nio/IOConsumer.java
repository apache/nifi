package org.apache.nifi.nio;

import java.io.IOException;

@FunctionalInterface
public interface IOConsumer<T> {
    void accept(T t) throws IOException;
}
