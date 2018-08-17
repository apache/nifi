package org.apache.nifi.repository.schema;

import java.io.Closeable;
import java.io.IOException;

public interface RecordIterator extends Closeable {

    Record next() throws IOException;

}
