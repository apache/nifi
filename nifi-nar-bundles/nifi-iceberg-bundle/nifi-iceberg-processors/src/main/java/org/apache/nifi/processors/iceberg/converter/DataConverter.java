package org.apache.nifi.processors.iceberg.converter;


/**
 * Interface for data conversion between NiFi Record and Iceberg Record.
 */
public interface DataConverter<D, T> {
    T convert(D data);
}
