package org.apache.nifi.jasn1;

import com.beanit.jasn1.ber.types.BerType;
import org.apache.nifi.logging.ComponentLog;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.Iterator;

public interface RecordModelIteratorProvider {

    Iterator<BerType> iterator(InputStream inputStream, ComponentLog logger, Class<? extends BerType> rootClass, String recordField, Field seqOfField);
}
