package org.apache.nifi.util.console.utils;

import org.apache.nifi.properties.scheme.PropertyProtectionScheme;

import java.util.Arrays;
import java.util.Iterator;

public class ResolveEncryptionSchemes implements Iterable  {

    @Override
    public Iterator iterator() {

        return Arrays.stream(PropertyProtectionScheme.values()).iterator();

//        String[] files = new File(".").list();
//        return files == null ? Collections.emptyIterator() : Arrays.asList(files).iterator();
    }
}
