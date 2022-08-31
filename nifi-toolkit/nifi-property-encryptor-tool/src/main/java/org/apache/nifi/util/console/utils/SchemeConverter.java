package org.apache.nifi.util.console.utils;

import org.apache.nifi.properties.scheme.ProtectionScheme;
import org.apache.nifi.properties.scheme.StandardProtectionSchemeResolver;
import picocli.CommandLine;

public class SchemeConverter implements CommandLine.ITypeConverter<ProtectionScheme> {
    @Override
    public ProtectionScheme convert(String s) throws Exception {
        return new StandardProtectionSchemeResolver().getProtectionScheme(s);
    }
}
