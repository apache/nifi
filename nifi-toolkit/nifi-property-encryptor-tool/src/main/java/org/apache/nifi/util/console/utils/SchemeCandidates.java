package org.apache.nifi.util.console.utils;

import org.apache.nifi.properties.scheme.StandardProtectionSchemeResolver;

import java.util.ArrayList;

public class SchemeCandidates extends ArrayList<String> {
    SchemeCandidates() {
        super(new StandardProtectionSchemeResolver().getSupportedProtectionSchemes());
    }
}
