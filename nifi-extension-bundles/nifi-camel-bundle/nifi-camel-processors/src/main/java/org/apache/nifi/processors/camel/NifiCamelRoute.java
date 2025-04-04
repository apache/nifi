package org.apache.nifi.processors.camel;

public class NifiCamelRoute {
    private final String name;
    private final String uri;

    NifiCamelRoute(String name) {
        this.name = name;
        this.uri = "direct:" + name;
    }

    public String getName() {
        return name;
    }

    public String getUri() {
        return uri;
    }
}