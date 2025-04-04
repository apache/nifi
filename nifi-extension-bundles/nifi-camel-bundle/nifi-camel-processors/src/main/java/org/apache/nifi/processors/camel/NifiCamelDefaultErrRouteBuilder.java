package org.apache.nifi.processors.camel;

import org.apache.camel.builder.RouteBuilder;

public class NifiCamelDefaultErrRouteBuilder extends RouteBuilder {
    private final NifiCamelRoute route;

    public NifiCamelDefaultErrRouteBuilder(NifiCamelRoute route) {
        this.route = route;
    }

    public void configure() {
        from(route.getUri()).throwException(new NifiCamelRouteToErrException());
    }

    private final class NifiCamelRouteToErrException extends Exception {
        public NifiCamelRouteToErrException() {
            super("Camel message routed to " + route.getUri());
        }
    }

}


