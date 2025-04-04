package org.apache.nifi.processors.camel;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;

import java.io.InputStream;

public class NifiCamelOutRouteBuilder extends RouteBuilder {
    private final Processor routeProcessor = new CamelRouteOutputValidatorProcessor();
    private final NifiCamelRoute route;
    public final String outEndpointHeaderName;
    public static final String OUTPUT_STREAM_HEADER_NAME = "stream";

    public NifiCamelOutRouteBuilder(NifiCamelRoute route, String outEndpointHeaderName) {
        this.route = route;
        this.outEndpointHeaderName = outEndpointHeaderName;
    }

    public void configure() {
        from(route.getUri())
            .setHeader(outEndpointHeaderName, constant(route.getName()))
            .process(routeProcessor)
            .to(OUTPUT_STREAM_HEADER_NAME + ":header");
    }

    private static class CamelRouteOutputValidatorProcessor implements Processor {
        @Override
        public void process(Exchange exchange) throws ClassCastException {
            Object stream = exchange.getVariable(OUTPUT_STREAM_HEADER_NAME);
            if (stream == null) {
                throw new NullPointerException("Expected variable " + OUTPUT_STREAM_HEADER_NAME + " to be non-null");
            }
            Object body = exchange.getIn().getBody();
            if (body != null && !(body instanceof InputStream)) {
                throw new ClassCastException("Expected route output to be an instance of java.io.InputStream. Actual: " + body.getClass().getCanonicalName());
            }
            exchange.getIn().setHeader(OUTPUT_STREAM_HEADER_NAME, stream);
        }
    }
}
