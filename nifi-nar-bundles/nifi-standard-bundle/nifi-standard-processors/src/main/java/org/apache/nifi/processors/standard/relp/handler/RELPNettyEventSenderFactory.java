package org.apache.nifi.processors.standard.relp.handler;

import io.netty.channel.ChannelHandler;
import org.apache.nifi.event.transport.configuration.TransportProtocol;
import org.apache.nifi.event.transport.netty.NettyEventSenderFactory;
import org.apache.nifi.event.transport.netty.channel.LogExceptionChannelHandler;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.standard.relp.frame.RELPResponseEncoder;
import org.apache.nifi.processors.standard.relp.response.RELPResponse;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * Netty Event Sender Factory for RELP message responses
 */
public class RELPNettyEventSenderFactory extends NettyEventSenderFactory<RELPResponse> {

//    /**
//     * Netty Event Sender Factory using byte array
//     *
//     * @param log Component Log
//     * @param address Remote Address
//     * @param port Remote Port Number
//     * @param protocol Channel Protocol
//     */
//    public ByteArrayNettyEventSenderFactory(final ComponentLog log, final String address, final int port, final TransportProtocol protocol) {
//        super(address, port, protocol);
//        final List<ChannelHandler> handlers = new ArrayList<>();
//        handlers.add(new LogExceptionChannelHandler(log));
//        handlers.add(new ByteArrayEncoder());
//        setHandlerSupplier(() -> handlers);
//    }

    public RELPNettyEventSenderFactory(final ComponentLog log, final String address, final int port, final TransportProtocol protocol, final Charset charset) {
        super(address, port, protocol);
        final RELPResponseEncoder relpResponseEncoder = new RELPResponseEncoder(charset);

        final List<ChannelHandler> handlers = new ArrayList<>();
        handlers.add(new LogExceptionChannelHandler(log));
        handlers.add(relpResponseEncoder);
        setHandlerSupplier(() -> handlers);
    }
}
