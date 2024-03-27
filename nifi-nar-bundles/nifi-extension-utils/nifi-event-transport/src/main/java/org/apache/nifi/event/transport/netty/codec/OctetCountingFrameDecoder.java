/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.event.transport.netty.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.TooLongFrameException;

import java.nio.charset.Charset;

import static io.netty.util.internal.ObjectUtil.checkPositive;


/**
 * A decoder that implements the Octet Counting framing mechanism described in
 * <a href="https://datatracker.ietf.org/doc/html/rfc6587#section-3.4.1">RFC 6587 section 3.4.1</a>.
 * Octet Counting is a method for framing messages by prefixing each message with its length.
 * <p>
 * This class extends {@link DelimiterBasedFrameDecoder} to decode the incoming {@link ByteBuf} based on the
 * Octet Counting framing. The core logic of this class is heavily inspired by {@link LengthFieldBasedFrameDecoder}.
 * </p>
 *
 * @see <a href="https://datatracker.ietf.org/doc/html/rfc6587#section-3.4.1">RFC 6587 section 3.4.1</a>
 * @see LengthFieldBasedFrameDecoder
 */
public class OctetCountingFrameDecoder extends DelimiterBasedFrameDecoder {

    private final int maxFrameLength;
    private final int maxLengthFieldLength;
    private final boolean failFast;
    private final boolean strict;
    private boolean discardingTooLongFrame;
    private long tooLongFrameLength;
    private long bytesToDiscard;
    private int frameLengthInt = -1;

    /**
     * Creates a new instance.
     *
     * @param maxFrameLength       the maximum length of the frame.  If the length of the frame is
     *                             greater than this value, {@link TooLongFrameException} will be
     *                             thrown.
     * @param maxLengthFieldLength the length of the length field
     * @param demarcator           fallback demarcator used when OctetCounting fails
     */
    public OctetCountingFrameDecoder(int maxFrameLength, int maxLengthFieldLength, ByteBuf demarcator) {
        this(maxFrameLength, maxLengthFieldLength, demarcator, true);
    }


    /**
     * Creates a new instance.
     *  @param maxFrameLength       the maximum length of the frame.  If the length of the frame is
     *                             greater than this value, {@link TooLongFrameException} will be
     *                             thrown.
     * @param maxLengthFieldLength the length of the length field
     * @param demarcator           fallback demarcator used when OctetCounting fails
     * @param failFast             If <tt>true</tt>, a {@link TooLongFrameException} is thrown as
     *                             soon as the decoder notices the length of the frame will exceed
     *                             <tt>maxFrameLength</tt> regardless of whether the entire frame
     *                             has been read.  If <tt>false</tt>, a {@link TooLongFrameException}
     */
    public OctetCountingFrameDecoder(
            int maxFrameLength, int maxLengthFieldLength,
            ByteBuf demarcator, boolean failFast) {
        super(maxFrameLength, true, demarcator == null? Unpooled.wrappedBuffer("\n".getBytes()):demarcator);

        checkPositive(maxFrameLength, "maxFrameLength");

        this.strict = demarcator == null;
        this.maxFrameLength = maxFrameLength;
        this.maxLengthFieldLength = maxLengthFieldLength;
        this.failFast = failFast;
    }


    /**
     * Decodes the specified region of the buffer into an unadjusted frame length.  The default implementation is
     * capable of decoding the specified region into an unsigned 8/16/24/32/64 bit integer.  Override this method to
     * decode the length field encoded differently.  Note that this method must not modify the state of the specified
     * buffer (e.g. {@code readerIndex}, {@code writerIndex}, and the content of the buffer.)
     *
     * @throws DecoderException if failed to decode the specified region
     */
    protected static long getFrameLength(ByteBuf in, int maxLength) {
        int offset = in.readerIndex();
        char c = (char) in.getByte(offset);
        int i = 0;
        if (!('1' <= c && c <= '9'))
            throw new CorruptedFrameException("OctetCounting frames should start with a digit other than 0, but got '" + c + "'.");
        do {
            i++;
            if (i > maxLength) {
                throw new TooLongFrameException("Length field exceed max length : " + maxLength);
            }
            if (!in.isReadable(i + 1)) return -1;
            c = (char) in.getByte(offset + i);
        } while (Character.isDigit(c));
        if (c != ' ') {
            throw new CorruptedFrameException("OctetCounting frames should start with a number and a space, but got '" + c + "' after the number.");
        }
        in.skipBytes(i+1);
        return Integer.parseInt(in.toString(offset, i, Charset.defaultCharset()));
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        try{
            return decode0(ctx, in);
        }catch (DecoderException e){
            if (!strict) {
                return super.decode(ctx, in);
            } else {
                throw e;
            }
        }
    }

    private void discardingTooLongFrame(ByteBuf in) {
        long bytesToDiscard = this.bytesToDiscard;
        int localBytesToDiscard = (int) Math.min(bytesToDiscard, in.readableBytes());
        in.skipBytes(localBytesToDiscard);
        bytesToDiscard -= localBytesToDiscard;
        this.bytesToDiscard = bytesToDiscard;

        failIfNecessary(false);
    }

    private void exceededFrameLength(ByteBuf in, long frameLength) {
        long discard = frameLength - in.readableBytes();
        tooLongFrameLength = frameLength;

        if (discard < 0) {
            // buffer contains more bytes than the frameLength, so we can discard all now
            in.skipBytes((int) frameLength);
        } else {
            // Enter the discard mode and discard everything received so far.
            discardingTooLongFrame = true;
            bytesToDiscard = discard;
            in.skipBytes(in.readableBytes());
        }
        failIfNecessary(true);
    }

    /**
     * Create a frame out of the {@link ByteBuf} and return it.
     *
     * @param ctx the {@link ChannelHandlerContext} which this {@link ByteToMessageDecoder} belongs to
     * @param in  the {@link ByteBuf} from which to read data
     * @return frame           the {@link ByteBuf} which represent the frame or {@code null} if no frame could
     * be created.
     */
    protected Object decode0(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        long frameLength = 0;
        if (frameLengthInt == -1) { // new frame

            if (discardingTooLongFrame) {
                discardingTooLongFrame(in);
            }

            frameLength = getFrameLength(in, maxLengthFieldLength);

            if (frameLength == -1) { //not enough bytes
                return null;
            }

            if (frameLength > maxFrameLength) {
                exceededFrameLength(in, frameLength);
                return null;
            }
            // never overflows because it's less than maxFrameLength
            frameLengthInt = (int) frameLength;
        }
        if (in.readableBytes() < frameLengthInt) { // frameLengthInt exist , just check buf
            return null;
        }

        // extract frame
        int readerIndex = in.readerIndex();
        ByteBuf frame = in.retainedSlice(readerIndex, frameLengthInt);
        in.readerIndex(readerIndex + frameLengthInt);
        frameLengthInt = -1; // start processing the next frame
        return frame;
    }

    private void failIfNecessary(boolean firstDetectionOfTooLongFrame) {
        if (bytesToDiscard == 0) {
            // Reset to the initial state and tell the handlers that
            // the frame was too large.
            long tooLongFrameLength = this.tooLongFrameLength;
            this.tooLongFrameLength = 0;
            discardingTooLongFrame = false;
            if (!failFast || firstDetectionOfTooLongFrame) {
                fail(tooLongFrameLength);
            }
        } else {
            // Keep discarding and notify handlers if necessary.
            if (failFast && firstDetectionOfTooLongFrame) {
                fail(tooLongFrameLength);
            }
        }
    }

    private void fail(long frameLength) {
        if (frameLength > 0) {
            throw new TooLongFrameException(
                    "Frame length exceeds " + maxFrameLength +
                            ": " + frameLength + " - discarded");
        } else {
            throw new TooLongFrameException(
                    "Frame length exceeds " + maxFrameLength +
                            " - discarding");
        }
    }
}
