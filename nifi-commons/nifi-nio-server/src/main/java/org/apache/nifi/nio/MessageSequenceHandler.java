package org.apache.nifi.nio;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.apache.nifi.nio.ByteBufferUtil.adjustBufferForNextFill;

public class MessageSequenceHandler<T extends MessageSequence> {

    private static final Logger log = LoggerFactory.getLogger(MessageSequenceHandler.class);

    private SSLContext sslContext;
    private final Configurations conf;
    private final Supplier<T> messageSequenceSupplier;
    private final Consumer<T> onSequenceSuccess;
    private final BiConsumer<T, Exception> onSequenceFailure;

    private final ExecutorService ioThreadPool;

    private volatile boolean isRunning = false;

    /**
     * Constructor.
     * @param name the name of the handler
     * @param messageSequenceSupplier the function to create new MessageSequence instance when a connection is created
     * @param onSequenceSuccess called when a MessageSequence is done
     * @param onSequenceFailure called when a MessageSequence failed
     */
    public MessageSequenceHandler(String name,
                                  Supplier<T> messageSequenceSupplier,
                                  Consumer<T> onSequenceSuccess,
                                  BiConsumer<T, Exception> onSequenceFailure) {
        this(name, new Configurations.Builder().build(), messageSequenceSupplier, onSequenceSuccess, onSequenceFailure);
    }

    /**
     * Constructor.
     * @param name the name of the handler
     * @param conf the configurations
     * @param messageSequenceSupplier the function to create new MessageSequence instance when a connection is created
     * @param onSequenceSuccess called when a MessageSequence is done
     * @param onSequenceFailure called when a MessageSequence failed
     */
    public MessageSequenceHandler(String name,
                                  Configurations conf,
                                  Supplier<T> messageSequenceSupplier,
                                  Consumer<T> onSequenceSuccess,
                                  BiConsumer<T, Exception> onSequenceFailure) {
        this.conf = conf;
        this.messageSequenceSupplier = messageSequenceSupplier;
        this.onSequenceSuccess = onSequenceSuccess;
        this.onSequenceFailure = onSequenceFailure;

        final ThreadFactory defaultThreadFactory = Executors.defaultThreadFactory();
        final AtomicInteger threadCounter = new AtomicInteger(0);
        ioThreadPool = Executors.newFixedThreadPool(conf.getIoThreadPoolSize(), runnable -> {
            final Thread thread = defaultThreadFactory.newThread(runnable);
            thread.setName(name + "-IO-" + threadCounter.incrementAndGet());
            return thread;
        });

    }

    /**
     * Start the message sequence handler.
     * @param serverSocketChannel the server socket channel to be handled by this handler
     * @param sslContext the SSL context, can be null for non-secure communication
     * @throws IOException if the handler failed to start.
     * Any Exception occurred while processing message sequences after the handler has started up,
     * the handler catches is, and call onSequenceFailure.
     * So that the thread running this handler will not terminate until stop is called.
     */
    public void start(final ServerSocketChannel serverSocketChannel, final SSLContext sslContext) throws IOException {

        this.sslContext = sslContext;

        // Throw any IOExceptions occurred before starting selectKeys.
        // Once it starts the loop, each IOException should be caught inside as it's tied to its client connection,
        // so that the loop can keep running for other clients.
        final Selector selector = Selector.open();
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

        isRunning = true;
        while (isRunning) {
            selectKeys(selector);
        }
    }

    public void stop() {
        log.info("Stopping {}", this);
        isRunning = false;
    }


    private enum ActionResult {
        NOT_READY,
        CONTINUE,
        COMPLETED,
    }


    private ActionResult executeAction(MessageAction action, Supplier<String> remainingInfo, Supplier<Boolean> isReady, IOSupplier<Boolean> isCompleted) throws IOException {

        final Boolean ready = isReady.get();
        if (log.isTraceEnabled()) {
            log.trace("Remaining {}, Action {} is ready ? {}", remainingInfo.get(), action, ready);
        }

        if (!ready) {
            return ActionResult.NOT_READY;
        }

        if (isCompleted.get()) {
            return ActionResult.COMPLETED;
        }

        return ActionResult.CONTINUE;
    }

    /**
     * Process buffers, read buffered data and writes response.
     * @return if the sequence is done, null,
     * otherwise the last MessageAction which will be called again at the next IO cycle
     */
    private MessageAction processBuffers(final ChannelAttachment attachment, final MessageSequence sequence,
                                final ByteBuffer readBuffer, final ByteBuffer writeBuffer) throws IOException {

        MessageAction action = null;
        while (isRunning && !sequence.isDone()
            && (readBuffer.hasRemaining() || writeBuffer.hasRemaining())) {

            action = sequence.getNextAction();

            // No next action. Break the loop.
            if (action == null) {
                break;
            }

            // TODO: after ChannelToBoundStream finished, the action does not complete immediately, but it's completed at the next run
            // TODO: and the next action becomes WriteBytes. And its prepare is not called, thus the bytes become null.
            attachment.prepareAction(action);

            log.trace("Executing action {} readBuffer={}, writeBuffer={}", action, readBuffer, writeBuffer);

            final ActionResult actionResult;

            if (action instanceof MessageAction.SSLAction) {
                MessageAction.SSLAction sslAction = (MessageAction.SSLAction) action;
                actionResult = sslAction.execute(attachment.getSslEngine())
                    ? ActionResult.COMPLETED : ActionResult.CONTINUE;

            } else if (action instanceof MessageAction.Read) {
                MessageAction.Read read = (MessageAction.Read) action;
                actionResult = executeAction(read,
                    () -> readBuffer.remaining() + " bytes",
                    () -> read.isBufferReady(readBuffer),
                    () -> read.execute(readBuffer));

            } else if (action instanceof MessageAction.Write) {
                MessageAction.Write write = (MessageAction.Write) action;
                actionResult = executeAction(write,
                    () -> writeBuffer.remaining() + " bytes",
                    () -> write.isBufferReady(writeBuffer),
                    () -> write.execute(writeBuffer));


            } else if (action instanceof MessageAction.Both) {
                MessageAction.Both both = (MessageAction.Both) action;
                actionResult = executeAction(both,
                    () -> readBuffer.remaining() + " bytes for read, "
                        + writeBuffer.remaining() + " bytes for write",
                    () -> both.isBufferReady(readBuffer, writeBuffer),
                    () -> both.execute(readBuffer, writeBuffer));


            } else {
                throw new RuntimeException(action + " should implement either MessageAction.Read, Write or Both.");
            }

            log.trace("{} results {} readBuffer={}, writeBuffer={}", action, actionResult, readBuffer, writeBuffer);
            if (ActionResult.NOT_READY.equals(actionResult)) {
                log.trace("The action {} is not ready. Waiting for another I/O cycle.", action);
                break;

            } else if (ActionResult.CONTINUE.equals(actionResult)) {
                log.trace("The action {} has not completed. Waiting for another I/O cycle.", action);
                break;

            } else if (ActionResult.COMPLETED.equals(actionResult)) {
                log.trace("The action {} has completed.", action);
                // Continue with the next action.
            }

        }

        return action;
    }

    @SuppressWarnings("unchecked")
    private void selectKeys(Selector selector) {
        try {
            selector.selectNow();
        } catch (IOException e) {
            log.warn("Failed to select keys due to " + e, e);
            try {
                Thread.sleep(10_000);
            } catch (InterruptedException ie) {
                log.info("Thread interrupted");
            }
        }

        final Iterator<SelectionKey> keyIterator = selector.selectedKeys().iterator();
        final Map<SelectionKey, Future<Boolean>> ioResults = new HashMap<>();
        while (keyIterator.hasNext() && isRunning) {

            final SelectionKey key = keyIterator.next();

            if (key.isAcceptable()) {
                try {
                    // Accept new connection.
                    ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
                    SocketChannel clientChannel = serverSocketChannel.accept();
                    clientChannel.configureBlocking(false);

                    // Create a new stateful message sequence for this channel.
                    final T sequence = messageSequenceSupplier.get();
                    final SSLEngine sslEngine = sslContext != null ? sslContext.createSSLEngine() : null;
                    final boolean secure = sslEngine != null;
                    if (secure) {
                        sslEngine.setUseClientMode(false);
                        sslEngine.setNeedClientAuth(true);
                        sslEngine.beginHandshake();
                    }

                    final ChannelAttachment attachment = new ChannelAttachment(sequence, sslEngine);
                    attachment.setLastCommunicationTimestamp(System.currentTimeMillis());
                    // Register for READ operation only.
                    // Because channels are mostly writable except when the buffer is full.
                    // Registering for WRITE operation will cause high CPU usage unnecessarily.
                    final MessageAction initialAction = sequence.getNextAction();
                    if (isOpWriteRequired(initialAction)) {
                        clientChannel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE, attachment);
                    } else {
                        clientChannel.register(selector, SelectionKey.OP_READ, attachment);
                    }

                    final InetSocketAddress clientAddress = (InetSocketAddress) clientChannel.getRemoteAddress();
                    log.debug("Calling onAccept {}", sequence);
                    sequence.onAccept(clientAddress, secure);

                } catch (Exception e) {
                    log.error("Failed to establish a connection due to " + e, e);

                } finally {
                    // Connection has been established, or failed.
                    keyIterator.remove();
                }


            } else if (key.isValid()) {

                final ChannelAttachment<T> attachment = (ChannelAttachment<T>) key.attachment();

                // Handle IO.
                final T sequence = (T) attachment.getSequence();

                final Future<Boolean> ioResult = ioThreadPool.submit(() -> {
                    try {
                        return handleIO(key);
                    } catch (Exception e) {
                        if (sequence.isKnownException(e)) {
                            log.info("Failed to process message due to " + e);

                        } else {
                            log.error("Failed to process message due to " + e, e);
                            log.debug("Calling onSequenceFailure {}", sequence);
                            onSequenceFailure.accept(sequence, e);
                        }

                        // Returning true because this sequence has ended.
                        return true;
                    }
                });
                ioResults.put(key, ioResult);

            }

        }

        // This log can be helpful to decide the number of IO threads.
        // For example, 10 IO threads may not be enough to handle 1000 IO tasks.
        if (log.isTraceEnabled() && ioResults.size() > 0) {
            log.trace("Checking {} ioResults", ioResults.size());
        }

        final long now = System.currentTimeMillis();
        for (Map.Entry<SelectionKey, Future<Boolean>> ioResult : ioResults.entrySet()) {
            final SelectionKey key = ioResult.getKey();
            final ChannelAttachment<T> attachment = (ChannelAttachment<T>) key.attachment();
            try {

                // Remove the key so that it will not use CPU until it gets selected again.
                boolean stillSSLHandshaking = false;
                final SSLEngine sslEngine = attachment.getSslEngine();
                if (sslEngine != null) {
                    final SSLEngineResult.HandshakeStatus sslHandshakeStatus = sslEngine.getHandshakeStatus();
                    if (!sslHandshakeStatus.equals(SSLEngineResult.HandshakeStatus.FINISHED)
                        && !sslHandshakeStatus.equals(SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING)) {
                        stillSSLHandshaking = true;
                    }
                }
                if (stillSSLHandshaking) {
                    log.trace("Still doing SSL handshaking. Handshake status={}", sslEngine.getHandshakeStatus());
                } else {
                    log.trace("Removing key {}", key);
                    key.selector().selectedKeys().remove(key);
                }

                final boolean completed = ioResult.getValue().get(conf.getIoTimeoutMillis(), TimeUnit.MILLISECONDS);
                if (completed) {

                    log.debug("Key {} has finished", key);
                    final T sequence = (T) attachment.getSequence();
                    if (sequence.shouldCloseConnectionWhenDone()) {
                        closeConnection(key);
                        continue;
                    }
                }

                // Check idle time.
                final long idleMillis = now - attachment.getLastCommunicationTimestamp();
                if (idleMillis > conf.getIdleConnectionTimeoutMillis()) {
                    log.debug("Closing idle connection {}, {}", key, idleMillis);
                    closeConnection(key);
                }

            } catch (Exception e) {
                final T sequence = (T) attachment.getSequence();

                log.error("Failed to get async IO result " + e, e);
                log.debug("Calling onSequenceFailure {}", sequence);
                onSequenceFailure.accept(sequence, e);

                closeConnection(key);
            }
        }

    }

    private void closeConnection(SelectionKey key) {
        try {
            log.info("Closing client connection. {}", key);
            key.channel().close();
        } catch (IOException closeException) {
            log.info("Failed to close client connection {} due to " + closeException, key);
        }
    }

    /**
     * Execute IO operations.
     * @param key the key to operate on.
     * @return true if the sequence for the key has been done and its connection should be closed.
     */
    @SuppressWarnings("unchecked")
    private boolean handleIO(final SelectionKey key) throws IOException {

        final SocketChannel clientChannel = (SocketChannel) key.channel();
        final ChannelAttachment<T> attachment = (ChannelAttachment<T>) key.attachment();

        final T sequence = (T) attachment.getSequence();
        final ByteBuffer netReadBuffer = attachment.getNetReadBuffer();
        final ByteBuffer netWriteBuffer = attachment.getNetWriteBuffer();
        final ByteBuffer appReadBuffer = attachment.getAppReadBuffer();
        final ByteBuffer appWriteBuffer = attachment.getAppWriteBuffer();
        final SSLEngine sslEngine = attachment.getSslEngine();

        final boolean readable = key.isReadable();


        while (isRunning) {

            int readFromChannel = 0;
            int writtenToChannel = 0;

            // Reading data from the channel.
            if (readable && netReadBuffer.hasRemaining()) {
                readFromChannel = clientChannel.read(netReadBuffer);
                if (readFromChannel > 0) {
                    log.debug("Read {} bytes from the channel {} {}", readFromChannel, debugSSLHandshake(sslEngine), key);

                } else if (readFromChannel < 0) {
                    final MessageAction nextAction = sequence.getNextAction();
                    boolean hasRemainingReadData = false;
                    if (nextAction instanceof MessageAction.ReadFromChannel) {
                        hasRemainingReadData = ((MessageAction.ReadFromChannel) nextAction).hasRemainingReadData();
                    }

                    if (!hasRemainingReadData) {
                        log.debug("Reached end-of-stream. {} {}", debugSSLHandshake(sslEngine), key);
                        sequence.onEndOfReadStream();

                        if (sequence.isDone()) {
                            onSequenceDone(key, attachment, sequence, appReadBuffer, appWriteBuffer);
                        }
                        return true;
                    }
                }
            }

            // Flip the read buffer to consume.
            netReadBuffer.flip();

            // Handle SSL
            if (sslEngine != null) {
                unwrap(netReadBuffer, appReadBuffer, sslEngine);
            }

            // Process the app buffer.
            MessageAction nextAction = processBuffers(attachment, sequence, appReadBuffer, appWriteBuffer);
            log.trace("Processed. readBuffer={}, writeBuffer={}, key={}", appReadBuffer, appWriteBuffer, key);

            // Adjust the read buffer for the next read.
            adjustBufferForNextFill(appReadBuffer);
            log.trace("Adjusted netReadBuffer={}, appReadBuffer={}, key={}", netReadBuffer, appReadBuffer, key);


            // Handle SSL
            if (sslEngine != null
                // Need to wrap even if there's no data written by the app, if in the middle of SSL handshake.
                && (SSLEngineResult.HandshakeStatus.NEED_WRAP.equals(sslEngine.getHandshakeStatus())
                        || appWriteBuffer.position() > 0)) {
                wrap(netWriteBuffer, appWriteBuffer, sslEngine);
            }

            // Writing data to the channel.
            if (netWriteBuffer.position() > 0) {
                // Flip the write buffer to consume.
                netWriteBuffer.flip();

                writtenToChannel = clientChannel.write(netWriteBuffer);
                if (writtenToChannel > 0) {
                    log.debug("Written {} bytes to the channel {} {}", writtenToChannel, debugSSLHandshake(sslEngine), key);
                }

                // Adjust the write buffer for the next write.
                adjustBufferForNextFill(netWriteBuffer);
                log.trace("Adjusted writeBuffer {} {}", netWriteBuffer, key);
            }


            if (sequence.isDone()) {
                onSequenceDone(key, attachment, sequence, appReadBuffer, appWriteBuffer);

                // If the sequence allows multiple cycles without closing,
                // register interest ops again based on the next action.
                nextAction = sequence.getNextAction();
                if (sequence.shouldCloseConnectionWhenDone() || nextAction == null) {
                    // No need to check next sequence cycle.
                    return true;
                }
            }

            // - When channel is not readable, readFromChannel is -1.
            // - When channel is readable, but doesn't have data, readFromChannel is 0.
            // - Note for the 3rd condition, with a secured channel,
            //   both readFromChannel and writtenToChannel can be less than 0
            //   but netReadBuffer has some remaining data and it needs to be unwrapped at the next cycle.
            if (readFromChannel < 1 && writtenToChannel < 1 && netReadBuffer.position() == 0) {
                log.trace("No I/O happened. Waiting for another I/O cycle. key={}, nextAction={}", key, nextAction);
                if (netWriteBuffer.position() > 0
                    || isOpWriteRequired(nextAction)) {
                    // There is more data to write, but the channel is not writable,
                    // or the app data is not written to the buffer yet.
                    // Stop writing data. But register for WRITEs so that the remaining data can be written
                    // when the channel gets writable again.
                    log.trace("Set interestOps for READ and WRITE");
                    key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
                } else {
                    // If there's no more data to write, register only for READs to reduce CPU usage.
                    log.trace("Set interestOps for READ only");
                    key.interestOps(SelectionKey.OP_READ);
                }
                return false;
            } else {
                // Track last activity.
                attachment.setLastCommunicationTimestamp(System.currentTimeMillis());
            }

        }

        return true;
    }

    private void onSequenceDone(SelectionKey key, ChannelAttachment<T> attachment, T sequence, ByteBuffer appReadBuffer, ByteBuffer appWriteBuffer) {
        log.debug("Sequence is done. readBuffer={}, writeBuffer={}, key={}", appReadBuffer, appWriteBuffer, key);
        if (onSequenceSuccess != null) {
            log.debug("Calling onSequenceSuccess {}", sequence);
            onSequenceSuccess.accept(sequence);
        }
        sequence.clear();
        attachment.onSequenceSuccess();
    }

    private boolean isOpWriteRequired(MessageAction nextAction) {
        return nextAction instanceof MessageAction.Write
            || nextAction instanceof MessageAction.Both
            // ReadAhead needs to be selected even if there's no channel data to read.
            // Because it already read some at the previous cycle, but possibly not consumed yet.
            || nextAction instanceof ReadAhead;
    }

    private String debugSSLHandshake(SSLEngine engine) {
        if (log.isDebugEnabled() && engine != null
            && !engine.getHandshakeStatus().equals(SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING)) {
            return engine.getHandshakeStatus().toString();
        }
        return "";
    }

    private void unwrap(ByteBuffer netReadBuffer, ByteBuffer appReadBuffer, SSLEngine sslEngine) throws SSLException {

        if (netReadBuffer.hasRemaining()) {
            // Unwrap encrypted data into app buffer
            log.trace("unwrapping netReadBuffer={}, appReadBuffer={}", netReadBuffer, appReadBuffer);
            final SSLEngineResult unwrapResult = sslEngine.unwrap(netReadBuffer, appReadBuffer);
            log.trace("unwrap result={} netReadBuffer={}, appReadBuffer={}", unwrapResult, netReadBuffer, appReadBuffer);

            runSSLTaskIfNeeded(sslEngine, unwrapResult);
        }

        // Adjust net buffer to fill more data
        adjustBufferForNextFill(netReadBuffer);

        // Flip app buffer to consume
        appReadBuffer.flip();
    }

    private void runSSLTaskIfNeeded(SSLEngine sslEngine, SSLEngineResult result) {
        if (SSLEngineResult.HandshakeStatus.NEED_TASK.equals(result.getHandshakeStatus())){
            Runnable runnable;
            while ((runnable = sslEngine.getDelegatedTask()) != null) {
                log.trace("running ssl task {}", runnable);
                runnable.run();
            }
        }
    }

    private void wrap(ByteBuffer netWriteBuffer, ByteBuffer appWriteBuffer, SSLEngine sslEngine) throws SSLException {

        log.trace("wrapping netWriteBuffer={} appWriteBuffer={}", netWriteBuffer, appWriteBuffer);
        // Flip app buffer to consume.
        appWriteBuffer.flip();

        final SSLEngineResult wrapResult = sslEngine.wrap(appWriteBuffer, netWriteBuffer);

        runSSLTaskIfNeeded(sslEngine, wrapResult);

        // Adjust app buffer for next write.
        adjustBufferForNextFill(appWriteBuffer);

        log.trace("wrap result={} netWriteBuffer={} appWriteBuffer={}", wrapResult, netWriteBuffer, appWriteBuffer);

    }

}
