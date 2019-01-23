package org.apache.nifi.nio;

import java.net.InetSocketAddress;

public interface MessageSequence extends Clearable {

    /**
     * <p>This method is called when the server accepts a new client connection.</p>
     * <p>By default, this method does nothing. Subclasses can override this to do something when accepting new connection.</p>
     * @param clientAddress the client address
     * @param secure true if the connection is secure
     */
    default void onAccept(InetSocketAddress clientAddress, boolean secure) {

    }

    /**
     * Returns the action which will handle the next bytes received, or next bytes to be sent.
     * @return the next action based on the current status, or null if it reached to the end of communication.
     */
    MessageAction getNextAction();

    /**
     * Returns if this sequence has finished.
     * @return true if this sequence has finished.
     */
    default boolean isDone() {
        return getNextAction() == null;
    }

    default boolean shouldCloseConnectionWhenDone() {
        return false;
    }

    /**
     * <p>Returns if the exception is known to happen, based on the current sequence sequence state.</p>
     *
     * <p>If it is known, {@link MessageSequenceHandler} does not execute its onSequenceFailure callback.</p>
     * @param e exception happened
     * @return true if the exception is known
     */
    default boolean isKnownException(Exception e) {
        return false;
    }

    default void onEndOfReadStream() {

    }
}
