package org.apache.nifi.nio;

public interface MultiMessageSequence extends MessageSequence {

    @Override
    default MessageAction getNextAction() {

        final MessageSequence nextSequence = getNextSequence();
        if (nextSequence != null) {
            final MessageAction nextAction = nextSequence.getNextAction();

            if (nextAction != null) {
                return nextAction;
            }
        }

        return null;
    }

    @Override
    default boolean isDone() {
        return getNextSequence() == null;
    }

    /**
     * Returns the message sequence which will handle the next communications.
     * @return the next sequence based on the current status, or null if it reached to the end of communication.
     */
    MessageSequence getNextSequence();


}
