package org.apache.nifi.events;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TestNodeBulletinProcessingStrategy {

    @Test
    public void testUpdate() {

        NodeBulletinProcessingStrategy nBulletinProcessingStrategy = new NodeBulletinProcessingStrategy();

        nBulletinProcessingStrategy.update(new ComponentBulletin(1));
        nBulletinProcessingStrategy.update(new ComponentBulletin(2));
        nBulletinProcessingStrategy.update(new ComponentBulletin(3));
        nBulletinProcessingStrategy.update(new ComponentBulletin(4));
        nBulletinProcessingStrategy.update(new ComponentBulletin(5));
        assertEquals(5, nBulletinProcessingStrategy.getBulletins().size());

        nBulletinProcessingStrategy.update(new ComponentBulletin(1));
        nBulletinProcessingStrategy.update(new ComponentBulletin(2));
        nBulletinProcessingStrategy.update(new ComponentBulletin(3));
        nBulletinProcessingStrategy.update(new ComponentBulletin(4));
        nBulletinProcessingStrategy.update(new ComponentBulletin(5));
        nBulletinProcessingStrategy.update(new ComponentBulletin(6));
        nBulletinProcessingStrategy.update(new ComponentBulletin(7));
        assertEquals(NodeBulletinProcessingStrategy.MAX_ENTRIES, nBulletinProcessingStrategy.getBulletins().size());

    }

}
