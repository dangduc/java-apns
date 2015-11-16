package com.notnoop.apns.integration;

import java.util.Set;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("deprecation")
public class ApnsSimulatorLongRunningTest extends ApnsSimulatorTestBase {

    final Logger logger = LoggerFactory.getLogger(ApnsSimulatorLongRunningTest.class);

    @Test
    public void multipleTokensBad_issue145() throws InterruptedException {
        final int rounds = 15;
        for (int i = 0; i < rounds; ++i) {
            logger.debug("*********** "+i);
            Set<Integer> notificationsShouldReceive = send(8, 0);
            assertNotificationsReceived(notificationsShouldReceive);
        }

    }

}
