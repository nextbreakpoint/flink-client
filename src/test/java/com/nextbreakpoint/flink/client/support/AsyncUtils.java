/*
 * This file is part of Flink Client
 * https://github.com/nextbreakpoint/flink-client
 */
package com.nextbreakpoint.flink.client.support;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.awaitility.Awaitility;
import org.awaitility.core.ThrowingRunnable;

import java.util.concurrent.TimeUnit;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class AsyncUtils {
    public static void await(ThrowingRunnable throwingRunnable) {
        Awaitility.await()
                .pollInterval(1, TimeUnit.SECONDS)
                .pollDelay(2, TimeUnit.SECONDS)
                .atMost(20, TimeUnit.SECONDS)
                .untilAsserted(throwingRunnable);
    }
}
