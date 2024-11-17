/*
 * This file is part of FlinkClient
 * https://github.com/nextbreakpoint/flinkclient
 */
package com.nextbreakpoint.flinkclient.support;

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
