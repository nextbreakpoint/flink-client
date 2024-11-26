/*
 * This file is part of Flink Client
 * https://github.com/nextbreakpoint/flink-client
 */
package com.nextbreakpoint.flink.client.support;

import com.nextbreakpoint.flink.client.api.ApiCallback;
import com.nextbreakpoint.flink.client.api.ApiException;
import lombok.Getter;

import java.util.List;
import java.util.Map;

public class TestCallback<T> implements ApiCallback<T> {
    private final boolean logException;
    @Getter
    private volatile ApiException exception;
    @Getter
    private volatile T result;
    @Getter
    private volatile boolean completed;

    public TestCallback() {
        this(true);
    }

    public TestCallback(boolean logException) {
        this.logException = logException;
    }

    @Override
    public void onFailure(ApiException exception, int statusCode, Map<String, List<String>> responseHeaders) {
        if (logException) {
            exception.printStackTrace();
        }
        this.exception = exception;
        completed = true;
    }

    @Override
    public void onSuccess(T result, int statusCode, Map<String, List<String>> responseHeaders) {
        this.result = result;
        completed = true;
    }

    @Override
    public void onUploadProgress(long bytesWritten, long contentLength, boolean done) {
    }

    @Override
    public void onDownloadProgress(long bytesRead, long contentLength, boolean done) {
    }
}
