/*
 * This file is part of FlinkClient
 * https://github.com/nextbreakpoint/flinkclient
 */
package com.nextbreakpoint.flinkclient.support;

import com.nextbreakpoint.flinkclient.api.ApiCallback;
import com.nextbreakpoint.flinkclient.api.ApiException;
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
