/*
 * This file is part of Flink Client
 * https://github.com/nextbreakpoint/flink-client
 */
package com.nextbreakpoint.flink.client.api;

import com.google.gson.Gson;
import com.nextbreakpoint.flink.client.model.OneOfAsynchronousOperationResultOperation;
import com.nextbreakpoint.flink.client.model.OneOfAsynchronousOperationResultOperationTypeAdapter;

public class FlinkApi extends DefaultApi {
    public FlinkApi() {
        final JSON json = getApiClient().getJSON();

        final Gson gson = json.getGson()
                .newBuilder()
                .registerTypeAdapter(OneOfAsynchronousOperationResultOperation.class, new OneOfAsynchronousOperationResultOperationTypeAdapter())
                .create();

        json.setGson(gson);
    }
}
