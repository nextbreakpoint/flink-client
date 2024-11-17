/*
 * This file is part of FlinkClient
 * https://github.com/nextbreakpoint/flinkclient
 */
package com.nextbreakpoint.flinkclient.api;

import com.google.gson.Gson;
import com.nextbreakpoint.flinkclient.model.OneOfAsynchronousOperationResultOperation;
import com.nextbreakpoint.flinkclient.model.OneOfAsynchronousOperationResultOperationTypeAdapter;

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
