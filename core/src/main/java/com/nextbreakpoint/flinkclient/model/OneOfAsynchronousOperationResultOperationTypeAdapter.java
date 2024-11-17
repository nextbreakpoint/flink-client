/*
 * This file is part of FlinkClient
 * https://github.com/nextbreakpoint/flinkclient
 */
package com.nextbreakpoint.flinkclient.model;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;

public class OneOfAsynchronousOperationResultOperationTypeAdapter extends TypeAdapter<OneOfAsynchronousOperationResultOperation> {
    @Override
    public void write(JsonWriter jsonWriter, OneOfAsynchronousOperationResultOperation operation) throws IOException {
        jsonWriter.beginObject();
        if (operation instanceof SavepointInfo) {
            final SavepointInfo savepointInfo = (SavepointInfo) operation;
            if (savepointInfo.getLocation() != null) {
                jsonWriter.name("location");
                jsonWriter.value(savepointInfo.getLocation());
            }
            if (savepointInfo.getFailureCause() != null) {
                jsonWriter.name("failure-cause");
                jsonWriter.beginObject();
                final SerializedThrowable failureCause = savepointInfo.getFailureCause();
                if (failureCause.getSerializedThrowable() != null) {
                    jsonWriter.name("serialized-throwable");
                    jsonWriter.value(failureCause.getSerializedThrowable());
                }
                jsonWriter.endObject();
            }
        } else if (operation instanceof CheckpointInfo) {
            final CheckpointInfo checkpointInfo = (CheckpointInfo) operation;
            if (checkpointInfo.getCheckpointId() != null) {
                jsonWriter.name("checkpointId");
                jsonWriter.value(checkpointInfo.getCheckpointId());
            }
            if (checkpointInfo.getFailureCause() != null) {
                jsonWriter.name("failureCause");
                jsonWriter.beginObject();
                final SerializedThrowable failureCause = checkpointInfo.getFailureCause();
                if (failureCause.getSerializedThrowable() != null) {
                    jsonWriter.name("serialized-throwable");
                    jsonWriter.value(failureCause.getSerializedThrowable());
                }
                jsonWriter.endObject();
            }
        } else if (operation instanceof AsynchronousOperationInfo) {
            final AsynchronousOperationInfo operationInfo = (AsynchronousOperationInfo) operation;
            if (operationInfo.getFailureCause() != null) {
                jsonWriter.name("failure-cause");
                jsonWriter.beginObject();
                final SerializedThrowable failureCause = operationInfo.getFailureCause();
                if (failureCause.getSerializedThrowable() != null) {
                    jsonWriter.name("serialized-throwable");
                    jsonWriter.value(failureCause.getSerializedThrowable());
                }
                jsonWriter.endObject();
            }
        }
        jsonWriter.endObject();
    }

    @Override
    public OneOfAsynchronousOperationResultOperation read(JsonReader jsonReader) throws IOException {
        String location = null;
        Long checkpointId = null;
        SerializedThrowable failureCause = null;
        jsonReader.beginObject();
        while (jsonReader.hasNext()) {
            switch (jsonReader.nextName()) {
                case "location":
                    location = jsonReader.nextString();
                    break;
                case "checkpointId":
                    checkpointId = jsonReader.nextLong();
                    break;
                case "failure-cause":
                case "failureCause":
                    jsonReader.beginObject();
                    failureCause = new SerializedThrowable();
                    while (jsonReader.hasNext()) {
                        if (jsonReader.nextName().equals("serialized-throwable")) {
                            failureCause.setSerializedThrowable(jsonReader.nextString());
                        }
                    }
                    jsonReader.endObject();
                    break;
            }
        }
        jsonReader.endObject();
        if (location != null) {
            final SavepointInfo savepointInfo = new SavepointInfo();
            savepointInfo.setLocation(location);
            savepointInfo.failureCause(failureCause);
            return savepointInfo;
        } else if (checkpointId != null) {
            final CheckpointInfo checkpointInfo = new CheckpointInfo();
            checkpointInfo.setCheckpointId(checkpointId);
            checkpointInfo.failureCause(failureCause);
            return checkpointInfo;
        } else {
            final AsynchronousOperationInfo operationInfo = new AsynchronousOperationInfo();
            operationInfo.failureCause(failureCause);
            return operationInfo;
        }
    }
}
