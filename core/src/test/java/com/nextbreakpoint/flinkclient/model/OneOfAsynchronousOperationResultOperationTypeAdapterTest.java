/*
 * This file is part of FlinkClient
 * https://github.com/nextbreakpoint/flinkclient
 */
package com.nextbreakpoint.flinkclient.model;

import com.google.gson.Gson;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OneOfAsynchronousOperationResultOperationTypeAdapterTest {
    private static final String SAVEPOINT_INFO_JSON = "{\"failure-cause\":{\"serialized-throwable\":\"error\"},\"location\":\"/tmp/test\"}";
    private static final String CHECKPOINT_INFO_JSON = "{\"checkpointId\":1000,\"failureCause\":{\"serialized-throwable\":\"error\"}}";
    private static final String OPERATION_INFO_JSON = "{\"failure-cause\":{\"serialized-throwable\":\"error\"}}";

    @Test
    void shouldSerializeSavepointInfo() {
        final Gson gson = createGson();
        final SavepointInfo savepointInfo = new SavepointInfo();
        savepointInfo.setLocation("/tmp/test");
        final SerializedThrowable failureCause = new SerializedThrowable();
        failureCause.setSerializedThrowable("error");
        savepointInfo.setFailureCause(failureCause);
        final String actualJson = gson.toJson(savepointInfo);
        assertThat(actualJson).isEqualTo(SAVEPOINT_INFO_JSON);
    }

    @Test
    void shouldSerializeCheckpointInfo() {
        final Gson gson = createGson();
        final CheckpointInfo checkpointInfo = new CheckpointInfo();
        checkpointInfo.setCheckpointId(1000L);
        final SerializedThrowable failureCause = new SerializedThrowable();
        failureCause.setSerializedThrowable("error");
        checkpointInfo.setFailureCause(failureCause);
        final String actualJson = gson.toJson(checkpointInfo);
        assertThat(actualJson).isEqualTo(CHECKPOINT_INFO_JSON);
    }

    @Test
    void shouldSerializeAsynchronousOperationInfo() {
        final Gson gson = createGson();
        final AsynchronousOperationInfo operationInfo = new AsynchronousOperationInfo();
        final SerializedThrowable failureCause = new SerializedThrowable();
        failureCause.setSerializedThrowable("error");
        operationInfo.setFailureCause(failureCause);
        final String actualJson = gson.toJson(operationInfo);
        assertThat(actualJson).isEqualTo(OPERATION_INFO_JSON);
    }

    @Test
    void shouldDeserializeSavepointInfo() {
        final Gson gson = createGson();
        final SavepointInfo savepointInfo = new SavepointInfo();
        savepointInfo.setLocation("/tmp/test");
        final SerializedThrowable failureCause = new SerializedThrowable();
        failureCause.setSerializedThrowable("error");
        savepointInfo.setFailureCause(failureCause);
        final SavepointInfo actualSavepointInfo = gson.fromJson(SAVEPOINT_INFO_JSON, SavepointInfo.class);
        assertThat(actualSavepointInfo).isEqualTo(savepointInfo);
    }

    @Test
    void shouldDeserializeCheckpointInfo() {
        final Gson gson = createGson();
        final CheckpointInfo expectedCheckpointInfo = new CheckpointInfo();
        expectedCheckpointInfo.setCheckpointId(1000L);
        final SerializedThrowable failureCause = new SerializedThrowable();
        failureCause.setSerializedThrowable("error");
        expectedCheckpointInfo.setFailureCause(failureCause);
        final CheckpointInfo actualCheckpointInfo = gson.fromJson(CHECKPOINT_INFO_JSON, CheckpointInfo.class);
        assertThat(actualCheckpointInfo).isEqualTo(expectedCheckpointInfo);
    }

    @Test
    void shouldDeserializeAsynchronousOperationInfo() {
        final Gson gson = createGson();
        final AsynchronousOperationInfo expectedOperationInfo = new AsynchronousOperationInfo();
        final SerializedThrowable failureCause = new SerializedThrowable();
        failureCause.setSerializedThrowable("error");
        expectedOperationInfo.setFailureCause(failureCause);
        final AsynchronousOperationInfo actualOperationInfo = gson.fromJson(OPERATION_INFO_JSON, AsynchronousOperationInfo.class);
        assertThat(actualOperationInfo).isEqualTo(expectedOperationInfo);
    }

    @Test
    void shouldDeserializeSavepointInfoUsingAdapter() {
        final Gson gson = createGson();
        final SavepointInfo savepointInfo = new SavepointInfo();
        savepointInfo.setLocation("/tmp/test");
        final SerializedThrowable failureCause = new SerializedThrowable();
        failureCause.setSerializedThrowable("error");
        savepointInfo.setFailureCause(failureCause);
        final OneOfAsynchronousOperationResultOperation actualSavepointInfo = gson.fromJson(SAVEPOINT_INFO_JSON, OneOfAsynchronousOperationResultOperation.class);
        assertThat(actualSavepointInfo).isEqualTo(savepointInfo);
    }

    @Test
    void shouldDeserializeCheckpointInfoUsingAdapter() {
        final Gson gson = createGson();
        final CheckpointInfo expectedCheckpointInfo = new CheckpointInfo();
        expectedCheckpointInfo.setCheckpointId(1000L);
        final SerializedThrowable failureCause = new SerializedThrowable();
        failureCause.setSerializedThrowable("error");
        expectedCheckpointInfo.setFailureCause(failureCause);
        final OneOfAsynchronousOperationResultOperation actualCheckpointInfo = gson.fromJson(CHECKPOINT_INFO_JSON, OneOfAsynchronousOperationResultOperation.class);
        assertThat(actualCheckpointInfo).isEqualTo(expectedCheckpointInfo);
    }

    @Test
    void shouldDeserializeAsynchronousOperationInfoUsingAdapter() {
        final Gson gson = createGson();
        final AsynchronousOperationInfo expectedOperationInfo = new AsynchronousOperationInfo();
        final SerializedThrowable failureCause = new SerializedThrowable();
        failureCause.setSerializedThrowable("error");
        expectedOperationInfo.setFailureCause(failureCause);
        final OneOfAsynchronousOperationResultOperation actualOperationInfo = gson.fromJson(OPERATION_INFO_JSON, OneOfAsynchronousOperationResultOperation.class);
        assertThat(actualOperationInfo).isEqualTo(expectedOperationInfo);
    }

    private static Gson createGson() {
        return new Gson()
                .newBuilder()
                .registerTypeAdapter(OneOfAsynchronousOperationResultOperation.class, new OneOfAsynchronousOperationResultOperationTypeAdapter())
                .create();
    }
}