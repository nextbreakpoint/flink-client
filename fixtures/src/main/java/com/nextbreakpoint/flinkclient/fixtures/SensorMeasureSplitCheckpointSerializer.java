package com.nextbreakpoint.flinkclient.fixtures;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class SensorMeasureSplitCheckpointSerializer implements SimpleVersionedSerializer<Collection<SensorMeasureSplit>> {
    private static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(Collection<SensorMeasureSplit> checkpoint) throws IOException {
        final int total = checkpoint.stream().mapToInt(split -> split.getTemperatures().size()).sum();
        final DataOutputSerializer out = new DataOutputSerializer((8 + 4) * total + 4);
        out.writeInt(checkpoint.size());
        for (SensorMeasureSplit split : checkpoint) {
            SensorMeasureSplitSerializer.serializeV1(out, split);
        }
        return out.getCopyOfBuffer();
    }

    @Override
    public Collection<SensorMeasureSplit> deserialize(int version, byte[] bytes) throws IOException {
        if (version != CURRENT_VERSION) {
            throw new IOException("Unrecognized version: " + version);
        }
        final DataInputDeserializer in = new DataInputDeserializer(bytes);
        final int num = in.readInt();
        final ArrayList<SensorMeasureSplit> result = new ArrayList<>(num);
        for (int remaining = num; remaining > 0; remaining--) {
            result.add(SensorMeasureSplitSerializer.deserializeV1(in));
        }
        return result;
    }
}
