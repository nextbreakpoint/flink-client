package com.nextbreakpoint.flinkclient.fixtures;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.util.ArrayList;

public class SensorMeasureSplitSerializer implements SimpleVersionedSerializer<SensorMeasureSplit> {
    private static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(SensorMeasureSplit split) throws IOException {
        final DataOutputSerializer out = new DataOutputSerializer(8 + 4);
        serializeV1(out, split);
        return out.getCopyOfBuffer();
    }

    @Override
    public SensorMeasureSplit deserialize(int version, byte[] bytes) throws IOException {
        if (version != CURRENT_VERSION) {
            throw new IOException("Unrecognized version: " + version);
        }
        final DataInputDeserializer in = new DataInputDeserializer(bytes);
        return deserializeV1(in);
    }

    public static void serializeV1(DataOutputSerializer out, SensorMeasureSplit split) throws IOException {
        out.writeUTF(split.splitId());
        out.writeInt(split.getMaxCount());
        out.writeInt(split.getTemperatures().size());
        for (Sensor temperature : split.getTemperatures()) {
            out.writeUTF(temperature.getSensorId());
            out.writeDouble(temperature.getTemperature());
        }
    }

    public static SensorMeasureSplit deserializeV1(DataInputDeserializer in) throws IOException {
        final String splitId = in.readUTF();
        final int maxCount = in.readInt();
        final int num = in.readInt();
        final ArrayList<Sensor> temperatures = new ArrayList<>(num);
        for (int remaining = num; remaining > 0; remaining--) {
            final String sensorId = in.readUTF();
            final double temperature = in.readDouble();
            temperatures.add(new Sensor(sensorId, temperature));
        }
        return new SensorMeasureSplit(splitId, maxCount, temperatures);
    }
}
