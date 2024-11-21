package com.nextbreakpoint.flinkclient.fixtures;

import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceEnumerator;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceReader;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.Collection;
import java.util.List;
import java.util.Objects;

/*
 * https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/sources/
 */
public class SensorMeasureSource implements Source<SensorMeasure, SensorMeasureSplit, Collection<SensorMeasureSplit>> {
    private final int maxCount;
    private final List<Sensor> temperatures;

    public SensorMeasureSource(int maxCount, List<Sensor> temperatures) {
        this.maxCount = maxCount;
        this.temperatures = Objects.requireNonNull(temperatures);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<SensorMeasureSplit, Collection<SensorMeasureSplit>> createEnumerator(SplitEnumeratorContext<SensorMeasureSplit> splitEnumeratorContext) throws Exception {
        final List<SensorMeasureSplit> splits = List.of(new SensorMeasureSplit("1", maxCount, temperatures));
        return new IteratorSourceEnumerator<>(splitEnumeratorContext, splits);
    }

    @Override
    public SplitEnumerator<SensorMeasureSplit, Collection<SensorMeasureSplit>> restoreEnumerator(SplitEnumeratorContext<SensorMeasureSplit> splitEnumeratorContext, Collection<SensorMeasureSplit> checkpoint) throws Exception {
        return new IteratorSourceEnumerator<>(splitEnumeratorContext, checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<SensorMeasureSplit> getSplitSerializer() {
        return new SensorMeasureSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Collection<SensorMeasureSplit>> getEnumeratorCheckpointSerializer() {
        return new SensorMeasureSplitCheckpointSerializer();
    }

    @Override
    public SourceReader<SensorMeasure, SensorMeasureSplit> createReader(SourceReaderContext sourceReaderContext) throws Exception {
        return new IteratorSourceReader<>(sourceReaderContext);
    }
}
