package com.nextbreakpoint.flinkclient.fixtures;

import lombok.Getter;
import org.apache.flink.api.connector.source.lib.util.IteratorSourceSplit;

import java.util.List;
import java.util.Objects;

public class SensorMeasureSplit implements IteratorSourceSplit<SensorMeasure, SensorMeasureIterator> {
    private final String splitId;
    @Getter
    private final int maxCount;
    @Getter
    private final List<Sensor> temperatures;

    public SensorMeasureSplit(String splitId, int maxCount, List<Sensor> temperatures) {
        this.splitId = Objects.requireNonNull(splitId);
        this.maxCount = maxCount;
        this.temperatures = Objects.requireNonNull(temperatures);
    }

    @Override
    public SensorMeasureIterator getIterator() {
        return new SensorMeasureIterator(maxCount, temperatures);
    }

    @Override
    public IteratorSourceSplit<SensorMeasure, SensorMeasureIterator> getUpdatedSplitForIterator(SensorMeasureIterator iterator) {
        return new SensorMeasureSplit(splitId, iterator.getMaxCount(), iterator.getTemperatures());
    }

    @Override
    public String splitId() {
        return splitId;
    }
}
