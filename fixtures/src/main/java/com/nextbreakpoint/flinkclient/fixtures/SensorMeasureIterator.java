package com.nextbreakpoint.flinkclient.fixtures;

import lombok.Getter;
import org.apache.flink.util.SplittableIterator;

import java.time.Clock;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class SensorMeasureIterator extends SplittableIterator<SensorMeasure> {
    private final AtomicInteger count = new AtomicInteger(0);
    @Getter
    private final int maxCount;
    @Getter
    private final List<Sensor> temperatures;

    public SensorMeasureIterator(int maxCount, List<Sensor> temperatures) {
        this.maxCount = maxCount;
        this.temperatures = Objects.requireNonNull(temperatures);
    }

    @Override
    public Iterator<SensorMeasure>[] split(int numPartitions) {
        return new Iterator[] { this };
    }

    @Override
    public int getMaximumNumberOfSplits() {
        return 1;
    }

    @Override
    public boolean hasNext() {
        return maxCount <= 0 || count.get() < maxCount;
    }

    @Override
    public SensorMeasure next() {
        return createSensorTemperatureMeasure(count.addAndGet(1), Instant.now(Clock.systemUTC()));
    }

    private SensorMeasure createSensorTemperatureMeasure(int index, Instant timestamp) {
        final Sensor temperature = temperatures.get(index % temperatures.size());

        final double value = nextTemperatureValue(temperature.getTemperature(), timestamp.toEpochMilli());

        return SensorMeasure.builder()
                .withMeasureId(UUID.randomUUID().toString())
                .withSensorId(temperature.getSensorId())
                .withTemperature(value)
                .withTimestamp(timestamp)
                .build();
    }

    private double nextTemperatureValue(double amplitude, long timestamp) {
        return Math.sin(2 * Math.PI * (timestamp / 1000.0)) * amplitude / 10 + (Math.random() * amplitude / 20.0) + amplitude;
    }
}
