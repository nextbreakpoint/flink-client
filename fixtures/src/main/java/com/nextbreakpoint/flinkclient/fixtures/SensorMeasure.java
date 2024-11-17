package com.nextbreakpoint.flinkclient.fixtures;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.time.Instant;

@Data
@Builder(setterPrefix = "with")
@AllArgsConstructor
@NoArgsConstructor
public class SensorMeasure {
    private @NonNull String measureId;
    private @NonNull String sensorId;
    private double temperature;
    private @NonNull Instant timestamp;
}
