package com.nextbreakpoint.flinkclient.fixtures;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Sensor implements Serializable {
    private @NonNull String sensorId;
    private double temperature;
}
