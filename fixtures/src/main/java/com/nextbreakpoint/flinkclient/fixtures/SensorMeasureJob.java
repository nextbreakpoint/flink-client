package com.nextbreakpoint.flinkclient.fixtures;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;
import java.util.UUID;

import static com.nextbreakpoint.flinkclient.fixtures.Arguments.ENABLE_WEBUI;
import static com.nextbreakpoint.flinkclient.fixtures.Arguments.LOCAL_ENVIRONMENT;
import static com.nextbreakpoint.flinkclient.fixtures.Arguments.MAX_COUNT;
import static com.nextbreakpoint.flinkclient.fixtures.Arguments.PARALLELISM;
import static com.nextbreakpoint.flinkclient.fixtures.Arguments.REST_PORT;
import static com.nextbreakpoint.flinkclient.fixtures.Parameters.getOptionalParam;

@Slf4j
public class SensorMeasureJob {

    public static void main(String[] args) throws Exception {
        final ParameterTool parameters = ParameterTool.fromArgs(args);

        final boolean localEnvironment = Boolean.parseBoolean(getOptionalParam(parameters, LOCAL_ENVIRONMENT, "false"));
        final boolean enabledWebui = Boolean.parseBoolean(getOptionalParam(parameters, ENABLE_WEBUI, "false"));
        final int restPort = Integer.parseInt(getOptionalParam(parameters, REST_PORT, "8081"));
        final int parallelism = Integer.parseInt(getOptionalParam(parameters, PARALLELISM, "0"));
        final int maxCount = Integer.parseInt(getOptionalParam(parameters, MAX_COUNT, "-1"));

        log.info("LOCAL_ENVIRONMENT={}", localEnvironment);
        log.info("ENABLE_WEBUI={}", enabledWebui);
        log.info("REST_PORT={}", restPort);
        log.info("PARALLELISM={}", parallelism);
        log.info("MAX_COUNT={}", maxCount);

        final StreamExecutionEnvironment environment = Environment.getStreamExecutionEnvironment(localEnvironment, enabledWebui, restPort);

        final List<Sensor> temperatures = List.of(
                new Sensor(UUID.randomUUID().toString(), 20.0 + Math.random() * 5.0),
                new Sensor(UUID.randomUUID().toString(), 20.0 + Math.random() * 5.0),
                new Sensor(UUID.randomUUID().toString(), 20.0 + Math.random() * 5.0),
                new Sensor(UUID.randomUUID().toString(), 20.0 + Math.random() * 5.0),
                new Sensor(UUID.randomUUID().toString(), 20.0 + Math.random() * 5.0)
        );

        final SensorMeasureSource source = new SensorMeasureSource(maxCount, temperatures);

        environment.setParallelism(parallelism > 0 ? parallelism : environment.getParallelism());
        environment.getConfig().setAutoWatermarkInterval(100);
        environment.enableCheckpointing(10000);

        environment
                .fromSource(source, WatermarkStrategy.forMonotonousTimestamps(), "sensor")
                .uid("sensor")
                .sinkTo(new SensorMeasureLoggingSink())
                .name("console")
                .uid("console");

        environment.execute("sensor-to-console-job");
    }
}
