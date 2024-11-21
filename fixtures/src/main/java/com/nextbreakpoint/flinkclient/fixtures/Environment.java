package com.nextbreakpoint.flinkclient.fixtures;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Environment {
    private Environment() {}

    public static StreamExecutionEnvironment getStreamExecutionEnvironment(boolean localEnvironment, boolean webui, int port) {
        if (localEnvironment) {
            if (webui) {
                Configuration configuration = new Configuration();
                configuration.setString("rest.port", String.valueOf(port));
                return StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
            } else {
                return StreamExecutionEnvironment.createLocalEnvironment();
            }
        } else {
            return StreamExecutionEnvironment.getExecutionEnvironment();
        }
    }
}
