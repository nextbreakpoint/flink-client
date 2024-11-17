package com.nextbreakpoint.flinkclient.fixtures;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;

@Slf4j
public class Parameters {
    private Parameters() {}

    public static String getRequiredParam(ParameterTool parameters, String paramName) {
        String param = parameters.get(paramName, System.getProperty(paramName, System.getenv(paramName)));
        if (param == null || param.trim().isEmpty()) {
            throw new RuntimeException(paramName + " parameter can not be empty");
        } else {
            return param;
        }
    }

    public static String getOptionalParam(ParameterTool parameters, String paramName) {
        final String s = parameters.get(paramName, System.getProperty(paramName, System.getenv(paramName)));
        if (s == null) {
            log.info("Parameter '{}' is null (not set), using default or whatever...", paramName);
        }
        return s;
    }

    public static String getOptionalParam(ParameterTool parameters, String paramName, String defaultValue) {
        final String s = parameters.get(paramName, System.getProperty(paramName, System.getenv(paramName)));
        if (s == null) {
            log.info("Parameter '{}' is null (not set), using default '{}'", paramName, defaultValue);
        }
        return s != null ? s : defaultValue;
    }
}
