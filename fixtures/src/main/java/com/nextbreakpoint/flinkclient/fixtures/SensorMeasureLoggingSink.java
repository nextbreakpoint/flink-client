package com.nextbreakpoint.flinkclient.fixtures;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.SupportsConcurrentExecutionAttempts;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.IOException;

@Slf4j
public class SensorMeasureLoggingSink implements Sink<SensorMeasure>, SupportsConcurrentExecutionAttempts {
    private static final long serialVersionUID = 1L;

    @Override
    public SinkWriter<SensorMeasure> createWriter(InitContext context) throws IOException {
        return new LoggingElementWriter();
    }

    private class LoggingElementWriter implements SinkWriter<SensorMeasure> {
        @Override
        public void write(SensorMeasure element, Context context) throws IOException, InterruptedException {
            log.info(element.toString());
        }

        @Override
        public void flush(boolean endOfInput) throws IOException, InterruptedException {
            // this writer has no pending data.
        }

        @Override
        public void close() throws Exception {
            // do nothing.
        }
    }
}