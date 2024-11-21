/*
 * This file is part of FlinkClient
 * https://github.com/nextbreakpoint/flinkclient
 */
package com.nextbreakpoint.flinkclient.main;

import com.nextbreakpoint.flinkclient.api.ApiException;
import com.nextbreakpoint.flinkclient.api.FlinkApi;
import com.nextbreakpoint.flinkclient.model.DashboardConfiguration;
import com.nextbreakpoint.flinkclient.model.JarListInfo;
import com.nextbreakpoint.flinkclient.model.JarRunResponseBody;
import com.nextbreakpoint.flinkclient.model.JarUploadResponseBody;
import com.nextbreakpoint.flinkclient.model.JobDetailsInfo;
import com.nextbreakpoint.flinkclient.model.JobIdsWithStatusOverview;
import com.nextbreakpoint.flinkclient.model.TerminationMode;
import org.slf4j.bridge.SLF4JBridgeHandler;

import java.io.File;
import java.time.Duration;

public class Example {
    static {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
    }

    public static void main(String[] args) throws ApiException {
        if (args.length < 4) {
            return;
        }

        final String basePath = args[0];
        final String jarPath = args[1];
        final String mainClass = args[2];
        final String arguments = args[3];

        // Create the Flink client API
        FlinkApi api = new FlinkApi();

        // Configure host and port of the server
        api.getApiClient().setBasePath(basePath);

        // Configure socket timeouts
        api.getApiClient().setHttpClient(api.getApiClient().getHttpClient().newBuilder()
                .connectTimeout(Duration.ofSeconds(20))
                .writeTimeout(Duration.ofSeconds(30))
                .readTimeout(Duration.ofSeconds(30))
                .build());

        // Optionally enable debugging
        // api.getApiClient().setDebugging(true);

        // Get Flink cluster configuration
        DashboardConfiguration config = api.getDashboardConfiguration();

        // Upload a jar which contain a Flink job
        JarUploadResponseBody result = api.uploadJar(new File(jarPath));

        // Get list of uploaded jars
        JarListInfo jars = api.getJarList();

        // Run an uploaded jar which some arguments
        final String jobId = jars.getFiles().stream().findFirst().orElseThrow().getId();
        JarRunResponseBody response = api.submitJobFromJar(jobId, null, true, null, null, arguments, mainClass, null);

        // Get status of all jobs
        JobIdsWithStatusOverview jobs = api.getJobIdsWithStatusesOverview();

        // Get details of a job
        JobDetailsInfo details = api.getJobDetails(response.getJobid());

        // Terminate a job
        api.cancelJob(response.getJobid(), TerminationMode.CANCEL);
    }
}
