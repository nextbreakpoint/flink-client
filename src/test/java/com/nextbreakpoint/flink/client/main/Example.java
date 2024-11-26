/*
 * This file is part of Flink Client
 * https://github.com/nextbreakpoint/flink-client
 */
package com.nextbreakpoint.flink.client.main;

import com.nextbreakpoint.flink.client.api.FlinkApi;
import com.nextbreakpoint.flink.client.api.ApiException;
import com.nextbreakpoint.flink.client.model.DashboardConfiguration;
import com.nextbreakpoint.flink.client.model.JarListInfo;
import com.nextbreakpoint.flink.client.model.JarRunResponseBody;
import com.nextbreakpoint.flink.client.model.JarUploadResponseBody;
import com.nextbreakpoint.flink.client.model.JobDetailsInfo;
import com.nextbreakpoint.flink.client.model.JobIdsWithStatusOverview;
import com.nextbreakpoint.flink.client.model.TerminationMode;
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
            System.out.println("Invalid arguments");
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
        System.out.println(config);

        // Upload a jar which contain a Flink job
        JarUploadResponseBody result = api.uploadJar(new File(jarPath));
        System.out.println(result);

        // Get list of uploaded jars
        JarListInfo jars = api.getJarList();
        System.out.println(jars);

        // Run an uploaded jar which some arguments
        final String jobId = jars.getFiles().stream().findFirst().orElseThrow().getId();
        JarRunResponseBody response = api.submitJobFromJar(jobId, null, true, null, null, arguments, mainClass, null);
        System.out.println(response);

        // Get status of all jobs
        JobIdsWithStatusOverview jobs = api.getJobIdsWithStatusesOverview();
        System.out.println(jobs);

        // Get details of a job
        JobDetailsInfo details = api.getJobDetails(response.getJobid());
        System.out.println(details);

        // Terminate a job
        api.cancelJob(response.getJobid(), TerminationMode.CANCEL);
    }
}
