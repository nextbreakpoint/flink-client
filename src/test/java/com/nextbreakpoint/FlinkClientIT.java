package com.nextbreakpoint;

import com.google.gson.Gson;
import com.nextbreakpoint.flinkclient.api.ApiCallback;
import com.nextbreakpoint.flinkclient.api.ApiException;
import com.nextbreakpoint.flinkclient.api.FlinkApi;
import com.nextbreakpoint.flinkclient.model.*;
import org.awaitility.Awaitility;
import org.awaitility.core.ThrowingRunnable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(JUnitPlatform.class)
@Tag("slow")
public class FlinkClientIT {
    private static final String JAR_NAME = "com.nextbreakpoint.flinkworkshop-1.0.1.jar";
    private static final String JAR_PATH = "/Users/andrea/Documents/projects/opensource/flink-workshop/flink/com.nextbreakpoint.flinkworkshop/target/" + JAR_NAME;
    private static final String ZIP_PATH = "/Users/andrea/Documents/projects/opensource/flink-workshop/flink/com.nextbreakpoint.flinkworkshop/target/com.nextbreakpoint.flinkworkshop-1.0.1.zip";

    private FlinkApi api;

    private void dumpAsJson(Object object) {
        System.out.println(new Gson().toJson(object));
    }

    private void await(ThrowingRunnable throwingRunnable) {
        Awaitility.await()
                .pollInterval(1, TimeUnit.SECONDS)
                .pollDelay(1, TimeUnit.SECONDS)
                .atMost(5, TimeUnit.SECONDS)
                .untilAsserted(throwingRunnable);
    }

    private class TestCallback<T> implements ApiCallback<T> {
        volatile ApiException e;
        volatile T result;

        @Override
        public void onFailure(ApiException e, int statusCode, Map<String, List<String>> responseHeaders) {
            e.printStackTrace();
            this.e = e;
        }

        @Override
        public void onSuccess(T result, int statusCode, Map<String, List<String>> responseHeaders) {
            dumpAsJson(result);
            this.result = result;
        }

        @Override
        public void onUploadProgress(long bytesWritten, long contentLength, boolean done) {
        }

        @Override
        public void onDownloadProgress(long bytesRead, long contentLength, boolean done) {
        }
    }

    private void verifyDashboardConfiguration(DashboardConfiguration config) {
        assertThat(config.getRefreshInterval()).isEqualTo(3000);
        assertThat(config.getTimezoneName()).isNotBlank();
        assertThat(config.getTimezoneOffset()).isNotNull();
        assertThat(config.getFlinkVersion()).isEqualTo("1.7.2");
        assertThat(config.getFlinkRevision()).isEqualTo("ceba8af @ 11.02.2019 @ 14:17:09 UTC");
    }

    private void verifyJarUploadResponseBody(JarUploadResponseBody result) {
        assertThat(result.getStatus()).isEqualTo(JarUploadResponseBody.StatusEnum.SUCCESS);
        assertThat(result.getFilename()).endsWith("_" + JAR_NAME);
        assertThat(result.getFilename()).contains("/flink-web-upload");
    }

    private void verifyJarListInfo(JarUploadResponseBody result, JarListInfo jarListInfo) {
        assertThat(jarListInfo.getFiles().size() > 0).isTrue();
        IntStream.range(0, jarListInfo.getFiles().size()).forEach((index) -> {
            assertThat(jarListInfo.getFiles().get(index).getId()).endsWith("_" + JAR_NAME);
            assertThat(jarListInfo.getFiles().get(index).getName()).isEqualTo(JAR_NAME);
        });
        assertThat(jarListInfo.getFiles().stream().anyMatch(file -> result.getFilename().endsWith("/" + file.getId()))).isTrue();
    }

    private void verifyJarDeleted(JarListInfo jarListInfo, String fileInfoId) {
        assertThat(jarListInfo.getFiles().stream().filter(fileInfo -> fileInfo.getId().equals(fileInfoId)).count()).isEqualTo(0);
    }

    private void runTestJar() throws ApiException {
        File jarFile = new File(JAR_PATH);
        JarUploadResponseBody result = api.uploadJar(jarFile);
        assertThat(result).isNotNull();
        dumpAsJson(result);
        JarListInfo jarListInfo = api.listJars();
        assertThat(jarListInfo).isNotNull();
        dumpAsJson(jarListInfo);
        JarRunResponseBody response = api.runJar(jarListInfo.getFiles().get(0).getId(), true, null, "--BUCKET_BASE_PATH file:///var/tmp", null, "com.nextbreakpoint.flink.jobs.TestJob", null);
        assertThat(response).isNotNull();
        dumpAsJson(response);
    }

    private void terminateAllJobs() throws ApiException {
        JobIdsWithStatusOverview statusOverview = api.getJobs();
        assertThat(statusOverview).isNotNull();
        dumpAsJson(statusOverview);
        statusOverview.getJobs().forEach(jobIdWithStatus -> {
            try {
                api.terminateJob(jobIdWithStatus.getId(), "cancel");
            } catch (ApiException ignored) {
            }
        });
    }

    @BeforeEach
    void setup() {
        api = new FlinkApi();
        api.getApiClient().setBasePath("http://localhost:8081");
    }

    @Nested
    class Config {
        @Test
        void shouldShowConfig() throws ApiException {
            // when
            DashboardConfiguration config = api.showConfig();

            // then
            dumpAsJson(config);
            verifyDashboardConfiguration(config);
        }
    }

    @Nested
    class ConfigAsync {
        @Test
        void shouldShowConfig() throws ApiException {
            // given
            TestCallback<DashboardConfiguration> callback = new TestCallback<>();

            // when
            api.showConfigAsync(callback);

            // then
            await(() -> {
                assertThat(callback.result).isNotNull();
                verifyDashboardConfiguration(callback.result);
            });
        }
    }

    @Nested
    class Shutdown {
        @Test
        void shouldShutdownCluster() throws ApiException {
//            api.shutdownCluster();
        }
    }

    @Nested
    class ShutdownAsync {
        @Test
        void shouldShutdownCluster() {
//            TestCallback<Void> callback = new TestCallback<>();
//
//            api.shutdownClusterAsync(callback);
        }
    }

    @Nested
    class Jars {
        @Test
        void shouldUploadJar() throws ApiException {
            // when
            JarUploadResponseBody result = api.uploadJar(new File(JAR_PATH));

            // then
            assertThat(result).isNotNull();
            dumpAsJson(result);
            verifyJarUploadResponseBody(result);
        }

        @Test
        void shouldThrowWhenUploadingNonExistentFile() {
            assertThatThrownBy(() -> api.uploadJar(new File(ZIP_PATH))).isInstanceOf(ApiException.class);
        }

        @Test
        void shouldListJar() throws ApiException {
            // given
            JarUploadResponseBody result = api.uploadJar(new File(JAR_PATH));
            dumpAsJson(result);

            // when
            JarListInfo jarListInfo = api.listJars();

            // then
            assertThat(jarListInfo).isNotNull();
            dumpAsJson(jarListInfo);
            verifyJarListInfo(result, jarListInfo);
        }

        @Test
        void shouldDeleteJar() throws ApiException {
            // given
            JarUploadResponseBody result = api.uploadJar(new File(JAR_PATH));
            dumpAsJson(result);
            JarFileInfo jarFileInfo = api.listJars().getFiles().get(0);

            // when
            api.deleteJar(jarFileInfo.getId());

            // then
            JarListInfo jarListInfo = api.listJars();
            dumpAsJson(jarListInfo);
            verifyJarDeleted(jarListInfo, jarFileInfo.getId());
        }
    }

    @Nested
    class JarsAsync {
        @Test
        void shouldUploadJar() throws ApiException {
            // given
            TestCallback<JarUploadResponseBody> callback = new TestCallback<>();

            // when
            api.uploadJarAsync(new File(JAR_PATH), callback);

            // then
            await(() -> {
                assertThat(callback.result).isNotNull();
                verifyJarUploadResponseBody(callback.result);
            });
        }

        @Test
        void shouldThrowWhenUploadingNonExistentFile() throws ApiException {
            // given
            TestCallback<JarUploadResponseBody> callback = new TestCallback<>();

            // when
            api.uploadJarAsync(new File(ZIP_PATH), callback);

            // then
            await(() -> assertThat(callback.e).isNotNull());
        }

        @Test
        void shouldListJar() throws ApiException {
            // given
            TestCallback<JarListInfo> callback = new TestCallback<>();

            JarUploadResponseBody result = api.uploadJar(new File(JAR_PATH));
            dumpAsJson(result);

            // when
            api.listJarsAsync(callback);

            // then
            await(() -> {
                assertThat(callback.result).isNotNull();
                verifyJarListInfo(result, callback.result);
            });
        }

        @Test
        void shouldDeleteJar() throws ApiException {
            // given
            TestCallback<Void> callback = new TestCallback<>();

            JarUploadResponseBody result = api.uploadJar(new File(JAR_PATH));
            dumpAsJson(result);
            JarFileInfo jarFileInfo = api.listJars().getFiles().get(0);

            // when
            api.deleteJarAsync(jarFileInfo.getId(), callback);

            // then
            await(() -> {
                JarListInfo jarListInfo = api.listJars();
                dumpAsJson(jarListInfo);
                verifyJarDeleted(jarListInfo, jarFileInfo.getId());
            });
        }
    }

    @Nested
    class Jobs {
        @BeforeEach
        void terminateJobs() throws ApiException {
            terminateAllJobs();
        }

        @BeforeEach
        void runJob() throws ApiException {
            runTestJar();
        }

        @Test
        void shouldManageJobs() throws ApiException {
            // when
            JobIdsWithStatusOverview statusOverview = api.getJobs();

            // then
            assertThat(statusOverview).isNotNull();
            dumpAsJson(statusOverview);

            final List<JobIdWithStatus> jobs = statusOverview.getJobs()
                    .stream()
                    .filter(jobIdWithStatus -> jobIdWithStatus.getStatus().getValue().equals("RUNNING"))
                    .collect(Collectors.toList());
            assertThat(jobs).hasSize(1);
            final String jobId = jobs.get(0).getId();

            // and when
            MultipleJobsDetails multipleJobsDetails = api.getJobsOverview();

            // then
            assertThat(multipleJobsDetails).isNotNull();
            dumpAsJson(multipleJobsDetails);
            assertThat(multipleJobsDetails.getJobs().size() > 0).isTrue();

            // and when
            Object jobsMetrics = api.getJobsMetrics("numberOfFailedCheckpoints", "sum", null);

            // then
            assertThat(jobsMetrics).isNotNull();
            dumpAsJson(jobsMetrics);

            // and when
            String jobConfig = api.getJobConfig(jobId);

            // then
            assertThat(jobConfig).isNotNull();
            System.out.println(jobConfig);

            // and when
            JobPlanInfo jobPlan = api.getJobPlan(jobId);

            // then
            assertThat(jobPlan).isNotNull();
            dumpAsJson(jobPlan);
            assertThat(jobPlan.getPlan()).isNotNull();

            // and when
            JobDetailsInfo jobDetails = api.getJobDetails(jobId);

            // then
            assertThat(jobDetails).isNotNull();
            dumpAsJson(jobDetails);
            assertThat(jobDetails.getPlan()).isNotNull();
            assertThat(jobDetails.getJid()).isEqualTo(jobId);
            assertThat(jobDetails.getVertices()).hasSize(1);

            // and when
            CheckpointingStatistics checkpointingStatistics = api.getJobCheckpoints(jobId);

            // then
            assertThat(checkpointingStatistics).isNotNull();
            dumpAsJson(checkpointingStatistics);
            assertThat(checkpointingStatistics.getCounts()).isNotNull();

            // and when
            JobAccumulatorsInfo jobAccumulatorsInfo = api.getJobAccumulators(jobId, true);

            // then
            assertThat(jobAccumulatorsInfo).isNotNull();
            dumpAsJson(jobAccumulatorsInfo);
            assertThat(jobAccumulatorsInfo.getJobAccumulators()).isNotNull();

            // and when
            JobExceptionsInfo exceptionInjobExceptionsInfoo = api.getJobExceptions(jobId);

            // then
            assertThat(exceptionInjobExceptionsInfoo).isNotNull();
            dumpAsJson(exceptionInjobExceptionsInfoo);
            assertThat(exceptionInjobExceptionsInfoo.getAllExceptions()).isNotNull();

            // and when
            Object jobMetrics = api.getJobMetrics(jobId, "numberOfFailedCheckpoints");

            // then
            assertThat(jobMetrics).isNotNull();
            dumpAsJson(jobMetrics);

            // and when
            JobExecutionResultResponseBody jobResult = api.getJobResult(jobId);

            // then
            assertThat(jobResult).isNotNull();
            dumpAsJson(jobResult);
            assertThat(jobResult.getStatus().getId()).isEqualTo(QueueStatus.IdEnum.IN_PROGRESS);
            assertThat(jobResult.getJobExecutionResult()).isNull();

            // and when
            CheckpointConfigInfo checkpointConfigInfo = api.getJobCheckpointsConfig(jobId);

            // then
            assertThat(checkpointConfigInfo).isNotNull();
            dumpAsJson(checkpointConfigInfo);
            assertThat(checkpointConfigInfo.getInterval()).isEqualTo(60000L);

            // and when
            JobVertexAccumulatorsInfo jobTaskAllAccumulatorsInfo = api.getJobTaskAccumulators(jobId, jobDetails.getVertices().get(0).getId().toString());

            // then
            assertThat(jobTaskAllAccumulatorsInfo).isNotNull();
            dumpAsJson(jobTaskAllAccumulatorsInfo);
            assertThat(jobTaskAllAccumulatorsInfo.getUserAccumulators()).isNotNull();

            // and when
            JobVertexBackPressureInfo jobTaskBackpressure = api.getJobTaskBackpressure(jobId, jobDetails.getVertices().get(0).getId().toString());

            // then
            assertThat(jobTaskBackpressure).isNotNull();
            dumpAsJson(jobTaskBackpressure);
            assertThat(jobTaskBackpressure.getStatus()).isEqualTo(JobVertexBackPressureInfo.StatusEnum.DEPRECATED);

            // and when
            JobVertexDetailsInfo jobTaskDetails = api.getJobTaskDetails(jobId, jobDetails.getVertices().get(0).getId().toString());

            // then
            assertThat(jobTaskDetails).isNotNull();
            dumpAsJson(jobTaskDetails);
            assertThat(jobTaskDetails.getSubtasks()).hasSize(1);

            // and when
            JobVertexTaskManagersInfo jobTaskManagerDetails = api.getJobTaskDetailsByTaskManager(jobId, jobDetails.getVertices().get(0).getId().toString());

            // then
            assertThat(jobTaskManagerDetails).isNotNull();
            dumpAsJson(jobTaskManagerDetails);
            assertThat(jobTaskManagerDetails.getTaskmanagers()).hasSize(1);

            // and when
            Object jobTaskMetrics = api.getJobTaskMetrics(jobId, jobDetails.getVertices().get(0).getId().toString(), "");/*TODO*/

            // then
            assertThat(jobTaskMetrics).isNotNull();
            dumpAsJson(jobTaskMetrics);

            // and when
            SubtasksAllAccumulatorsInfo jobSubtasksAllAccumulatorsInfo = api.getJobSubtaskAccumulators(jobId, jobDetails.getVertices().get(0).getId().toString());

            // then
            assertThat(jobSubtasksAllAccumulatorsInfo).isNotNull();
            dumpAsJson(jobSubtasksAllAccumulatorsInfo);
            assertThat(jobSubtasksAllAccumulatorsInfo.getSubtasks()).hasSize(1);

            Awaitility.await()
                    .pollDelay(20, TimeUnit.SECONDS)
                    .atMost(700, TimeUnit.SECONDS)
                    .pollInterval(20, TimeUnit.SECONDS)
                    .untilAsserted(() -> {
                        // and when
                        SubtaskExecutionAttemptDetailsInfo subtaskExecutionDetailsInfo = api.getJobSubtaskDetails(jobId, jobDetails.getVertices().get(0).getId().toString(), 0);

                        // then
                        assertThat(subtaskExecutionDetailsInfo).isNotNull();
                        dumpAsJson(subtaskExecutionDetailsInfo);
                        assertThat(subtaskExecutionDetailsInfo.getStatus()).isEqualTo(SubtaskExecutionAttemptDetailsInfo.StatusEnum.RUNNING);
                    });

//            // and when
//            SubtaskExecutionAttemptAccumulatorsInfo subtaskExecutionAttemptAccumulatorsInfo = api.getJobSubtaskAttemptAccumulators(jobId, jobDetails.getVertices().get(0).getId().toString(), 0, 0);
//
//            // then
//            assertThat(subtaskExecutionAttemptAccumulatorsInfo).isNotNull();
//            dumpAsJson(subtaskExecutionAttemptAccumulatorsInfo);
//            assertThat(subtaskExecutionAttemptAccumulatorsInfo.getUserAccumulators()).isNotNull();

//            // and when
//            SubtaskExecutionAttemptDetailsInfo subtaskExecutionAttemptDetailsInfo = api.getJobSubtaskAttemptDetails(jobId, jobDetails.getVertices().get(0).getId().toString(), 0, 0);
//
//            // then
//            assertThat(subtaskExecutionAttemptDetailsInfo).isNotNull();
//            dumpAsJson(subtaskExecutionAttemptDetailsInfo);
//            assertThat(subtaskExecutionAttemptDetailsInfo.getStatus()).isEqualTo(SubtaskExecutionAttemptDetailsInfo.StatusEnum.RUNNING);

            // and when
            Object subtaskMetrics = api.getJobSubtaskMetrics(jobId, jobDetails.getVertices().get(0).getId().toString(), 0, "");/*TODO*/

            // then
            assertThat(subtaskMetrics).isNotNull();
            dumpAsJson(subtaskMetrics);

            // and when
            SubtasksTimesInfo subtasksTimesInfo = api.getJobSubtaskTimes(jobId, jobDetails.getVertices().get(0).getId().toString());

            // then
            assertThat(subtasksTimesInfo).isNotNull();
            dumpAsJson(subtasksTimesInfo);
            assertThat(subtasksTimesInfo.getSubtasks()).hasSize(1);

            // and when
            Object subtasksAggregatedMetrics = api.getJobAggregatedSubtaskMetrics(jobId, jobDetails.getVertices().get(0).getId().toString(), "", "sum", null);/*TODO*/

            // then
            assertThat(subtasksAggregatedMetrics).isNotNull();
            dumpAsJson(subtasksAggregatedMetrics);

            // and when
            TriggerResponse triggerSavepointResponse = api.createJobSavepoint(new SavepointTriggerRequestBody().cancelJob(false).targetDirectory("file:///var/tmp"), jobId);

            // then
            assertThat(triggerSavepointResponse).isNotNull();
            dumpAsJson(triggerSavepointResponse);
            assertThat(triggerSavepointResponse.getRequestId()).isNotNull();

            Awaitility.await()
                    .pollDelay(20, TimeUnit.SECONDS)
                    .atMost(700, TimeUnit.SECONDS)
                    .pollInterval(20, TimeUnit.SECONDS)
                    .untilAsserted(() -> {
                        // and when
                        AsynchronousOperationResult asynchronousOperationResult = api.getJobSavepointStatus(jobId, triggerSavepointResponse.getRequestId().toString());

                        // then
                        assertThat(asynchronousOperationResult).isNotNull();
                        dumpAsJson(asynchronousOperationResult);
                        assertThat(asynchronousOperationResult.getStatus().getId()).isEqualTo(QueueStatus.IdEnum.COMPLETED);
                    });

            // and when
            TriggerResponse triggerRescalingResponse = api.triggerJobRescaling(jobId, 2);

            // then
            assertThat(triggerRescalingResponse).isNotNull();
            dumpAsJson(triggerRescalingResponse);
            assertThat(triggerRescalingResponse.getRequestId()).isNotNull();

            Awaitility.await()
                    .pollDelay(20, TimeUnit.SECONDS)
                    .atMost(700, TimeUnit.SECONDS)
                    .pollInterval(20, TimeUnit.SECONDS)
                    .untilAsserted(() -> {
                        // and when
                        AsynchronousOperationResult asynchronousOperationResult = api.getJobRescalingStatus(jobId, triggerRescalingResponse.getRequestId().toString());

                        // then
                        assertThat(asynchronousOperationResult).isNotNull();
                        dumpAsJson(asynchronousOperationResult);
                        assertThat(asynchronousOperationResult.getStatus().getId()).isEqualTo(QueueStatus.IdEnum.COMPLETED);
                    });

            Awaitility.await()
                    .pollDelay(20, TimeUnit.SECONDS)
                    .atMost(700, TimeUnit.SECONDS)
                    .pollInterval(20, TimeUnit.SECONDS)
                    .untilAsserted(() -> {
                        // and when
                        CheckpointingStatistics jobCheckpoints = api.getJobCheckpoints(jobId);

                        // then
                        assertThat(jobCheckpoints).isNotNull();
                        dumpAsJson(jobCheckpoints);
                        assertThat(jobCheckpoints.getHistory()).isNotNull();
                        assertThat(jobCheckpoints.getHistory().size() > 0).isTrue();

                        // and when
                        Integer checkpointId = jobCheckpoints.getHistory().get(0).getId();
                        CheckpointStatistics jobCheckpointDetails = api.getJobCheckpointDetails(jobId, checkpointId);

                        // then
                        assertThat(jobCheckpointDetails).isNotNull();
                        dumpAsJson(jobCheckpointDetails);
                        assertThat(jobCheckpointDetails.getStatus().getValue()).isEqualTo(CheckpointStatistics.StatusEnum.COMPLETED.getValue());

                        // and when
                        TaskCheckpointStatisticsWithSubtaskDetails taskCheckpointStatistics = api.getJobCheckpointStatistics(jobId, checkpointId, jobDetails.getVertices().get(0).getId().toString());

                        // then
                        assertThat(taskCheckpointStatistics).isNotNull();
                        dumpAsJson(taskCheckpointStatistics);
                        assertThat(taskCheckpointStatistics.getStatus().getValue()).isEqualTo(CheckpointStatistics.StatusEnum.COMPLETED.getValue());
                    });
        }
    }

    @Nested
    class JobsAync {
        @BeforeEach
        void terminateJobs() throws ApiException {
            terminateAllJobs();
        }

        @BeforeEach
        void runJob() throws ApiException {
            runTestJar();
        }

        @Test
        void shouldRunJob() {
        }
    }

    @Nested
    class JobManager {
        @Test
        void shouldReturnMetrics() throws ApiException {
            // when
            Object metrics = api.getJobManagerMetrics("Status.JVM.CPU.Time");

            // then
            assertThat(metrics).isNotNull();
            dumpAsJson(metrics);
        }
    }

    @Nested
    class JobManagerAsync {
        @Test
        void shouldReturnMetrics() {
        }
    }

    @Nested
    class TaskManager {
        @Test
        void shouldReturnMetrics() throws ApiException {
            // when
            TaskManagersInfo taskManagerInfo = api.getTaskManagersOverview();

            // then
            assertThat(taskManagerInfo).isNotNull();
            dumpAsJson(taskManagerInfo);
            assertThat(taskManagerInfo.getTaskmanagers()).isNotNull();
            assertThat(taskManagerInfo.getTaskmanagers()).hasSize(1);
            final String taskManagerId = taskManagerInfo.getTaskmanagers().get(0).getId().toString();

            // and when
            TaskManagerDetailsInfo taskManagerDetails = api.getTaskManagerDetails(taskManagerId);

            // then
            assertThat(taskManagerDetails).isNotNull();
            dumpAsJson(taskManagerDetails);
            assertThat(taskManagerDetails.getMetrics()).isNotNull();
            assertThat(taskManagerDetails.getMetrics().getHeapMax()).isGreaterThan(0);

            // and when
            Object metrics = api.getTaskManagerMetrics(taskManagerId, "Status.JVM.CPU.Time");

            // then
            assertThat(metrics).isNotNull();
            dumpAsJson(metrics);

            // and when
            Object aggregatedMetrics = api.getTaskManagerAggregatedMetrics("Status.JVM.CPU.Time", "sum", taskManagerId);

            // then
            assertThat(aggregatedMetrics).isNotNull();
            dumpAsJson(aggregatedMetrics);
        }
    }

    @Nested
    class TaskManagerAsync {
        @Test
        void shouldReturnMetrics() {
        }
    }
}