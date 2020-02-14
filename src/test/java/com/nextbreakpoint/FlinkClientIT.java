package com.nextbreakpoint;

import com.google.gson.Gson;
import com.nextbreakpoint.flinkclient.api.ApiCallback;
import com.nextbreakpoint.flinkclient.api.ApiException;
import com.nextbreakpoint.flinkclient.api.FlinkApi;
import com.nextbreakpoint.flinkclient.model.*;
import org.awaitility.Awaitility;
import org.awaitility.core.ThrowingRunnable;
import org.junit.jupiter.api.*;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.mockito.internal.util.io.IOUtil;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
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
    private static final String JAR_URL = "https://github.com/nextbreakpoint/flink-workshop/releases/download/v1.2.0/com.nextbreakpoint.flinkworkshop-1.2.0.jar";
    private static final String JAR_PATH = "/tmp/com.nextbreakpoint.flinkworkshop-1.2.0.jar";
    private static final String ZIP_PATH = "/tmp/com.nextbreakpoint.flinkworkshop-1.2.0.zip";
    private static final String JAR_NAME = "com.nextbreakpoint.flinkworkshop-1.2.0.jar";
    private static final String ENTRY_CLASS = "com.nextbreakpoint.flink.jobs.stream.TestJob";

    private FlinkApi api;

    private void dumpAsJson(Object object) {
        System.out.println(new Gson().toJson(object));
    }

    private void await(ThrowingRunnable throwingRunnable) {
        Awaitility.await()
                .pollInterval(1, TimeUnit.SECONDS)
                .pollDelay(2, TimeUnit.SECONDS)
                .atMost(20, TimeUnit.SECONDS)
                .untilAsserted(throwingRunnable);
    }

    private class TestCallback<T> implements ApiCallback<T> {
        private final boolean logException;
        volatile ApiException exception;
        volatile T result;
        volatile boolean completed;

        public TestCallback() {
            this(true);
        }

        public TestCallback(boolean logException) {
            this.logException = logException;
        }

        @Override
        public void onFailure(ApiException e, int statusCode, Map<String, List<String>> responseHeaders) {
            if (logException) {
                e.printStackTrace();
            }
            this.exception = e;
            completed = true;
        }

        @Override
        public void onSuccess(T result, int statusCode, Map<String, List<String>> responseHeaders) {
            dumpAsJson(result);
            this.result = result;
            completed = true;
        }

        @Override
        public void onUploadProgress(long bytesWritten, long contentLength, boolean done) {
        }

        @Override
        public void onDownloadProgress(long bytesRead, long contentLength, boolean done) {
        }
    }

    private void runTestJar() throws ApiException {
        final JarUploadResponseBody result = api.uploadJar(new File(JAR_PATH));
        assertThat(result).isNotNull();
        dumpAsJson(result);
        final JarListInfo jarListInfo = api.listJars();
        assertThat(jarListInfo).isNotNull();
        dumpAsJson(jarListInfo);
        final JarRunResponseBody response = api.runJar(jarListInfo.getFiles().get(0).getId(), true, null, "--PARALLELISM 1", null, "com.nextbreakpoint.flink.jobs.stream.TestJob", null);
        assertThat(response).isNotNull();
        dumpAsJson(response);
//        assertThat(response.getJobId()).isNotNull();
    }

    private void terminateAllJobs() throws ApiException {
        final JobIdsWithStatusOverview statusOverview = api.getJobs();
        assertThat(statusOverview).isNotNull();
        dumpAsJson(statusOverview);
        statusOverview.getJobs().forEach(jobIdWithStatus -> {
            try {
                api.terminateJob(jobIdWithStatus.getId(), "cancel");
            } catch (ApiException ignored) {
            }
        });
    }

    private void runTestJarAsync() throws ApiException {
        final JarUploadResponseBody result = api.uploadJar(new File(JAR_PATH));
        assertThat(result).isNotNull();
        dumpAsJson(result);
        final JarListInfo jarListInfo = api.listJars();
        assertThat(jarListInfo).isNotNull();
        dumpAsJson(jarListInfo);
        final TestCallback<JarRunResponseBody> callback = new TestCallback<>(false);
        api.runJarAsync(jarListInfo.getFiles().get(0).getId(), true, null, "--PARALLELISM 1", null, "com.nextbreakpoint.flink.jobs.stream.TestJob", null, callback);
        await(() -> {
            assertThat(callback.result).isNotNull();
            assertThat(callback.completed).isTrue();
        });
    }

    private void terminateAllJobsAsync() throws ApiException {
        final JobIdsWithStatusOverview statusOverview = api.getJobs();
        assertThat(statusOverview).isNotNull();
        dumpAsJson(statusOverview);
        statusOverview.getJobs().forEach(jobIdWithStatus -> {
            try {
                final TestCallback<Void> callback = new TestCallback<>(false);
                api.terminateJobAsync(jobIdWithStatus.getId(), "cancel", callback);
                await(() -> {
                    assertThat(callback.completed).isTrue();
                });
            } catch (ApiException ignored) {
            }
        });
    }

    private JobIdWithStatus getRunningJob() throws ApiException {
        await(() -> {
            final JobIdsWithStatusOverview jobIdsWithStatusOverview = api.getJobs();
            final List<JobIdWithStatus> jobs = listRunningJobs(jobIdsWithStatusOverview);
            assertThat(jobs).hasSize(1);
        });
        final JobIdsWithStatusOverview jobIdsWithStatusOverview = api.getJobs();
        dumpAsJson(jobIdsWithStatusOverview);
        final List<JobIdWithStatus> jobs = listRunningJobs(jobIdsWithStatusOverview);
        assertThat(jobs).hasSize(1);
        return jobs.get(0);
    }

    private List<JobIdWithStatus> listRunningJobs(JobIdsWithStatusOverview jobIdsWithStatusOverview) {
        return jobIdsWithStatusOverview.getJobs()
                .stream()
                .filter(jobIdWithStatus -> jobIdWithStatus.getStatus().getValue().equals("RUNNING"))
                .collect(Collectors.toList());
    }

    private void verifyDashboardConfiguration(DashboardConfiguration dashboardConfiguration) {
        assertThat(dashboardConfiguration.getRefreshInterval()).isEqualTo(3000);
        assertThat(dashboardConfiguration.getTimezoneName()).isNotBlank();
        assertThat(dashboardConfiguration.getTimezoneOffset()).isNotNull();
        assertThat(dashboardConfiguration.getFlinkVersion()).isEqualTo("1.9.2");
        assertThat(dashboardConfiguration.getFlinkRevision()).isNotBlank();
    }

    private void verifyJarUploadResponseBody(JarUploadResponseBody jarUploadResponseBody) {
        assertThat(jarUploadResponseBody.getStatus()).isEqualTo(JarUploadResponseBody.StatusEnum.SUCCESS);
        assertThat(jarUploadResponseBody.getFilename()).endsWith("_" + JAR_NAME);
        assertThat(jarUploadResponseBody.getFilename()).contains("/flink-web-upload");
    }

    private void verifyJarListInfo(JarUploadResponseBody jarUploadResponseBody, JarListInfo jarListInfo) {
        assertThat(jarListInfo.getFiles().size() > 0).isTrue();
        IntStream.range(0, jarListInfo.getFiles().size()).forEach((index) -> {
            assertThat(jarListInfo.getFiles().get(index).getId()).endsWith("_" + JAR_NAME);
            assertThat(jarListInfo.getFiles().get(index).getName()).isEqualTo(JAR_NAME);
        });
        assertThat(jarListInfo.getFiles().stream().anyMatch(file -> jarUploadResponseBody.getFilename().endsWith("/" + file.getId()))).isTrue();
    }

    private void verifyJarDeleted(JarListInfo jarListInfo, String fileInfoId) {
        assertThat(jarListInfo.getFiles().stream().filter(fileInfo -> fileInfo.getId().equals(fileInfoId)).count()).isEqualTo(0);
    }

    private void verifyTaskManagerDetailsInfo(TaskManagerDetailsInfo taskManagerDetails) {
        assertThat(taskManagerDetails.getMetrics()).isNotNull();
        assertThat(taskManagerDetails.getHardware().getFreeMemory()).isGreaterThan(0);
    }

    private void verifyTaskManagersInfo(TaskManagersInfo taskManagerInfo) {
        assertThat(taskManagerInfo.getTaskmanagers()).isNotNull();
        assertThat(taskManagerInfo.getTaskmanagers()).hasSize(1);
    }

    private void verifyMultipleJobsDetails(MultipleJobsDetails multipleJobsDetails) {
        assertThat(multipleJobsDetails.getJobs().size() > 0).isTrue();
    }

    private void verifyJobIdWithStatusOverview(JobIdsWithStatusOverview jobIdsWithStatusOverview) {
        assertThat(jobIdsWithStatusOverview.getJobs().size() > 0).isTrue();
    }

    private void verifyClusterOverviewWithVersion(ClusterOverviewWithVersion clusterOverviewWithVersion) {
        assertThat(clusterOverviewWithVersion.getFlinkVersion()).isEqualTo("1.9.2");
    }

    @BeforeEach
    void setup() throws IOException {
        api = new FlinkApi();
        api.getApiClient().setBasePath("http://localhost:8081");

        // download jar containing jobs if not exists
        if (!Files.exists(Paths.get(JAR_PATH))) {
            try (BufferedInputStream in = new BufferedInputStream(new URL(JAR_URL).openStream());
                FileOutputStream fileOutputStream = new FileOutputStream(JAR_PATH)) {
                byte bytes[] = new byte[1024];
                int length;
                while ((length = in.read(bytes, 0, 1024)) != -1) {
                    fileOutputStream.write(bytes, 0, length);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Nested
    class Config {
        @Test
        void shouldShowConfig() throws ApiException {
            // when
            final DashboardConfiguration config = api.showConfig();

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
            final TestCallback<DashboardConfiguration> callback = new TestCallback<>();

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
    class Cluster {
        @Test
        void shouldReturnOverview() throws ApiException {
            // when
            final ClusterOverviewWithVersion clusterOverviewWithVersion = api.getOverview();

            // then
            assertThat(clusterOverviewWithVersion).isNotNull();
            dumpAsJson(clusterOverviewWithVersion);
            verifyClusterOverviewWithVersion(clusterOverviewWithVersion);
        }

        @Test
        void shouldShutdownCluster() throws ApiException {
//            api.shutdownCluster();
        }
    }

    @Nested
    class ClusterAsync {
        @Test
        void shouldReturnOverview() throws ApiException {
            // given
            final TestCallback<ClusterOverviewWithVersion> callback = new TestCallback<>();

            // when
            api.getOverviewAsync(callback);

            // then
            await(() -> {
                assertThat(callback.result).isNotNull();
                verifyClusterOverviewWithVersion(callback.result);
            });
        }

        @Test
        void shouldShutdownCluster() {
//            final TestCallback<Void> callback = new TestCallback<>();
//
//            api.shutdownClusterAsync(callback);
        }
    }

    @Nested
    class Jars {
        @Test
        void shouldUploadJar() throws ApiException {
            // when
            final JarUploadResponseBody jarUploadResponseBody = api.uploadJar(new File(JAR_PATH));

            // then
            assertThat(jarUploadResponseBody).isNotNull();
            dumpAsJson(jarUploadResponseBody);
            verifyJarUploadResponseBody(jarUploadResponseBody);
        }

        @Test
        void shouldThrowWhenUploadingNonExistentFile() {
            assertThatThrownBy(() -> {
                api.uploadJar(new File(ZIP_PATH));
            }).isInstanceOf(ApiException.class);
        }

        @Test
        void shouldListJar() throws ApiException {
            // given
            final JarUploadResponseBody jarUploadResponseBody = api.uploadJar(new File(JAR_PATH));
            dumpAsJson(jarUploadResponseBody);

            // when
            final JarListInfo jarListInfo = api.listJars();

            // then
            assertThat(jarListInfo).isNotNull();
            dumpAsJson(jarListInfo);
            verifyJarListInfo(jarUploadResponseBody, jarListInfo);
        }

        @Test
        void shouldDeleteJar() throws ApiException {
            // given
            final JarUploadResponseBody jarUploadResponseBody = api.uploadJar(new File(JAR_PATH));
            dumpAsJson(jarUploadResponseBody);
            final JarFileInfo jarFileInfo = api.listJars().getFiles().get(0);

            // when
            api.deleteJar(jarFileInfo.getId());

            // then
            final JarListInfo jarListInfo = api.listJars();
            dumpAsJson(jarListInfo);
            verifyJarDeleted(jarListInfo, jarFileInfo.getId());
        }

        @Test
        void shouldReturnPlan() throws ApiException {
            // given
            final JarUploadResponseBody jarUploadResponseBody = api.uploadJar(new File(JAR_PATH));
            dumpAsJson(jarUploadResponseBody);
            final JarFileInfo jarFileInfo = api.listJars().getFiles().get(0);

            // when
            final JobPlanInfo jobPlanInfo = api.showPlan(jarFileInfo.getId(), "--PARALLELISM 1", null, ENTRY_CLASS, 1);

            // then
            assertThat(jobPlanInfo).isNotNull();
            dumpAsJson(jobPlanInfo);
            assertThat(jobPlanInfo.getPlan()).isNotNull();
        }
    }

    @Nested
    class JarsAsync {
        @Test
        void shouldUploadJar() throws ApiException {
            // given
            final TestCallback<JarUploadResponseBody> callback = new TestCallback<>();

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
            final TestCallback<JarUploadResponseBody> callback = new TestCallback<>();

            // when
            api.uploadJarAsync(new File(ZIP_PATH), callback);

            // then
            await(() -> assertThat(callback.exception).isNotNull());
        }

        @Test
        void shouldListJar() throws ApiException {
            // given
            final TestCallback<JarListInfo> callback = new TestCallback<>();
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
            final TestCallback<Void> callback = new TestCallback<>();
            final JarUploadResponseBody result = api.uploadJar(new File(JAR_PATH));
            dumpAsJson(result);
            final JarFileInfo jarFileInfo = api.listJars().getFiles().get(0);

            // when
            api.deleteJarAsync(jarFileInfo.getId(), callback);

            // then
            await(() -> {
                final JarListInfo jarListInfo = api.listJars();
                dumpAsJson(jarListInfo);
                verifyJarDeleted(jarListInfo, jarFileInfo.getId());
            });
        }

        @Test
        void shouldReturnPlan() throws ApiException {
            // given
            final TestCallback<JobPlanInfo> callback = new TestCallback<>();
            final JarUploadResponseBody jarUploadResponseBody = api.uploadJar(new File(JAR_PATH));
            dumpAsJson(jarUploadResponseBody);
            final JarFileInfo jarFileInfo = api.listJars().getFiles().get(0);

            // when
            api.showPlanAsync(jarFileInfo.getId(), "--PARALLELISM 1", null, "com.nextbreakpoint.flink.jobs.stream.TestJob", 1, callback);

            // then
            await(() -> {
                assertThat(callback.result).isNotNull();
                assertThat(callback.result.getPlan()).isNotNull();
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
        void shouldReturnJobsDetailsAndOthers() throws ApiException {
            // when
            final JobIdsWithStatusOverview jobIdsWithStatusOverview = api.getJobs();

            // then
            assertThat(jobIdsWithStatusOverview).isNotNull();
            dumpAsJson(jobIdsWithStatusOverview);
            verifyJobIdWithStatusOverview(jobIdsWithStatusOverview);

            // when
            final MultipleJobsDetails multipleJobsDetails = api.getJobsOverview();

            // then
            assertThat(multipleJobsDetails).isNotNull();
            dumpAsJson(multipleJobsDetails);
            verifyMultipleJobsDetails(multipleJobsDetails);

            // when
            final Object jobsMetrics = api.getJobsMetrics("numberOfFailedCheckpoints", "sum", null);

            // then
            assertThat(jobsMetrics).isNotNull();
            dumpAsJson(jobsMetrics);
        }

        @Test
        void shouldReturnJobDetailsAndOthers() throws ApiException {
            // given
            final String jobId = getRunningJob().getId();

            // when
            final String jobConfig = api.getJobConfig(jobId);

            // then
            assertThat(jobConfig).isNotNull();
            dumpAsJson(jobConfig);

            // when
            final JobPlanInfo jobPlan = api.getJobPlan(jobId);

            // then
            assertThat(jobPlan).isNotNull();
            dumpAsJson(jobPlan);
            assertThat(jobPlan.getPlan()).isNotNull();

            // when
            final Object jobMetrics = api.getJobMetrics(jobId, "numberOfFailedCheckpoints");

            // then
            assertThat(jobMetrics).isNotNull();
            dumpAsJson(jobMetrics);

            // when
            final JobDetailsInfo jobDetailsInfo = api.getJobDetails(jobId);

            // then
            assertThat(jobDetailsInfo).isNotNull();
            dumpAsJson(jobDetailsInfo);
            assertThat(jobDetailsInfo.getPlan()).isNotNull();
            assertThat(jobDetailsInfo.getJid()).isEqualTo(jobId);
            assertThat(jobDetailsInfo.getVertices()).hasSize(1);

            // when
            final JobExecutionResultResponseBody jobExecutionResultResponseBody = api.getJobResult(jobId);

            // then
            assertThat(jobExecutionResultResponseBody).isNotNull();
            dumpAsJson(jobExecutionResultResponseBody);
            assertThat(jobExecutionResultResponseBody.getStatus().getId()).isEqualTo(QueueStatus.IdEnum.IN_PROGRESS);
            assertThat(jobExecutionResultResponseBody.getJobExecutionResult()).isNull();

            // when
            final JobExceptionsInfo jobExceptionsInfo = api.getJobExceptions(jobId);

            // then
            assertThat(jobExceptionsInfo).isNotNull();
            dumpAsJson(jobExceptionsInfo);
            assertThat(jobExceptionsInfo.getAllExceptions()).isNotNull();

            // when
            final JobAccumulatorsInfo jobAccumulatorsInfo = api.getJobAccumulators(jobId, true);

            // then
            assertThat(jobAccumulatorsInfo).isNotNull();
            dumpAsJson(jobAccumulatorsInfo);
            assertThat(jobAccumulatorsInfo.getJobAccumulators()).isNotNull();

            // when
            final CheckpointingStatistics checkpointingStatistics = api.getJobCheckpoints(jobId);

            // then
            assertThat(checkpointingStatistics).isNotNull();
            dumpAsJson(checkpointingStatistics);
            assertThat(checkpointingStatistics.getCounts()).isNotNull();
        }

        @Test
        void shouldReturnJobTaskDetailsAndOthers() throws ApiException {
            // given
            final String jobId = getRunningJob().getId();
            final JobDetailsInfo jobDetailsInfo = api.getJobDetails(jobId);
            final String vertexid = jobDetailsInfo.getVertices().get(0).getId().toString();

            // when
            final Object jobTaskMetrics = api.getJobTaskMetrics(jobId, vertexid, "numRecordsIn");/*TODO*/

            // then
            assertThat(jobTaskMetrics).isNotNull();
            dumpAsJson(jobTaskMetrics);

            // when
            final JobVertexBackPressureInfo jobTaskBackpressure = api.getJobTaskBackpressure(jobId, vertexid);

            // then
            assertThat(jobTaskBackpressure).isNotNull();
            dumpAsJson(jobTaskBackpressure);
            assertThat(jobTaskBackpressure.getStatus()).isEqualTo(JobVertexBackPressureInfo.StatusEnum.DEPRECATED);

            // when
            final JobVertexDetailsInfo jobVertexDetailsInfo = api.getJobTaskDetails(jobId, vertexid);

            // then
            assertThat(jobVertexDetailsInfo).isNotNull();
            dumpAsJson(jobVertexDetailsInfo);
            assertThat(jobVertexDetailsInfo.getSubtasks()).hasSize(1);

            // when
            final JobVertexAccumulatorsInfo jobVertexAccumulatorsInfo = api.getJobTaskAccumulators(jobId, vertexid);

            // then
            assertThat(jobVertexAccumulatorsInfo).isNotNull();
            dumpAsJson(jobVertexAccumulatorsInfo);
            assertThat(jobVertexAccumulatorsInfo.getUserAccumulators()).isNotNull();

            // when
            final JobVertexTaskManagersInfo jobVertexTaskManagersInfo = api.getJobTaskDetailsByTaskManager(jobId, vertexid);

            // then
            assertThat(jobVertexTaskManagersInfo).isNotNull();
            dumpAsJson(jobVertexTaskManagersInfo);
            assertThat(jobVertexTaskManagersInfo.getTaskmanagers()).hasSize(1);
        }

        @Test
        void shouldReturnJobSubtaskDetailsAndOthers() throws ApiException {
            // given
            final String jobId = getRunningJob().getId();
            final JobDetailsInfo jobDetailsInfo = api.getJobDetails(jobId);
            final String vertexId = jobDetailsInfo.getVertices().get(0).getId().toString();

            Awaitility.await()
                    .pollInterval(1, TimeUnit.SECONDS)
                    .pollDelay(5, TimeUnit.SECONDS)
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(() -> {
                        // when
                        final SubtaskExecutionAttemptDetailsInfo subtaskExecutionAttemptDetailsInfo = api.getJobSubtaskDetails(jobId, vertexId, 0);

                        // then
                        assertThat(subtaskExecutionAttemptDetailsInfo).isNotNull();
                        dumpAsJson(subtaskExecutionAttemptDetailsInfo);
                        assertThat(subtaskExecutionAttemptDetailsInfo.getStatus()).isEqualTo(SubtaskExecutionAttemptDetailsInfo.StatusEnum.RUNNING);
                    });

            // when
            final Object jobSubtaskMetrics = api.getJobSubtaskMetrics(jobId, vertexId, 0, "numRecordsIn");/*TODO*/

            // then
            assertThat(jobSubtaskMetrics).isNotNull();
            dumpAsJson(jobSubtaskMetrics);

            // when
            final Object jobAggregatedSubtaskMetrics = api.getJobAggregatedSubtaskMetrics(jobId, vertexId, "numRecordsIn", "sum", null);/*TODO*/

            // then
            assertThat(jobAggregatedSubtaskMetrics).isNotNull();
            dumpAsJson(jobAggregatedSubtaskMetrics);

            // when
            final SubtasksTimesInfo subtasksTimesInfo = api.getJobSubtaskTimes(jobId, vertexId);

            // then
            assertThat(subtasksTimesInfo).isNotNull();
            dumpAsJson(subtasksTimesInfo);
            assertThat(subtasksTimesInfo.getSubtasks()).hasSize(1);

            // when
            final SubtasksAllAccumulatorsInfo subtasksAllAccumulatorsInfo = api.getJobSubtaskAccumulators(jobId, vertexId);

            // then
            assertThat(subtasksAllAccumulatorsInfo).isNotNull();
            dumpAsJson(subtasksAllAccumulatorsInfo);
            assertThat(subtasksAllAccumulatorsInfo.getSubtasks()).hasSize(1);

//            // when
//            final SubtaskExecutionAttemptAccumulatorsInfo subtaskExecutionAttemptAccumulatorsInfo = api.getJobSubtaskAttemptAccumulators(jobId, vertexId, 0, 0);
//
//            // then
//            assertThat(subtaskExecutionAttemptAccumulatorsInfo).isNotNull();
//            dumpAsJson(subtaskExecutionAttemptAccumulatorsInfo);
//            assertThat(subtaskExecutionAttemptAccumulatorsInfo.getUserAccumulators()).isNotNull();
//
//            // when
//            final SubtaskExecutionAttemptDetailsInfo subtaskExecutionAttemptDetailsInfo = api.getJobSubtaskAttemptDetails(jobId, vertexId, 0, 0);
//
//            // then
//            assertThat(subtaskExecutionAttemptDetailsInfo).isNotNull();
//            dumpAsJson(subtaskExecutionAttemptDetailsInfo);
//            assertThat(subtaskExecutionAttemptDetailsInfo.getStatus()).isEqualTo(SubtaskExecutionAttemptDetailsInfo.StatusEnum.RUNNING);
        }

        @Test
        void shouldCreateSavepoint() throws ApiException {
            // given
            final String jobId = getRunningJob().getId();
            final JobDetailsInfo jobDetailsInfo = api.getJobDetails(jobId);
            final String vertexId = jobDetailsInfo.getVertices().get(0).getId();

            Awaitility.await()
                    .pollInterval(5, TimeUnit.SECONDS)
                    .pollDelay(10, TimeUnit.SECONDS)
                    .atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(() -> {
                        // when
                        final SubtaskExecutionAttemptDetailsInfo subtaskExecutionAttemptDetailsInfo = api.getJobSubtaskDetails(jobId, vertexId, 0);

                        // then
                        assertThat(subtaskExecutionAttemptDetailsInfo).isNotNull();
                        dumpAsJson(subtaskExecutionAttemptDetailsInfo);
                        assertThat(subtaskExecutionAttemptDetailsInfo.getStatus()).isEqualTo(SubtaskExecutionAttemptDetailsInfo.StatusEnum.RUNNING);
                    });

            // when
            final TriggerResponse triggerSavepointResponse = api.createJobSavepoint(new SavepointTriggerRequestBody().cancelJob(false).targetDirectory("file:///var/tmp"), jobId);

            // then
            assertThat(triggerSavepointResponse).isNotNull();
            dumpAsJson(triggerSavepointResponse);
            assertThat(triggerSavepointResponse.getRequestId()).isNotNull();

            Awaitility.await()
                    .pollInterval(1, TimeUnit.SECONDS)
                    .pollDelay(5, TimeUnit.SECONDS)
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(() -> {
                        // when
                        final AsynchronousOperationResult asynchronousOperationResult = api.getJobSavepointStatus(jobId, triggerSavepointResponse.getRequestId());

                        // then
                        assertThat(asynchronousOperationResult).isNotNull();
                        dumpAsJson(asynchronousOperationResult);
                        assertThat(asynchronousOperationResult.getStatus().getId()).isEqualTo(QueueStatus.IdEnum.COMPLETED);
                    });

            Awaitility.await()
                    .pollInterval(5, TimeUnit.SECONDS)
                    .pollDelay(10, TimeUnit.SECONDS)
                    .atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(() -> {
                        final CheckpointingStatistics checkpointingStatistics = api.getJobCheckpoints(jobId);
                        assertThat(checkpointingStatistics.getLatest()).isNotNull();
                        assertThat(checkpointingStatistics.getLatest().getSavepoint()).isNotNull();
                    });

            // given
            final CheckpointingStatistics checkpointingStatistics = api.getJobCheckpoints(jobId);
            final String savepointPath = checkpointingStatistics.getLatest().getSavepoint().getExternalPath();

            // when
            final TriggerResponse triggerSavepointDisposalResponse = api.triggerSavepointDisposal(new SavepointDisposalRequest().savepointPath(savepointPath));

            // then
            assertThat(triggerSavepointDisposalResponse).isNotNull();
            dumpAsJson(triggerSavepointDisposalResponse);
            assertThat(triggerSavepointDisposalResponse.getRequestId()).isNotNull();

            Awaitility.await()
                    .pollInterval(1, TimeUnit.SECONDS)
                    .pollDelay(5, TimeUnit.SECONDS)
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(() -> {
                        // when
                        final AsynchronousOperationResult asynchronousOperationResult = api.getSavepointDisposalStatus(triggerSavepointDisposalResponse.getRequestId());

                        // then
                        assertThat(asynchronousOperationResult).isNotNull();
                        dumpAsJson(asynchronousOperationResult);
                        assertThat(asynchronousOperationResult.getStatus().getId()).isEqualTo(QueueStatus.IdEnum.COMPLETED);
                    });
        }

        @Test
        @Disabled
        void shouldRescaleJob() throws ApiException {
            // given
            final String jobId = getRunningJob().getId();

            // when
            final TriggerResponse triggerRescalingResponse = api.triggerJobRescaling(jobId, 2);

            // then
            assertThat(triggerRescalingResponse).isNotNull();
            dumpAsJson(triggerRescalingResponse);
            assertThat(triggerRescalingResponse.getRequestId()).isNotNull();

            Awaitility.await()
                    .pollInterval(1, TimeUnit.SECONDS)
                    .pollDelay(5, TimeUnit.SECONDS)
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(() -> {
                        // when
                        final AsynchronousOperationResult asynchronousOperationResult = api.getJobRescalingStatus(jobId, triggerRescalingResponse.getRequestId());

                        // then
                        assertThat(asynchronousOperationResult).isNotNull();
                        dumpAsJson(asynchronousOperationResult);
                        assertThat(asynchronousOperationResult.getStatus().getId()).isEqualTo(QueueStatus.IdEnum.COMPLETED);
                    });
        }

        @Test
        void shouldReturnCheckpointsDetailsAndStatistics() throws ApiException {
            // given
            final String jobId = getRunningJob().getId();
            final JobDetailsInfo jobDetailsInfo = api.getJobDetails(jobId);
            final String vertexId = jobDetailsInfo.getVertices().get(0).getId();

            // when
            final CheckpointConfigInfo checkpointConfigInfo = api.getJobCheckpointsConfig(jobId);

            // then
            assertThat(checkpointConfigInfo).isNotNull();
            dumpAsJson(checkpointConfigInfo);
            assertThat(checkpointConfigInfo.getInterval()).isEqualTo(60000L);
            assertThat(checkpointConfigInfo.getTimeout()).isEqualTo(600000L);

            Awaitility.await()
                    .pollInterval(20, TimeUnit.SECONDS)
                    .pollDelay(20, TimeUnit.SECONDS)
                    .atMost(120, TimeUnit.SECONDS)
                    .untilAsserted(() -> {
                        // when
                        final CheckpointingStatistics jobCheckpoints = api.getJobCheckpoints(jobId);

                        // then
                        assertThat(jobCheckpoints).isNotNull();
                        dumpAsJson(jobCheckpoints);
                        assertThat(jobCheckpoints.getHistory()).isNotNull();
                        assertThat(jobCheckpoints.getHistory().size() > 0).isTrue();
                    });

            // given
            final CheckpointingStatistics jobCheckpoints = api.getJobCheckpoints(jobId);
            final Integer checkpointId = jobCheckpoints.getHistory().get(0).getId();

            // when
            final CheckpointStatistics jobCheckpointDetails = api.getJobCheckpointDetails(jobId, checkpointId);

            // then
            assertThat(jobCheckpointDetails).isNotNull();
            dumpAsJson(jobCheckpointDetails);
            assertThat(jobCheckpointDetails.getStatus().getValue()).isEqualTo(CheckpointStatistics.StatusEnum.COMPLETED.getValue());

            // when
            final TaskCheckpointStatisticsWithSubtaskDetails jobCheckpointStatistics = api.getJobCheckpointStatistics(jobId, checkpointId, vertexId);

            // then
            assertThat(jobCheckpointStatistics).isNotNull();
            dumpAsJson(jobCheckpointStatistics);
            assertThat(jobCheckpointStatistics.getStatus().getValue()).isEqualTo(CheckpointStatistics.StatusEnum.COMPLETED.getValue());
        }
    }

    @Nested
    class JobsAync {
        @BeforeEach
        void terminateJobs() throws ApiException {
            terminateAllJobsAsync();
        }

        @BeforeEach
        void runJob() throws ApiException {
            runTestJarAsync();
        }

        @Test
        void shouldReturnJobsDetailsAndOthers() throws ApiException {
            // given
            final TestCallback<JobIdsWithStatusOverview> jobIdsWithStatusOverviewCallback = new TestCallback<>();

            // when
            api.getJobsAsync(jobIdsWithStatusOverviewCallback);

            // then
            await(() -> {
                assertThat(jobIdsWithStatusOverviewCallback.result).isNotNull();
                verifyJobIdWithStatusOverview(jobIdsWithStatusOverviewCallback.result);
            });

            // given
            final TestCallback<MultipleJobsDetails> multipleJobsDetailsCallback = new TestCallback<>();

            // when
            api.getJobsOverviewAsync(multipleJobsDetailsCallback);

            // then
            await(() -> {
                assertThat(multipleJobsDetailsCallback.result).isNotNull();
                verifyMultipleJobsDetails(multipleJobsDetailsCallback.result);
            });

            // given
            final TestCallback<Object> jobsMetricsCallback = new TestCallback<>();

            // when
            api.getJobsMetricsAsync("numberOfFailedCheckpoints", "sum", null, jobsMetricsCallback);

            // then
            await(() -> {
                assertThat(jobsMetricsCallback.result).isNotNull();
            });
        }

        @Test
        void shouldReturnJobDetailsAndOthers() throws ApiException {
            // given
            final String jobId = getRunningJob().getId();

            final TestCallback<String> jobConfigCallback = new TestCallback<>();

            // when
            api.getJobConfigAsync(jobId, jobConfigCallback);

            // then
            await(() -> {
                assertThat(jobConfigCallback.result).isNotNull();
            });

            // given
            final TestCallback<JobPlanInfo> jobPlanCallback = new TestCallback<>();

            // when
            api.getJobPlanAsync(jobId, jobPlanCallback);

            // then
            await(() -> {
                assertThat(jobPlanCallback.result).isNotNull();
                assertThat(jobPlanCallback.result.getPlan()).isNotNull();
            });

            // given
            final TestCallback<Object> jobMetricsCallback = new TestCallback<>();

            // when
            api.getJobMetricsAsync(jobId, "numberOfFailedCheckpoints", jobMetricsCallback);

            // then
            await(() -> {
                assertThat(jobMetricsCallback.result).isNotNull();
            });

            // given
            final TestCallback<JobDetailsInfo> jobDetailsCallback = new TestCallback<>();

            // when
            api.getJobDetailsAsync(jobId, jobDetailsCallback);

            // then
            await(() -> {
                assertThat(jobDetailsCallback.result).isNotNull();
                assertThat(jobDetailsCallback.result.getPlan()).isNotNull();
                assertThat(jobDetailsCallback.result.getJid()).isEqualTo(jobId);
                assertThat(jobDetailsCallback.result.getVertices()).hasSize(1);
            });

            // given
            final TestCallback<JobExecutionResultResponseBody> jobExecutionResultResponseBodyCallback = new TestCallback<>();

            // when
            api.getJobResultAsync(jobId, jobExecutionResultResponseBodyCallback);

            // then
            await(() -> {
                assertThat(jobExecutionResultResponseBodyCallback.result).isNotNull();
                assertThat(jobExecutionResultResponseBodyCallback.result.getStatus().getId()).isEqualTo(QueueStatus.IdEnum.IN_PROGRESS);
                assertThat(jobExecutionResultResponseBodyCallback.result.getJobExecutionResult()).isNull();
            });

            // given
            final TestCallback<JobExceptionsInfo> jobExceptionsInfoCallback = new TestCallback<>();

            // when
            api.getJobExceptionsAsync(jobId, jobExceptionsInfoCallback);

            // then
            await(() -> {
                assertThat(jobExceptionsInfoCallback.result).isNotNull();
                assertThat(jobExceptionsInfoCallback.result.getAllExceptions()).isNotNull();
            });

            // given
            final TestCallback<JobAccumulatorsInfo> jobAccumulatorsInfoCallback = new TestCallback<>();

            // when
            api.getJobAccumulatorsAsync(jobId, true, jobAccumulatorsInfoCallback);

            // then
            await(() -> {
                assertThat(jobAccumulatorsInfoCallback.result).isNotNull();
                assertThat(jobAccumulatorsInfoCallback.result.getJobAccumulators()).isNotNull();
            });

            // given
            final TestCallback<CheckpointingStatistics> checkpointingStatisticsCallback = new TestCallback<>();

            // when
            api.getJobCheckpointsAsync(jobId, checkpointingStatisticsCallback);

            // then
            await(() -> {
                assertThat(checkpointingStatisticsCallback.result).isNotNull();
                assertThat(checkpointingStatisticsCallback.result.getCounts()).isNotNull();
            });
        }

        @Test
        void shouldReturnJobTaskDetailsAndOthers() throws ApiException {
            // given
            final String jobId = getRunningJob().getId();
            final JobDetailsInfo jobDetailsInfo = api.getJobDetails(jobId);
            final String vertexid = jobDetailsInfo.getVertices().get(0).getId();

            final TestCallback<Object> jobTaskMetricsCallback = new TestCallback<>();

            // when
            api.getJobTaskMetricsAsync(jobId, vertexid, "numRecordsIn", jobTaskMetricsCallback);/*TODO*/

            // then
            await(() -> {
                assertThat(jobTaskMetricsCallback.result).isNotNull();
            });

            // given
            final TestCallback<JobVertexBackPressureInfo> jobTaskBackpressureCallback = new TestCallback<>();

            // when
            api.getJobTaskBackpressureAsync(jobId, vertexid, jobTaskBackpressureCallback);

            // then
            await(() -> {
                assertThat(jobTaskBackpressureCallback.result).isNotNull();
                assertThat(jobTaskBackpressureCallback.result.getStatus()).isEqualTo(JobVertexBackPressureInfo.StatusEnum.DEPRECATED);
            });

            // given
            final TestCallback<JobVertexDetailsInfo> jobVertexDetailsInfoCallback = new TestCallback<>();

            // when
            api.getJobTaskDetailsAsync(jobId, vertexid, jobVertexDetailsInfoCallback);

            // then
            await(() -> {
                assertThat(jobVertexDetailsInfoCallback.result).isNotNull();
                assertThat(jobVertexDetailsInfoCallback.result.getSubtasks()).hasSize(1);
            });

            // given
            final TestCallback<JobVertexAccumulatorsInfo> jobVertexAccumulatorsInfoCallback = new TestCallback<>();

            // when
            api.getJobTaskAccumulatorsAsync(jobId, vertexid, jobVertexAccumulatorsInfoCallback);

            // then
            await(() -> {
                assertThat(jobVertexAccumulatorsInfoCallback.result).isNotNull();
                assertThat(jobVertexAccumulatorsInfoCallback.result.getUserAccumulators()).isNotNull();
            });

            // given
            final TestCallback<JobVertexTaskManagersInfo> jobVertexTaskManagersInfoCallback = new TestCallback<>();

            // when
            api.getJobTaskDetailsByTaskManagerAsync(jobId, vertexid, jobVertexTaskManagersInfoCallback);

            // then
            await(() -> {
                assertThat(jobVertexTaskManagersInfoCallback.result).isNotNull();
                assertThat(jobVertexTaskManagersInfoCallback.result.getTaskmanagers()).hasSize(1);
            });
        }

        @Test
        void shouldReturnJobSubtaskDetailsAndOthers() throws ApiException {
            // given
            final String jobId = getRunningJob().getId();
            final JobDetailsInfo jobDetailsInfo = api.getJobDetails(jobId);
            final String vertexId = jobDetailsInfo.getVertices().get(0).getId();

            Awaitility.await()
                    .pollInterval(1, TimeUnit.SECONDS)
                    .pollDelay(5, TimeUnit.SECONDS)
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(() -> {
                        // when
                        final SubtaskExecutionAttemptDetailsInfo subtaskExecutionAttemptDetailsInfo = api.getJobSubtaskDetails(jobId, vertexId, 0);

                        // then
                        assertThat(subtaskExecutionAttemptDetailsInfo).isNotNull();
                        dumpAsJson(subtaskExecutionAttemptDetailsInfo);
                        assertThat(subtaskExecutionAttemptDetailsInfo.getStatus()).isEqualTo(SubtaskExecutionAttemptDetailsInfo.StatusEnum.RUNNING);
                    });

            // given
            final TestCallback<SubtaskExecutionAttemptDetailsInfo> subtaskExecutionDetailsInfoCallback = new TestCallback<>();

            // when
            api.getJobSubtaskDetailsAsync(jobId, vertexId, 0, subtaskExecutionDetailsInfoCallback);

            await(() -> {
                assertThat(subtaskExecutionDetailsInfoCallback.result).isNotNull();
                assertThat(subtaskExecutionDetailsInfoCallback.result.getStatus()).isEqualTo(SubtaskExecutionAttemptDetailsInfo.StatusEnum.RUNNING);
            });

            // given
            final TestCallback<Object> jobSubtaskMetricsCallback = new TestCallback<>();

            // when
            api.getJobSubtaskMetricsAsync(jobId, vertexId, 0, "numRecordsIn", jobSubtaskMetricsCallback);/*TODO*/

            // then
            await(() -> {
                assertThat(jobSubtaskMetricsCallback.result).isNotNull();
            });

            // given
            final TestCallback<Object> jobAggregatedSubtaskMetricsCallback = new TestCallback<>();

            // when
            api.getJobAggregatedSubtaskMetricsAsync(jobId, vertexId, "numRecordsIn", "sum", null, jobAggregatedSubtaskMetricsCallback);/*TODO*/

            // then
            await(() -> {
                assertThat(jobAggregatedSubtaskMetricsCallback.result).isNotNull();
            });

            // given
            final TestCallback<SubtasksTimesInfo> subtasksTimesInfoCallback = new TestCallback<>();

            // when
            api.getJobSubtaskTimesAsync(jobId, vertexId, subtasksTimesInfoCallback);

            // then
            await(() -> {
                assertThat(subtasksTimesInfoCallback.result).isNotNull();
                assertThat(subtasksTimesInfoCallback.result.getSubtasks()).hasSize(1);
            });

            // given
            final TestCallback<SubtasksAllAccumulatorsInfo> subtasksAllAccumulatorsInfoCallback = new TestCallback<>();

            // when
            api.getJobSubtaskAccumulatorsAsync(jobId, vertexId, subtasksAllAccumulatorsInfoCallback);

            // then
            await(() -> {
                assertThat(subtasksAllAccumulatorsInfoCallback.result).isNotNull();
                assertThat(subtasksAllAccumulatorsInfoCallback.result.getSubtasks()).hasSize(1);
            });

//            // given
//            final TestCallback<SubtaskExecutionAttemptAccumulatorsInfo> subtaskExecutionAttemptAccumulatorsInfoCallback = new TestCallback<>();
//
//            // when
//            api.getJobSubtaskAttemptAccumulatorsAsync(jobId, vertexId, 0, 0, subtaskExecutionAttemptAccumulatorsInfoCallback);
//
//            // then
//            await(() -> {
//                assertThat(subtaskExecutionAttemptAccumulatorsInfoCallback.result).isNotNull();
//                assertThat(subtaskExecutionAttemptAccumulatorsInfoCallback.result.getUserAccumulators()).isNotNull();
//            });
//
//            // given
//            final TestCallback<SubtaskExecutionAttemptDetailsInfo> subtaskExecutionAttemptDetailsInfoCallback = new TestCallback<>();
//
//            // when
//            api.getJobSubtaskAttemptDetailsAsync(jobId, vertexId, 0, 0, subtaskExecutionAttemptDetailsInfoCallback);
//
//            // then
//            await(() -> {
//                assertThat(subtaskExecutionAttemptDetailsInfoCallback.result).isNotNull();
//                assertThat(subtaskExecutionAttemptDetailsInfoCallback.result.getStatus()).isEqualTo(SubtaskExecutionAttemptDetailsInfo.StatusEnum.RUNNING);
//            });
        }

        @Test
        void shouldCreateSavepoint() throws ApiException {
            // given
            final String jobId = getRunningJob().getId();
            final JobDetailsInfo jobDetailsInfo = api.getJobDetails(jobId);
            final String vertexId = jobDetailsInfo.getVertices().get(0).getId();

            Awaitility.await()
                    .pollInterval(5, TimeUnit.SECONDS)
                    .pollDelay(10, TimeUnit.SECONDS)
                    .atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(() -> {
                        // when
                        final SubtaskExecutionAttemptDetailsInfo subtaskExecutionAttemptDetailsInfo = api.getJobSubtaskDetails(jobId, vertexId, 0);

                        // then
                        assertThat(subtaskExecutionAttemptDetailsInfo).isNotNull();
                        dumpAsJson(subtaskExecutionAttemptDetailsInfo);
                        assertThat(subtaskExecutionAttemptDetailsInfo.getStatus()).isEqualTo(SubtaskExecutionAttemptDetailsInfo.StatusEnum.RUNNING);
                    });

            final TestCallback<TriggerResponse> triggerSavepointResponseCallback = new TestCallback<>();

            // when
            api.createJobSavepointAsync(new SavepointTriggerRequestBody().cancelJob(false).targetDirectory("file:///var/tmp"), jobId, triggerSavepointResponseCallback);

            // then
            await(() -> {
                assertThat(triggerSavepointResponseCallback.result).isNotNull();
                assertThat(triggerSavepointResponseCallback.result.getRequestId()).isNotNull();
            });

            Awaitility.await()
                    .pollInterval(1, TimeUnit.SECONDS)
                    .pollDelay(5, TimeUnit.SECONDS)
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(() -> {
                        // when
                        final AsynchronousOperationResult asynchronousOperationResult = api.getJobSavepointStatus(jobId, triggerSavepointResponseCallback.result.getRequestId());

                        // then
                        assertThat(asynchronousOperationResult).isNotNull();
                        dumpAsJson(asynchronousOperationResult);
                        assertThat(asynchronousOperationResult.getStatus().getId()).isEqualTo(QueueStatus.IdEnum.COMPLETED);
                    });

            Awaitility.await()
                    .pollInterval(5, TimeUnit.SECONDS)
                    .pollDelay(10, TimeUnit.SECONDS)
                    .atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(() -> {
                        final CheckpointingStatistics checkpointingStatistics = api.getJobCheckpoints(jobId);
                        assertThat(checkpointingStatistics.getLatest()).isNotNull();
                        assertThat(checkpointingStatistics.getLatest().getSavepoint()).isNotNull();
                    });

            // given
            final TestCallback<AsynchronousOperationResult> savepointAsynchronousOperationResultCallback = new TestCallback<>();

            // when
            api.getJobSavepointStatusAsync(jobId, triggerSavepointResponseCallback.result.getRequestId(), savepointAsynchronousOperationResultCallback);

            // then
            await(() -> {
                assertThat(savepointAsynchronousOperationResultCallback.result).isNotNull();
                assertThat(savepointAsynchronousOperationResultCallback.result.getStatus().getId()).isEqualTo(QueueStatus.IdEnum.COMPLETED);
            });

            // given
            final CheckpointingStatistics checkpointingStatistics = api.getJobCheckpoints(jobId);
            final String savepointPath = checkpointingStatistics.getLatest().getSavepoint().getExternalPath();

            final TestCallback<TriggerResponse> triggerSavepointDisposalResponseCallback = new TestCallback<>();

            // when
            api.triggerSavepointDisposalAsync(new SavepointDisposalRequest().savepointPath(savepointPath), triggerSavepointDisposalResponseCallback);

            // then
            await(() -> {
                assertThat(triggerSavepointDisposalResponseCallback.result).isNotNull();
                assertThat(triggerSavepointDisposalResponseCallback.result.getRequestId()).isNotNull();
            });

            Awaitility.await()
                    .pollInterval(1, TimeUnit.SECONDS)
                    .pollDelay(5, TimeUnit.SECONDS)
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(() -> {
                        // when
                        final AsynchronousOperationResult asynchronousOperationResult = api.getSavepointDisposalStatus(triggerSavepointDisposalResponseCallback.result.getRequestId());

                        // then
                        assertThat(asynchronousOperationResult).isNotNull();
                        dumpAsJson(asynchronousOperationResult);
                        assertThat(asynchronousOperationResult.getStatus().getId()).isEqualTo(QueueStatus.IdEnum.COMPLETED);
                    });

            // given
            final TestCallback<AsynchronousOperationResult> savepointDisposalAsynchronousOperationResultCallback = new TestCallback<>();

            // when
            api.getSavepointDisposalStatusAsync(triggerSavepointDisposalResponseCallback.result.getRequestId(), savepointDisposalAsynchronousOperationResultCallback);

            // then
            await(() -> {
                assertThat(savepointDisposalAsynchronousOperationResultCallback.result).isNotNull();
                assertThat(savepointDisposalAsynchronousOperationResultCallback.result.getStatus().getId()).isEqualTo(QueueStatus.IdEnum.COMPLETED);
            });
        }

        @Test
        @Disabled
        void shouldRescaleJob() throws ApiException {
            // given
            final String jobId = getRunningJob().getId();

            final TestCallback<TriggerResponse> triggerRescalingResponseCallback = new TestCallback<>();

            // when
            api.triggerJobRescalingAsync(jobId, 2, triggerRescalingResponseCallback);

            // then
            await(() -> {
                assertThat(triggerRescalingResponseCallback.result).isNotNull();
                assertThat(triggerRescalingResponseCallback.result.getRequestId()).isNotNull();
            });

            Awaitility.await()
                    .pollInterval(1, TimeUnit.SECONDS)
                    .pollDelay(5, TimeUnit.SECONDS)
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(() -> {
                        // when
                        final AsynchronousOperationResult asynchronousOperationResult = api.getJobRescalingStatus(jobId, triggerRescalingResponseCallback.result.getRequestId());

                        // then
                        assertThat(asynchronousOperationResult).isNotNull();
                        dumpAsJson(asynchronousOperationResult);
                        assertThat(asynchronousOperationResult.getStatus().getId()).isEqualTo(QueueStatus.IdEnum.COMPLETED);
                    });

            // given
            final TestCallback<AsynchronousOperationResult> asynchronousOperationResultCallback = new TestCallback<>();

            // when
            api.getJobRescalingStatusAsync(jobId, triggerRescalingResponseCallback.result.getRequestId(), asynchronousOperationResultCallback);

            // then
            await(() -> {
                assertThat(asynchronousOperationResultCallback.result).isNotNull();
                assertThat(asynchronousOperationResultCallback.result.getStatus().getId()).isEqualTo(QueueStatus.IdEnum.COMPLETED);
            });
        }

        @Test
        void shouldReturnCheckpointsDetailsAndStatistics() throws ApiException {
            // given
            final String jobId = getRunningJob().getId();
            final JobDetailsInfo jobDetailsInfo = api.getJobDetails(jobId);
            final String vertexId = jobDetailsInfo.getVertices().get(0).getId();

            final TestCallback<CheckpointConfigInfo> checkpointConfigInfoCallback = new TestCallback<>();

            // when
            api.getJobCheckpointsConfigAsync(jobId, checkpointConfigInfoCallback);

            // then
            await(() -> {
                assertThat(checkpointConfigInfoCallback.result).isNotNull();
                assertThat(checkpointConfigInfoCallback.result.getInterval()).isEqualTo(60000L);
                assertThat(checkpointConfigInfoCallback.result.getTimeout()).isEqualTo(600000L);
            });

            Awaitility.await()
                    .pollInterval(20, TimeUnit.SECONDS)
                    .pollDelay(20, TimeUnit.SECONDS)
                    .atMost(120, TimeUnit.SECONDS)
                    .untilAsserted(() -> {
                        // when
                        final CheckpointingStatistics jobCheckpoints = api.getJobCheckpoints(jobId);

                        // then
                        assertThat(jobCheckpoints).isNotNull();
                        dumpAsJson(jobCheckpoints);
                        assertThat(jobCheckpoints.getHistory()).isNotNull();
                        assertThat(jobCheckpoints.getHistory().size() > 0).isTrue();
                    });

            // given
            final TestCallback<CheckpointingStatistics> jobCheckpointsCallback = new TestCallback<>();

            // when
            api.getJobCheckpointsAsync(jobId, jobCheckpointsCallback);

            // then
            await(() -> {
                assertThat(jobCheckpointsCallback.result).isNotNull();
                assertThat(jobCheckpointsCallback.result.getHistory()).isNotNull();
                assertThat(jobCheckpointsCallback.result.getHistory().size() > 0).isTrue();
            });

            // given
            final Integer checkpointId = jobCheckpointsCallback.result.getHistory().get(0).getId();

            final TestCallback<CheckpointStatistics> jobCheckpointDetailsCallback = new TestCallback<>();

            // when
            api.getJobCheckpointDetailsAsync(jobId, checkpointId, jobCheckpointDetailsCallback);

            // then
            await(() -> {
                assertThat(jobCheckpointDetailsCallback.result).isNotNull();
                assertThat(jobCheckpointDetailsCallback.result.getStatus().getValue()).isEqualTo(CheckpointStatistics.StatusEnum.COMPLETED.getValue());
            });

            // given
            final TestCallback<TaskCheckpointStatisticsWithSubtaskDetails> jobCheckpointStatisticsCallback = new TestCallback<>();

            // when
            api.getJobCheckpointStatisticsAsync(jobId, checkpointId, vertexId, jobCheckpointStatisticsCallback);

            // then
            await(() -> {
                assertThat(jobCheckpointStatisticsCallback.result).isNotNull();
                assertThat(jobCheckpointStatisticsCallback.result.getStatus().getValue()).isEqualTo(CheckpointStatistics.StatusEnum.COMPLETED.getValue());
            });
        }
    }

    @Nested
    class JobManager {
        @Test
        void shouldReturnMetrics() throws ApiException {
            // when
            final Object metrics = api.getJobManagerMetrics("Status.JVM.CPU.Time");

            // then
            assertThat(metrics).isNotNull();
            dumpAsJson(metrics);
        }

        @Test
        void shouldReturnConfig() throws ApiException {
            // when
            final List<ClusterConfigurationInfoEntry> jobManagerConfig = api.showJobManagerConfig();

            // then
            assertThat(jobManagerConfig).isNotNull();
            dumpAsJson(jobManagerConfig);
            assertThat(jobManagerConfig.size() > 0).isTrue();
        }
    }

    @Nested
    class JobManagerAsync {
        @Test
        void shouldReturnMetrics() throws ApiException {
            // given
            final TestCallback<Object> metricsCallback = new TestCallback<>();

            // when
            api.getJobManagerMetricsAsync("Status.JVM.CPU.Time", metricsCallback);

            // then
            await(() -> {
                assertThat(metricsCallback.result).isNotNull();
            });
        }

        @Test
        void shouldReturnConfig() throws ApiException {
            // given
            final TestCallback<List<ClusterConfigurationInfoEntry>> jobManagerConfigCallback = new TestCallback<>();

            // when
            api.showJobManagerConfigAsync(jobManagerConfigCallback);

            // then
            await(() -> {
                assertThat(jobManagerConfigCallback.result).isNotNull();
                assertThat(jobManagerConfigCallback.result.size() > 0).isTrue();
            });
        }
    }

    @Nested
    class TaskManager {
        @Test
        void shouldReturnMetrics() throws ApiException {
            // when
            final TaskManagersInfo taskManagersInfo = api.getTaskManagersOverview();

            // then
            assertThat(taskManagersInfo).isNotNull();
            dumpAsJson(taskManagersInfo);
            verifyTaskManagersInfo(taskManagersInfo);

            // given
            final String taskManagerId = taskManagersInfo.getTaskmanagers().get(0).getId();

            // when
            final TaskManagerDetailsInfo taskManagerDetails = api.getTaskManagerDetails(taskManagerId);

            // then
            assertThat(taskManagerDetails).isNotNull();
            dumpAsJson(taskManagerDetails);
            verifyTaskManagerDetailsInfo(taskManagerDetails);

            // when
            final Object metrics = api.getTaskManagerMetrics(taskManagerId, "Status.JVM.CPU.Time");

            // then
            assertThat(metrics).isNotNull();
            dumpAsJson(metrics);

            // when
            final Object aggregatedMetrics = api.getTaskManagerAggregatedMetrics("Status.JVM.CPU.Time", "sum", taskManagerId);

            // then
            assertThat(aggregatedMetrics).isNotNull();
            dumpAsJson(aggregatedMetrics);
        }
    }

    @Nested
    class TaskManagerAsync {
        @Test
        void shouldReturnMetrics() throws ApiException {
            // given
            final TestCallback<TaskManagersInfo> taskManagersInfoCallback = new TestCallback<>();

            // when
            api.getTaskManagersOverviewAsync(taskManagersInfoCallback);

            // then
            await(() -> {
                assertThat(taskManagersInfoCallback.result).isNotNull();
                verifyTaskManagersInfo(taskManagersInfoCallback.result);
            });

            // given
            final String taskManagerId = taskManagersInfoCallback.result.getTaskmanagers().get(0).getId();
            final TestCallback<TaskManagerDetailsInfo> taskManagerDetailsInfoCallback = new TestCallback<>();

            // when
            api.getTaskManagerDetailsAsync(taskManagerId, taskManagerDetailsInfoCallback);

            // then
            await(() -> {
                assertThat(taskManagerDetailsInfoCallback.result).isNotNull();
                verifyTaskManagerDetailsInfo(taskManagerDetailsInfoCallback.result);
            });

            // given
            final TestCallback<Object> metricsCallback = new TestCallback<>();

            // when
            api.getTaskManagerMetricsAsync(taskManagerId, "Status.JVM.CPU.Time", metricsCallback);

            // then
            await(() -> {
                assertThat(metricsCallback.result).isNotNull();
            });

            // given
            final TestCallback<Object> aggregatedMetricsCallback = new TestCallback<>();

            // when
            api.getTaskManagerAggregatedMetricsAsync("Status.JVM.CPU.Time", "sum", taskManagerId, aggregatedMetricsCallback);

            // then
            await(() -> {
                assertThat(aggregatedMetricsCallback.result).isNotNull();
            });
        }
    }
}