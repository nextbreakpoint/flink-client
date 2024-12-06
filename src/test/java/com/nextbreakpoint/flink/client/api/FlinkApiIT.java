/*
 * This file is part of Flink Client
 * https://github.com/nextbreakpoint/flink-client
 */
package com.nextbreakpoint.flink.client.api;

import com.nextbreakpoint.flink.client.model.*;
import com.nextbreakpoint.flink.client.support.Driver;
import com.nextbreakpoint.flink.client.support.SyncDriver;
import com.nextbreakpoint.flink.client.support.AsyncDriver;
import com.nextbreakpoint.flink.client.support.IOUtils;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.nextbreakpoint.flink.client.support.AsyncUtils.await;
import static com.nextbreakpoint.flink.client.support.IOUtils.dumpAsJson;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Tag("slow")
public class FlinkApiIT {
    //TODO find a better way to parameterise the tests
    private static final String BASE_PATH = new File(System.getProperty("base.directory")).getAbsolutePath();
    private static final String JAR_PATH = "/tmp/com.nextbreakpoint.flink.dummies-" + System.getProperty("flink-dummies.version") + "-shaded.jar";
    private static final String ZIP_PATH = "/tmp/com.nextbreakpoint.flink.dummies-" + System.getProperty("flink-dummies.version") + "-shaded.zip";
    private static final String JAR_NAME = "com.nextbreakpoint.flink.dummies-" + System.getProperty("flink-dummies.version") + "-shaded.jar";
    //private static final String JAR_URL = "https://github.com/nextbreakpoint/flink-dummies/releases/download/v" + System.getProperty("flink-dummies.version") + "/com.nextbreakpoint.flink.dummies-" + System.getProperty("flink-dummies.version") + ".jar";
    private static final String JAR_URL = "file://" + BASE_PATH + "/target/downloaded/" + JAR_NAME;
    private static final String ENTRY_CLASS = "com.nextbreakpoint.flink.dummies.TemperatureMeasurementMain";
    private static final String CONFIG_FILE = "file:///var/docker/measurement.properties";
    private static final String FLINK_VERSION = System.getProperty("flink.version");
    private static final String SERVER_URL = "http://localhost:8081";

    private static Driver defaultDriver;

    public static Stream<Arguments> drivers() {
        final FlinkApi api = new FlinkApi();
        api.getApiClient().setBasePath(SERVER_URL);
        return Stream.of(
                Arguments.arguments(Named.of("Synchronous API", new SyncDriver(api))),
                Arguments.arguments(Named.of("Asynchronous API", new AsyncDriver(api)))
        );
    }

    @BeforeAll
    static void setup() {
        final FlinkApi api = new FlinkApi();
        api.getApiClient().setBasePath(SERVER_URL);
        IOUtils.copyFile(JAR_URL, JAR_PATH);
        defaultDriver = new SyncDriver(api);
    }

    @Nested
    class Dashboard {
        @ParameterizedTest(name = "{index}) {0}")
        @MethodSource("com.nextbreakpoint.flink.client.api.FlinkApiIT#drivers")
        void shouldGetDashboardConfiguration(Driver driver) throws ApiException {
            // when
            final DashboardConfiguration config = driver.getDashboardConfiguration();

            // then
            assertThat(config).isNotNull();
            dumpAsJson(config);
            TestAssertions.verifyDashboardConfiguration(config);
        }
    }

    @Nested
    class Cluster {
        @ParameterizedTest(name = "{index}) {0}")
        @MethodSource("com.nextbreakpoint.flink.client.api.FlinkApiIT#drivers")
        void shouldReturnOverview(Driver driver) throws ApiException {
            // when
            final ClusterOverviewWithVersion clusterOverviewWithVersion = driver.getClusterOverview();

            // then
            assertThat(clusterOverviewWithVersion).isNotNull();
            dumpAsJson(clusterOverviewWithVersion);
            TestAssertions.verifyClusterOverviewWithVersion(clusterOverviewWithVersion);
        }
    }

    @Nested
    class Jars {
        @ParameterizedTest(name = "{index}) {0}")
        @MethodSource("com.nextbreakpoint.flink.client.api.FlinkApiIT#drivers")
        void shouldUploadJar(Driver driver) throws ApiException {
            // when
            final JarUploadResponseBody jarUploadResponseBody = driver.uploadJar(new File(JAR_PATH));

            // then
            assertThat(jarUploadResponseBody).isNotNull();
            dumpAsJson(jarUploadResponseBody);
            TestAssertions.verifyJarUploadResponseBody(jarUploadResponseBody);
        }

        @ParameterizedTest(name = "{index}) {0}")
        @MethodSource("com.nextbreakpoint.flink.client.api.FlinkApiIT#drivers")
        void shouldThrowWhenUploadingNonExistentFile(Driver driver) {
            assertThatThrownBy(() -> driver.uploadJar(new File(ZIP_PATH))).isInstanceOf(ApiException.class);
        }

        @ParameterizedTest(name = "{index}) {0}")
        @MethodSource("com.nextbreakpoint.flink.client.api.FlinkApiIT#drivers")
        void shouldListJar(Driver driver) throws ApiException {
            // given
            final JarUploadResponseBody jarUploadResponseBody = driver.uploadJar(new File(JAR_PATH));
            dumpAsJson(jarUploadResponseBody);

            // when
            final JarListInfo jarListInfo = driver.getJarList();

            // then
            assertThat(jarListInfo).isNotNull();
            dumpAsJson(jarListInfo);
            TestAssertions.verifyJarListInfo(jarUploadResponseBody, jarListInfo);
        }

        @ParameterizedTest(name = "{index}) {0}")
        @MethodSource("com.nextbreakpoint.flink.client.api.FlinkApiIT#drivers")
        void shouldDeleteJar(Driver driver) throws ApiException {
            // given
            final JarUploadResponseBody jarUploadResponseBody = driver.uploadJar(new File(JAR_PATH));
            dumpAsJson(jarUploadResponseBody);
            final JarFileInfo jarFileInfo = driver.getJarList().getFiles().get(0);

            // when
            driver.deleteJar(jarFileInfo.getId());

            // then
            final JarListInfo jarListInfo = driver.getJarList();
            dumpAsJson(jarListInfo);
            TestAssertions.verifyJarDeleted(jarListInfo, jarFileInfo.getId());
        }

        @ParameterizedTest(name = "{index}) {0}")
        @MethodSource("com.nextbreakpoint.flink.client.api.FlinkApiIT#drivers")
        void shouldReturnPlan(Driver driver) throws ApiException {
            // given
            final JarUploadResponseBody jarUploadResponseBody = driver.uploadJar(new File(JAR_PATH));
            dumpAsJson(jarUploadResponseBody);
            final JarFileInfo jarFileInfo = driver.getJarList().getFiles().get(0);

            // when
            final String jarId = jarFileInfo.getId();
            final JarPlanRequestBody jarPlanRequestBody = new JarPlanRequestBody();
            jarPlanRequestBody.entryClass(ENTRY_CLASS);
            jarPlanRequestBody.parallelism(1);
            jarPlanRequestBody.addProgramArgsListItem("--CONFIG_FILE");
            jarPlanRequestBody.addProgramArgsListItem(CONFIG_FILE);
            final JobPlanInfo jobPlanInfo = driver.generatePlanFromJar(jarId, jarPlanRequestBody, null, null, null, null);

            // then
            assertThat(jobPlanInfo).isNotNull();
            dumpAsJson(jobPlanInfo);
            assertThat(jobPlanInfo.getPlan()).isNotNull();
        }
    }

    @Nested
    class Jobs {
        @AfterEach
        void terminateJobs() throws ApiException {
            TestActions.cancelAllJobs(defaultDriver);
        }

        @BeforeEach
        void cancelJobs() throws ApiException {
            TestActions.cancelAllJobs(defaultDriver);
            TestActions.runTestJar(defaultDriver);
        }

        @ParameterizedTest(name = "{index}) {0}")
        @MethodSource("com.nextbreakpoint.flink.client.api.FlinkApiIT#drivers")
        void shouldReturnJobsDetailsAndOthers(Driver driver) throws ApiException {
            // when
            final JobIdsWithStatusOverview jobIdsWithStatusOverview = driver.getJobIdsWithStatusesOverview();

            // then
            assertThat(jobIdsWithStatusOverview).isNotNull();
            dumpAsJson(jobIdsWithStatusOverview);
            TestAssertions.verifyJobIdWithStatusOverview(jobIdsWithStatusOverview);

            // when
            final MultipleJobsDetails multipleJobsDetails = driver.getJobsOverview();

            // then
            assertThat(multipleJobsDetails).isNotNull();
            dumpAsJson(multipleJobsDetails);
            TestAssertions.verifyMultipleJobsDetails(multipleJobsDetails);

            // when
            final AggregatedMetricsResponseBody jobsMetrics = driver.getAggregatedJobMetrics("numberOfFailedCheckpoints", AggregationMode.SUM, null);

            // then
            assertThat(jobsMetrics).isNotNull();
            dumpAsJson(jobsMetrics);
        }

        @ParameterizedTest(name = "{index}) {0}")
        @MethodSource("com.nextbreakpoint.flink.client.api.FlinkApiIT#drivers")
        void shouldReturnJobDetailsAndOthers(Driver driver) throws ApiException {
            // given
            final String jobId = TestActions.getRunningJob(driver).getId();

            // when
            final JobConfigInfo jobConfig = driver.getJobConfig(jobId);

            // then
            assertThat(jobConfig).isNotNull();
            dumpAsJson(jobConfig);

            // when
            final JobPlanInfo jobPlan = driver.getJobPlan(jobId);

            // then
            assertThat(jobPlan).isNotNull();
            dumpAsJson(jobPlan);
            assertThat(jobPlan.getPlan()).isNotNull();

            // when
            final MetricCollectionResponseBody jobMetrics = driver.getJobMetrics(jobId, "numberOfFailedCheckpoints");

            // then
            assertThat(jobMetrics).isNotNull();
            dumpAsJson(jobMetrics);

            // when
            final JobDetailsInfo jobDetailsInfo = driver.getJobDetails(jobId);

            // then
            assertThat(jobDetailsInfo).isNotNull();
            dumpAsJson(jobDetailsInfo);
            assertThat(jobDetailsInfo.getPlan()).isNotNull();
            assertThat(jobDetailsInfo.getJid()).isEqualTo(jobId);
            assertThat(jobDetailsInfo.getVertices()).hasSize(8);

            // when
            final JobExecutionResultResponseBody jobExecutionResultResponseBody = driver.getJobExecutionResult(jobId);

            // then
            assertThat(jobExecutionResultResponseBody).isNotNull();
            dumpAsJson(jobExecutionResultResponseBody);
            assertThat(jobExecutionResultResponseBody.getStatus().getId()).isEqualTo(Id.IN_PROGRESS);
            assertThat(jobExecutionResultResponseBody.getJobExecutionResult()).isNull();

            // when
            final JobExceptionsInfoWithHistory jobExceptionsInfo = driver.getJobExceptions(jobId, null, null);

            // then
            assertThat(jobExceptionsInfo).isNotNull();
            dumpAsJson(jobExceptionsInfo);
            assertThat(jobExceptionsInfo.getExceptionHistory()).isNotNull();
            assertThat(jobExceptionsInfo.getExceptionHistory().getEntries()).isNotNull();

            // when
            final JobAccumulatorsInfo jobAccumulatorsInfo = driver.getJobAccumulators(jobId, true);

            // then
            assertThat(jobAccumulatorsInfo).isNotNull();
            dumpAsJson(jobAccumulatorsInfo);
            assertThat(jobAccumulatorsInfo.getJobAccumulators()).isNotNull();

            // when
            final CheckpointingStatistics checkpointingStatistics = driver.getCheckpointingStatistics(jobId);

            // then
            assertThat(checkpointingStatistics).isNotNull();
//            dumpAsJson(checkpointingStatistics);
            assertThat(checkpointingStatistics.getCounts()).isNotNull();
        }

        @ParameterizedTest(name = "{index}) {0}")
        @MethodSource("com.nextbreakpoint.flink.client.api.FlinkApiIT#drivers")
        void shouldReturnJobTaskDetailsAndOthers(Driver driver) throws ApiException {
            // given
            final String jobId = TestActions.getRunningJob(driver).getId();
            final JobDetailsInfo jobDetailsInfo = driver.getJobDetails(jobId);
            final String vertexId = jobDetailsInfo.getVertices().get(0).getId();

            // when
            final MetricCollectionResponseBody jobTaskMetrics = driver.getJobVertexMetrics(jobId, vertexId, "numRecordsIn");

            // then
            assertThat(jobTaskMetrics).isNotNull();
            dumpAsJson(jobTaskMetrics);

            // when
            final JobVertexBackPressureInfo jobTaskBackpressure = driver.getJobVertexBackPressure(jobId, vertexId);

            // then
            assertThat(jobTaskBackpressure).isNotNull();
            dumpAsJson(jobTaskBackpressure);
            assertThat(jobTaskBackpressure.getStatus()).isNotNull();

            // when
            final JobVertexDetailsInfo jobVertexDetailsInfo = driver.getJobVertexDetails(jobId, vertexId);

            // then
            assertThat(jobVertexDetailsInfo).isNotNull();
            dumpAsJson(jobVertexDetailsInfo);
            assertThat(jobVertexDetailsInfo.getSubtasks()).hasSize(1);

            // when
            final JobVertexAccumulatorsInfo jobVertexAccumulatorsInfo = driver.getJobVertexAccumulators(jobId, vertexId);

            // then
            assertThat(jobVertexAccumulatorsInfo).isNotNull();
            dumpAsJson(jobVertexAccumulatorsInfo);
            assertThat(jobVertexAccumulatorsInfo.getUserAccumulators()).isNotNull();

            // when
            final JobVertexTaskManagersInfo jobVertexTaskManagersInfo = driver.getJobVertexTaskManagers(jobId, vertexId);

            // then
            assertThat(jobVertexTaskManagersInfo).isNotNull();
            dumpAsJson(jobVertexTaskManagersInfo);
            assertThat(jobVertexTaskManagersInfo.getTaskmanagers()).hasSize(1);
        }

        @ParameterizedTest(name = "{index}) {0}")
        @MethodSource("com.nextbreakpoint.flink.client.api.FlinkApiIT#drivers")
        void shouldReturnJobSubtaskDetailsAndOthers(Driver driver) throws ApiException {
            // given
            final String jobId = TestActions.getRunningJob(driver).getId();
            final JobDetailsInfo jobDetailsInfo = driver.getJobDetails(jobId);
            final String vertexId = jobDetailsInfo.getVertices().get(0).getId();

            Awaitility.await()
                    .pollInterval(1, TimeUnit.SECONDS)
                    .pollDelay(5, TimeUnit.SECONDS)
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(() -> {
                        // when
                        final SubtaskExecutionAttemptDetailsInfo subtaskExecutionAttemptDetailsInfo = driver.getSubtaskCurrentAttemptDetails(jobId, vertexId, 0);

                        // then
                        assertThat(subtaskExecutionAttemptDetailsInfo).isNotNull();
                        dumpAsJson(subtaskExecutionAttemptDetailsInfo);
                        assertThat(subtaskExecutionAttemptDetailsInfo.getStatus()).isEqualTo(ExecutionState.RUNNING);
                    });

            // when
            final MetricCollectionResponseBody jobSubtaskMetrics = driver.getSubtaskMetrics(jobId, vertexId, 0, "numRecordsIn");

            // then
            assertThat(jobSubtaskMetrics).isNotNull();
            dumpAsJson(jobSubtaskMetrics);

            // when
            final AggregatedMetricsResponseBody jobAggregatedSubtaskMetrics = driver.getAggregatedSubtaskMetrics(jobId, vertexId, "numRecordsIn", AggregationMode.SUM, null);

            // then
            assertThat(jobAggregatedSubtaskMetrics).isNotNull();
            dumpAsJson(jobAggregatedSubtaskMetrics);

            // when
            final SubtasksTimesInfo subtasksTimesInfo = driver.getSubtasksTimes(jobId, vertexId);

            // then
            assertThat(subtasksTimesInfo).isNotNull();
            dumpAsJson(subtasksTimesInfo);
            assertThat(subtasksTimesInfo.getSubtasks()).hasSize(1);

            // when
            final SubtasksAllAccumulatorsInfo subtasksAllAccumulatorsInfo = driver.getSubtasksAllAccumulators(jobId, vertexId);

            // then
            assertThat(subtasksAllAccumulatorsInfo).isNotNull();
            dumpAsJson(subtasksAllAccumulatorsInfo);
            assertThat(subtasksAllAccumulatorsInfo.getSubtasks()).hasSize(1);

//            // when
//            final SubtaskExecutionAttemptAccumulatorsInfo subtaskExecutionAttemptAccumulatorsInfo = driver.getSubtaskExecutionAttemptAccumulators(jobId, vertexId, 0, 0);
//
//            // then
//            assertThat(subtaskExecutionAttemptAccumulatorsInfo).isNotNull();
//            dumpAsJson(subtaskExecutionAttemptAccumulatorsInfo);
//            assertThat(subtaskExecutionAttemptAccumulatorsInfo.getUserAccumulators()).isNotNull();
//
//            // when
//            final SubtaskExecutionAttemptDetailsInfo subtaskExecutionAttemptDetailsInfo = driver.getSubtaskCurrentAttemptDetails(jobId, vertexId, 0);
//
//            // then
//            assertThat(subtaskExecutionAttemptDetailsInfo).isNotNull();
//            dumpAsJson(subtaskExecutionAttemptDetailsInfo);
//            assertThat(subtaskExecutionAttemptDetailsInfo.getStatus()).isEqualTo(ExecutionState.RUNNING);
        }

        @ParameterizedTest(name = "{index}) {0}")
        @MethodSource("com.nextbreakpoint.flink.client.api.FlinkApiIT#drivers")
        void shouldCreateSavepoint(Driver driver) throws ApiException {
            // given
            final String jobId = TestActions.getRunningJob(driver).getId();
            final JobDetailsInfo jobDetailsInfo = driver.getJobDetails(jobId);
            final String vertexId = jobDetailsInfo.getVertices().get(0).getId();

            Awaitility.await()
                    .pollInterval(1, TimeUnit.SECONDS)
                    .pollDelay(5, TimeUnit.SECONDS)
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(() -> {
                        // when
                        final SubtaskExecutionAttemptDetailsInfo subtaskExecutionAttemptDetailsInfo = driver.getSubtaskCurrentAttemptDetails(jobId, vertexId, 0);

                        // then
                        assertThat(subtaskExecutionAttemptDetailsInfo).isNotNull();
                        dumpAsJson(subtaskExecutionAttemptDetailsInfo);
                        assertThat(subtaskExecutionAttemptDetailsInfo.getStatus()).isEqualTo(ExecutionState.RUNNING);
                    });

            // when
            final TriggerResponse triggerSavepointResponse = driver.triggerSavepoint(jobId, new SavepointTriggerRequestBody().cancelJob(false).targetDirectory("file:///var/tmp"));

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
                        final AsynchronousOperationResult asynchronousOperationResult = driver.getSavepointStatus(jobId, triggerSavepointResponse.getRequestId());

                        // then
                        assertThat(asynchronousOperationResult).isNotNull();
                        dumpAsJson(asynchronousOperationResult);
                        assertThat(asynchronousOperationResult.getStatus().getId()).isEqualTo(Id.COMPLETED);
                    });

            Awaitility.await()
                    .pollInterval(1, TimeUnit.SECONDS)
                    .pollDelay(5, TimeUnit.SECONDS)
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(() -> {
                        final CheckpointingStatistics checkpointingStatistics = driver.getCheckpointingStatistics(jobId);
                        assertThat(checkpointingStatistics.getLatest()).isNotNull();
                        assertThat(checkpointingStatistics.getLatest().getSavepoint()).isNotNull();
                    });

            // given
            final CheckpointingStatistics checkpointingStatistics = driver.getCheckpointingStatistics(jobId);
            final String savepointPath = checkpointingStatistics.getLatest().getSavepoint().getExternalPath();

            // when
            final TriggerResponse triggerSavepointDisposalResponse = driver.triggerSavepointDisposal(new SavepointDisposalRequest().savepointPath(savepointPath));

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
                        final AsynchronousOperationResult asynchronousOperationResult = driver.getSavepointDisposalStatus(triggerSavepointDisposalResponse.getRequestId());

                        // then
                        assertThat(asynchronousOperationResult).isNotNull();
                        dumpAsJson(asynchronousOperationResult);
                        assertThat(asynchronousOperationResult.getStatus().getId()).isEqualTo(Id.COMPLETED);
                    });
        }

        @ParameterizedTest(name = "{index}) {0}")
        @MethodSource("com.nextbreakpoint.flink.client.api.FlinkApiIT#drivers")
        @Disabled
        void shouldRescaleJob(Driver driver) throws ApiException {
            // given
            final String jobId = TestActions.getRunningJob(driver).getId();

            // when
//            final StopWithSavepointRequestBody savepointRequestBody = new StopWithSavepointRequestBody();
//            savepointRequestBody.drain(false);
//            savepointRequestBody.targetDirectory("/tmp");
//            savepointRequestBody.formatType(SavepointFormatType.CANONICAL);
//            final TriggerResponse triggerSavepointResponse = api.triggerStopWithSavepoint(jobId, savepointRequestBody);
//
//            Awaitility.await()
//                    .pollInterval(1, TimeUnit.SECONDS)
//                    .pollDelay(5, TimeUnit.SECONDS)
//                    .atMost(30, TimeUnit.SECONDS)
//                    .untilAsserted(() -> {
//                        // when
//                        final AsynchronousOperationResult asynchronousOperationResult = driver.getSavepointStatus(jobId, triggerSavepointResponse.getRequestId());
//
//                        // then
//                        assertThat(asynchronousOperationResult).isNotNull();
//                        dumpAsJson(asynchronousOperationResult);
//                        assertThat(asynchronousOperationResult.getStatus().getId()).isEqualTo(Id.COMPLETED);
//                    });

            final TriggerResponse triggerRescalingResponse = driver.rescaleJob(jobId, 2);

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
                        final AsynchronousOperationResult asynchronousOperationResult = driver.getRescalingStatus(jobId, triggerRescalingResponse.getRequestId());

                        // then
                        assertThat(asynchronousOperationResult).isNotNull();
                        dumpAsJson(asynchronousOperationResult);
                        assertThat(asynchronousOperationResult.getStatus().getId()).isEqualTo(Id.COMPLETED);
                    });
        }

        @ParameterizedTest(name = "{index}) {0}")
        @MethodSource("com.nextbreakpoint.flink.client.api.FlinkApiIT#drivers")
        void shouldReturnCheckpointsDetailsAndStatistics(Driver driver) throws ApiException {
            // given
            final String jobId = TestActions.getRunningJob(driver).getId();
            final JobDetailsInfo jobDetailsInfo = driver.getJobDetails(jobId);
            final String vertexId = jobDetailsInfo.getVertices().get(0).getId();

            // when
            final CheckpointConfigInfo checkpointConfigInfo = driver.getCheckpointConfig(jobId);

            // then
            assertThat(checkpointConfigInfo).isNotNull();
            dumpAsJson(checkpointConfigInfo);
            assertThat(checkpointConfigInfo.getInterval()).isEqualTo(10000L);
            assertThat(checkpointConfigInfo.getTimeout()).isEqualTo(600000L);

            Awaitility.await()
                    .pollInterval(1, TimeUnit.SECONDS)
                    .pollDelay(5, TimeUnit.SECONDS)
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(() -> {
                        // when
                        final CheckpointingStatistics jobCheckpoints = driver.getCheckpointingStatistics(jobId);

                        // then
                        assertThat(jobCheckpoints).isNotNull();
//                        dumpAsJson(jobCheckpoints);
                        assertThat(jobCheckpoints.getHistory()).isNotNull();
                        assertThat(jobCheckpoints.getHistory()).isNotEmpty();
                    });

            Awaitility.await()
                    .pollInterval(1, TimeUnit.SECONDS)
                    .pollDelay(5, TimeUnit.SECONDS)
                    .atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(() -> {
                        // given
                        final CheckpointingStatistics jobCheckpoints = driver.getCheckpointingStatistics(jobId);
                        final Long checkpointId = jobCheckpoints.getHistory().get(0).getId();

                        // when
                        final CheckpointStatistics jobCheckpointDetails = driver.getCheckpointStatisticDetails(jobId, checkpointId);

                        // then
                        assertThat(jobCheckpointDetails).isNotNull();
                        dumpAsJson(jobCheckpointDetails);
                        assertThat(jobCheckpointDetails.getStatus().getValue()).isEqualTo(CheckpointStatsStatus.COMPLETED.getValue());
                    });

            // given
            final CheckpointingStatistics jobCheckpoints = driver.getCheckpointingStatistics(jobId);
            final Long checkpointId = jobCheckpoints.getHistory().get(0).getId();

            // when
            final TaskCheckpointStatisticsWithSubtaskDetails jobCheckpointStatistics = driver.getTaskCheckpointStatistics(jobId, checkpointId, vertexId);

            // then
            assertThat(jobCheckpointStatistics).isNotNull();
//            dumpAsJson(jobCheckpointStatistics);
            assertThat(jobCheckpointStatistics.getStatus().getValue()).isEqualTo(CheckpointStatsStatus.COMPLETED.getValue());
        }
    }

    @Nested
    class JobManager {
        @ParameterizedTest(name = "{index}) {0}")
        @MethodSource("com.nextbreakpoint.flink.client.api.FlinkApiIT#drivers")
        void shouldReturnMetrics(Driver driver) throws ApiException {
            // when
            final MetricCollectionResponseBody metrics = driver.getJobManagerMetrics("Status.JVM.CPU.Time");

            // then
            assertThat(metrics).isNotNull();
            dumpAsJson(metrics);
        }

        @ParameterizedTest(name = "{index}) {0}")
        @MethodSource("com.nextbreakpoint.flink.client.api.FlinkApiIT#drivers")
        void shouldReturnConfig(Driver driver) throws ApiException {
            // when
            final List<ConfigurationInfoEntry> jobManagerConfig = driver.getClusterConfigurationInfo();

            // then
            assertThat(jobManagerConfig).isNotNull();
            dumpAsJson(jobManagerConfig);
            assertThat(jobManagerConfig).isNotEmpty();
        }
    }

    @Nested
    class TaskManager {
        @ParameterizedTest(name = "{index}) {0}")
        @MethodSource("com.nextbreakpoint.flink.client.api.FlinkApiIT#drivers")
        void shouldReturnMetrics(Driver driver) throws ApiException {
            // when
            final TaskManagersInfo taskManagersInfo = driver.getTaskManagers();

            // then
            assertThat(taskManagersInfo).isNotNull();
            dumpAsJson(taskManagersInfo);
            TestAssertions.verifyTaskManagersInfo(taskManagersInfo);

            // given
            final String taskManagerId = taskManagersInfo.getTaskmanagers().get(0).getId();

            // when
            final TaskManagerDetailsInfo taskManagerDetails = driver.getTaskManagerDetails(taskManagerId);

            // then
            assertThat(taskManagerDetails).isNotNull();
            dumpAsJson(taskManagerDetails);
            TestAssertions.verifyTaskManagerDetailsInfo(taskManagerDetails);

            // when
            final MetricCollectionResponseBody metrics = driver.getTaskManagerMetrics(taskManagerId, "Status.JVM.CPU.Time");

            // then
            assertThat(metrics).isNotNull();
            dumpAsJson(metrics);

            // when
            final AggregatedMetricsResponseBody aggregatedMetrics = driver.getAggregatedTaskManagerMetrics("Status.JVM.CPU.Time", AggregationMode.SUM, taskManagerId);

            // then
            assertThat(aggregatedMetrics).isNotNull();
            dumpAsJson(aggregatedMetrics);
        }
    }

    private static class TestActions {
        public static void runTestJar(Driver driver) throws ApiException {
            final JarUploadResponseBody result = driver.uploadJar(new File(JAR_PATH));
            assertThat(result).isNotNull();
            dumpAsJson(result);

            final JarListInfo jarListInfo = driver.getJarList();
            assertThat(jarListInfo).isNotNull();
            dumpAsJson(jarListInfo);

            final String jarId = jarListInfo.getFiles().get(0).getId();
            final JarRunRequestBody jarRunRequestBody = new JarRunRequestBody();
            jarRunRequestBody.entryClass(ENTRY_CLASS);
            jarRunRequestBody.parallelism(1);
            jarRunRequestBody.addProgramArgsListItem("--CONFIG_FILE");
            jarRunRequestBody.addProgramArgsListItem(CONFIG_FILE);
            final JarRunResponseBody response = driver.submitJobFromJar(jarId, jarRunRequestBody, true, null, null, null, null, null);
            assertThat(response).isNotNull();
            dumpAsJson(response);

            assertThat(response.getJobid()).isNotNull();
        }

        public static void cancelAllJobs(Driver driver) throws ApiException {
            final JobIdsWithStatusOverview statusOverview = driver.getJobIdsWithStatusesOverview();
            assertThat(statusOverview).isNotNull();
            dumpAsJson(statusOverview);

            statusOverview.getJobs().forEach(jobIdWithStatus -> {
                try {
                    driver.cancelJob(jobIdWithStatus.getId(), TerminationMode.CANCEL);
                } catch (ApiException ignored) {
                }
            });
        }

        public static JobIdWithStatus getRunningJob(Driver driver) throws ApiException {
            await(() -> {
                final JobIdsWithStatusOverview jobIdsWithStatusOverview = driver.getJobIdsWithStatusesOverview();
                final List<JobIdWithStatus> jobs = listRunningJobs(jobIdsWithStatusOverview);
                assertThat(jobs).hasSize(1);
            });
            final JobIdsWithStatusOverview jobIdsWithStatusOverview = driver.getJobIdsWithStatusesOverview();
            dumpAsJson(jobIdsWithStatusOverview);
            final List<JobIdWithStatus> jobs = listRunningJobs(jobIdsWithStatusOverview);
            assertThat(jobs).hasSize(1);
            return jobs.get(0);
        }

        public static List<JobIdWithStatus> listRunningJobs(JobIdsWithStatusOverview jobIdsWithStatusOverview) {
            return jobIdsWithStatusOverview.getJobs()
                    .stream()
                    .filter(jobIdWithStatus -> jobIdWithStatus.getStatus().getValue().equals("RUNNING"))
                    .collect(Collectors.toList());
        }
    }

    private static class TestAssertions {
        public static void verifyDashboardConfiguration(DashboardConfiguration dashboardConfiguration) {
            assertThat(dashboardConfiguration.getRefreshInterval()).isEqualTo(3000);
            assertThat(dashboardConfiguration.getTimezoneName()).isNotBlank();
            assertThat(dashboardConfiguration.getTimezoneOffset()).isNotNull();
            assertThat(dashboardConfiguration.getFlinkVersion()).isEqualTo(FLINK_VERSION);
            assertThat(dashboardConfiguration.getFlinkRevision()).isNotBlank();
        }

        public static void verifyJarUploadResponseBody(JarUploadResponseBody jarUploadResponseBody) {
            assertThat(jarUploadResponseBody.getStatus()).isEqualTo(UploadStatus.SUCCESS);
            assertThat(jarUploadResponseBody.getFilename()).endsWith("_" + JAR_NAME);
            assertThat(jarUploadResponseBody.getFilename()).contains("/flink-web-upload");
        }

        public static void verifyJarListInfo(JarUploadResponseBody jarUploadResponseBody, JarListInfo jarListInfo) {
            assertThat(jarListInfo.getFiles()).isNotEmpty();
            IntStream.range(0, jarListInfo.getFiles().size()).forEach((index) -> {
                assertThat(jarListInfo.getFiles().get(index).getId()).endsWith("_" + JAR_NAME);
                assertThat(jarListInfo.getFiles().get(index).getName()).isEqualTo(JAR_NAME);
            });
            assertThat(jarListInfo.getFiles().stream().anyMatch(file -> jarUploadResponseBody.getFilename().endsWith("/" + file.getId()))).isTrue();
        }

        public static void verifyJarDeleted(JarListInfo jarListInfo, String fileInfoId) {
            assertThat(jarListInfo.getFiles().stream().filter(fileInfo -> fileInfo.getId().equals(fileInfoId)).count()).isEqualTo(0);
        }

        public static void verifyTaskManagerDetailsInfo(TaskManagerDetailsInfo taskManagerDetails) {
            assertThat(taskManagerDetails.getMetrics()).isNotNull();
            assertThat(taskManagerDetails.getHardware().getFreeMemory()).isGreaterThan(0);
        }

        public static void verifyTaskManagersInfo(TaskManagersInfo taskManagerInfo) {
            assertThat(taskManagerInfo.getTaskmanagers()).isNotNull();
            assertThat(taskManagerInfo.getTaskmanagers()).hasSize(1);
        }

        public static void verifyMultipleJobsDetails(MultipleJobsDetails multipleJobsDetails) {
            assertThat(multipleJobsDetails.getJobs()).isNotEmpty();
        }

        public static void verifyJobIdWithStatusOverview(JobIdsWithStatusOverview jobIdsWithStatusOverview) {
            assertThat(jobIdsWithStatusOverview.getJobs()).isNotEmpty();
        }

        public static void verifyClusterOverviewWithVersion(ClusterOverviewWithVersion clusterOverviewWithVersion) {
            assertThat(clusterOverviewWithVersion.getFlinkVersion()).isEqualTo(FLINK_VERSION);
        }
    }
}