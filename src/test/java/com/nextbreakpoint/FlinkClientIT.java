package com.nextbreakpoint;

import com.google.gson.Gson;
import com.nextbreakpoint.flinkclient.api.ApiException;
import com.nextbreakpoint.flinkclient.api.FlinkApi;
import com.nextbreakpoint.flinkclient.model.*;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(JUnitPlatform.class)
@Tag("slow")
public class FlinkClientIT {
    private static final String JAR_PATH = "/Users/andrea/Documents/projects/opensource/flink-workshop/flink/com.nextbreakpoint.flinkworkshop/target/com.nextbreakpoint.flinkworkshop-1.0.1.jar";
    private static final String ZIP_PATH = "/Users/andrea/Documents/projects/opensource/flink-workshop/flink/com.nextbreakpoint.flinkworkshop/target/com.nextbreakpoint.flinkworkshop-1.0.1.zip";

    private FlinkApi api;

    @BeforeEach
    public void setup() {
        api = new FlinkApi();
        api.getApiClient().setBasePath("http://localhost:8081");
    }

    @Nested
    class Config {
        @Test
        public void shouldShowConfig() throws ApiException {
            // when
            DashboardConfiguration config = api.showConfig();

            // then
            System.out.println(new Gson().toJson(config));
            assertThat(config.getRefreshInterval()).isEqualTo(3000);
            assertThat(config.getTimezoneName()).isNotBlank();
            assertThat(config.getTimezoneOffset()).isNotNull();
            assertThat(config.getFlinkVersion()).isEqualTo("1.7.2");
            assertThat(config.getFlinkRevision()).isEqualTo("ceba8af @ 11.02.2019 @ 14:17:09 UTC");
        }
    }

    @Nested
    class Shutdown {
        @Test
        public void shouldShutdownCluster() throws ApiException {
            //api.shutdownCluster();
        }
    }

    @Nested
    class Jars {
        @Test
        public void shouldUploadJar() throws ApiException {
            // when
            File jarFile = new File(JAR_PATH);
            JarUploadResponseBody result = api.uploadJar(jarFile);

            // then
            assertThat(result).isNotNull();
            System.out.println(new Gson().toJson(result));
            assertThat(result.getStatus()).isEqualTo(JarUploadResponseBody.StatusEnum.SUCCESS);
            assertThat(result.getFilename()).endsWith("_com.nextbreakpoint.flinkworkshop-1.0.1.jar");
            assertThat(result.getFilename()).contains("/flink-web-upload");
        }

        @Test
        public void shouldThrowWhenUploadingNonExistentFile() {
            // when
            File jarFile = new File(ZIP_PATH);

            // then
            assertThatThrownBy(() -> api.uploadJar(jarFile)).isInstanceOf(ApiException.class);
        }

        @Test
        public void shouldListJar() throws ApiException {
            // given
            File jarFile = new File(JAR_PATH);
            JarUploadResponseBody result = api.uploadJar(jarFile);
            System.out.println(new Gson().toJson(result));

            // when
            JarListInfo jarListInfo = api.listJars();

            // then
            assertThat(jarListInfo).isNotNull();
            System.out.println(new Gson().toJson(jarListInfo));
            assertThat(jarListInfo.getFiles().size() > 0).isTrue();
            IntStream.range(0, jarListInfo.getFiles().size()).forEach((index) -> {
                assertThat(jarListInfo.getFiles().get(index).getId()).endsWith("_com.nextbreakpoint.flinkworkshop-1.0.1.jar");
                assertThat(jarListInfo.getFiles().get(index).getName()).isEqualTo("com.nextbreakpoint.flinkworkshop-1.0.1.jar");
            });
            assertThat(jarListInfo.getFiles().stream().anyMatch(file -> result.getFilename().endsWith("/" + file.getId()))).isTrue();
        }

        @Test
        public void shouldDeleteJar() throws ApiException {
            // given
            File jarFile = new File(JAR_PATH);
            JarUploadResponseBody result = api.uploadJar(jarFile);
            System.out.println(new Gson().toJson(result));
            JarFileInfo jarFileInfo = api.listJars().getFiles().get(0);

            // when
            api.deleteJar(jarFileInfo.getId());

            // then
            JarListInfo jarListInfo = api.listJars();
            System.out.println(new Gson().toJson(jarListInfo));
            assertThat(jarListInfo.getFiles().stream().filter(it -> it.getId().equals(jarFileInfo.getId())).count()).isEqualTo(0);
        }
    }

    @Nested
    class Jobs {
        @BeforeEach
        public void terminateJobs() throws ApiException {
            JobIdsWithStatusOverview statusOverview = api.getJobs();
            assertThat(statusOverview).isNotNull();
            System.out.println(new Gson().toJson(statusOverview));
            statusOverview.getJobs().forEach(jobIdWithStatus -> {
                try {
                    api.terminateJob(jobIdWithStatus.getId(), "cancel");
                } catch (ApiException e) {
                    e.printStackTrace();
                }
            });
        }

        @Test
        public void shouldRunJob() throws ApiException {
            // given
            File jarFile = new File(JAR_PATH);
            JarUploadResponseBody result = api.uploadJar(jarFile);
            System.out.println(new Gson().toJson(result));
            JarListInfo jarListInfo = api.listJars();
            System.out.println(new Gson().toJson(jarListInfo));
            JarRunResponseBody response = api.runJar(jarListInfo.getFiles().get(0).getId(), true, null, "--BUCKET_BASE_PATH file:///var/tmp", null, "com.nextbreakpoint.flink.jobs.TestJob", null);
            System.out.println(new Gson().toJson(response));

            // when
            JobIdsWithStatusOverview statusOverview = api.getJobs();

            // then
            assertThat(statusOverview).isNotNull();
            System.out.println(new Gson().toJson(statusOverview));

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
            System.out.println(new Gson().toJson(multipleJobsDetails));
            assertThat(multipleJobsDetails.getJobs().size() > 0).isTrue();

            // and when
            Object jobsMetrics = api.getJobsMetrics("numberOfFailedCheckpoints", "sum", null);

            // then
            assertThat(jobsMetrics).isNotNull();
            System.out.println(new Gson().toJson(jobsMetrics));

            // and when
            String jobConfig = api.getJobConfig(jobId);

            // then
            assertThat(jobConfig).isNotNull();
            System.out.println(jobConfig);

            // and when
            JobPlanInfo jobPlan = api.getJobPlan(jobId);

            // then
            assertThat(jobPlan).isNotNull();
            System.out.println(new Gson().toJson(jobPlan));
            assertThat(jobPlan.getPlan()).isNotNull();

            // and when
            JobDetailsInfo jobDetails = api.getJobDetails(jobId);

            // then
            assertThat(jobDetails).isNotNull();
            System.out.println(new Gson().toJson(jobDetails));
            assertThat(jobDetails.getPlan()).isNotNull();
            assertThat(jobDetails.getJid()).isEqualTo(jobId);
            assertThat(jobDetails.getVertices()).hasSize(1);

            // and when
            CheckpointingStatistics checkpointingStatistics = api.getJobCheckpoints(jobId);

            // then
            assertThat(checkpointingStatistics).isNotNull();
            System.out.println(new Gson().toJson(checkpointingStatistics));
            assertThat(checkpointingStatistics.getCounts()).isNotNull();

            // and when
            JobAccumulatorsInfo jobAccumulatorsInfo = api.getJobAccumulators(jobId, true);

            // then
            assertThat(jobAccumulatorsInfo).isNotNull();
            System.out.println(new Gson().toJson(jobAccumulatorsInfo));
            assertThat(jobAccumulatorsInfo.getJobAccumulators()).isNotNull();

            // and when
            JobExceptionsInfo exceptionInjobExceptionsInfoo = api.getJobExceptions(jobId);

            // then
            assertThat(exceptionInjobExceptionsInfoo).isNotNull();
            System.out.println(new Gson().toJson(exceptionInjobExceptionsInfoo));
            assertThat(exceptionInjobExceptionsInfoo.getAllExceptions()).isNotNull();

            // and when
            Object jobMetrics = api.getJobMetrics(jobId, "numberOfFailedCheckpoints");

            // then
            assertThat(jobMetrics).isNotNull();
            System.out.println(new Gson().toJson(jobMetrics));

            // and when
            JobExecutionResultResponseBody jobResult = api.getJobResult(jobId);

            // then
            assertThat(jobResult).isNotNull();
            System.out.println(new Gson().toJson(jobResult));
            assertThat(jobResult.getStatus().getId()).isEqualTo(QueueStatus.IdEnum.IN_PROGRESS);
            assertThat(jobResult.getJobExecutionResult()).isNull();

            // and when
            CheckpointConfigInfo checkpointConfigInfo = api.getJobCheckpointsConfig(jobId);

            // then
            assertThat(checkpointConfigInfo).isNotNull();
            System.out.println(new Gson().toJson(checkpointConfigInfo));
            assertThat(checkpointConfigInfo.getInterval()).isEqualTo(60000L);

            // and when
            JobVertexAccumulatorsInfo jobTaskAllAccumulatorsInfo = api.getJobTaskAccumulators(jobId, jobDetails.getVertices().get(0).getId().toString());

            // then
            assertThat(jobTaskAllAccumulatorsInfo).isNotNull();
            System.out.println(new Gson().toJson(jobTaskAllAccumulatorsInfo));
            assertThat(jobTaskAllAccumulatorsInfo.getUserAccumulators()).isNotNull();

            // and when
            JobVertexBackPressureInfo jobTaskBackpressure = api.getJobTaskBackpressure(jobId, jobDetails.getVertices().get(0).getId().toString());

            // then
            assertThat(jobTaskBackpressure).isNotNull();
            System.out.println(new Gson().toJson(jobTaskBackpressure));
            assertThat(jobTaskBackpressure.getStatus()).isEqualTo(JobVertexBackPressureInfo.StatusEnum.DEPRECATED);

            // and when
            JobVertexDetailsInfo jobTaskDetails = api.getJobTaskDetails(jobId, jobDetails.getVertices().get(0).getId().toString());

            // then
            assertThat(jobTaskDetails).isNotNull();
            System.out.println(new Gson().toJson(jobTaskDetails));
            assertThat(jobTaskDetails.getSubtasks()).hasSize(1);

            // and when
            JobVertexTaskManagersInfo jobTaskManagerDetails = api.getJobTaskDetailsByTaskManager(jobId, jobDetails.getVertices().get(0).getId().toString());

            // then
            assertThat(jobTaskManagerDetails).isNotNull();
            System.out.println(new Gson().toJson(jobTaskManagerDetails));
            assertThat(jobTaskManagerDetails.getTaskmanagers()).hasSize(1);

            // and when
            Object jobTaskMetrics = api.getJobTaskMetrics(jobId, jobDetails.getVertices().get(0).getId().toString(), "");/*TODO*/

            // then
            assertThat(jobTaskMetrics).isNotNull();
            System.out.println(new Gson().toJson(jobTaskMetrics));

            // and when
            SubtasksAllAccumulatorsInfo jobSubtasksAllAccumulatorsInfo = api.getJobSubtaskAccumulators(jobId, jobDetails.getVertices().get(0).getId().toString());

            // then
            assertThat(jobSubtasksAllAccumulatorsInfo).isNotNull();
            System.out.println(new Gson().toJson(jobSubtasksAllAccumulatorsInfo));
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
                        System.out.println(new Gson().toJson(subtaskExecutionDetailsInfo));
                        assertThat(subtaskExecutionDetailsInfo.getStatus()).isEqualTo(SubtaskExecutionAttemptDetailsInfo.StatusEnum.RUNNING);
                    });

//            // and when
//            SubtaskExecutionAttemptAccumulatorsInfo subtaskExecutionAttemptAccumulatorsInfo = api.getJobSubtaskAttemptAccumulators(jobId, jobDetails.getVertices().get(0).getId().toString(), 0, 0);
//
//            // then
//            assertThat(subtaskExecutionAttemptAccumulatorsInfo).isNotNull();
//            System.out.println(new Gson().toJson(subtaskExecutionAttemptAccumulatorsInfo));
//            assertThat(subtaskExecutionAttemptAccumulatorsInfo.getUserAccumulators()).isNotNull();

//            // and when
//            SubtaskExecutionAttemptDetailsInfo subtaskExecutionAttemptDetailsInfo = api.getJobSubtaskAttemptDetails(jobId, jobDetails.getVertices().get(0).getId().toString(), 0, 0);
//
//            // then
//            assertThat(subtaskExecutionAttemptDetailsInfo).isNotNull();
//            System.out.println(new Gson().toJson(subtaskExecutionAttemptDetailsInfo));
//            assertThat(subtaskExecutionAttemptDetailsInfo.getStatus()).isEqualTo(SubtaskExecutionAttemptDetailsInfo.StatusEnum.RUNNING);

            // and when
            Object subtaskMetrics = api.getJobSubtaskMetrics(jobId, jobDetails.getVertices().get(0).getId().toString(), 0, "");/*TODO*/

            // then
            assertThat(subtaskMetrics).isNotNull();
            System.out.println(new Gson().toJson(subtaskMetrics));

            // and when
            SubtasksTimesInfo subtasksTimesInfo = api.getJobSubtaskTimes(jobId, jobDetails.getVertices().get(0).getId().toString());

            // then
            assertThat(subtasksTimesInfo).isNotNull();
            System.out.println(new Gson().toJson(subtasksTimesInfo));
            assertThat(subtasksTimesInfo.getSubtasks()).hasSize(1);

            // and when
            Object subtasksAggregatedMetrics = api.getJobAggregatedSubtaskMetrics(jobId, jobDetails.getVertices().get(0).getId().toString(), "", "sum", null);/*TODO*/

            // then
            assertThat(subtasksAggregatedMetrics).isNotNull();
            System.out.println(new Gson().toJson(subtasksAggregatedMetrics));

            // and when
            TriggerResponse triggerSavepointResponse = api.createJobSavepoint(new SavepointTriggerRequestBody().cancelJob(false).targetDirectory("file:///var/tmp"), jobId);

            // then
            assertThat(triggerSavepointResponse).isNotNull();
            System.out.println(new Gson().toJson(triggerSavepointResponse));
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
                        System.out.println(new Gson().toJson(asynchronousOperationResult));
                        assertThat(asynchronousOperationResult.getStatus().getId()).isEqualTo(QueueStatus.IdEnum.COMPLETED);
                    });

            // and when
            TriggerResponse triggerRescalingResponse = api.triggerJobRescaling(jobId, 2);

            // then
            assertThat(triggerRescalingResponse).isNotNull();
            System.out.println(new Gson().toJson(triggerRescalingResponse));
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
                        System.out.println(new Gson().toJson(asynchronousOperationResult));
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
                    System.out.println(new Gson().toJson(jobCheckpoints));
                    assertThat(jobCheckpoints.getHistory()).isNotNull();
                    assertThat(jobCheckpoints.getHistory().size() > 0).isTrue();

                    // and when
                    Integer checkpointId = jobCheckpoints.getHistory().get(0).getId();
                    CheckpointStatistics jobCheckpointDetails = api.getJobCheckpointDetails(jobId, checkpointId);

                    // then
                    assertThat(jobCheckpointDetails).isNotNull();
                    System.out.println(new Gson().toJson(jobCheckpointDetails));
                    assertThat(jobCheckpointDetails.getStatus().getValue()).isEqualTo(CheckpointStatistics.StatusEnum.COMPLETED.getValue());

                    // and when
                    TaskCheckpointStatisticsWithSubtaskDetails taskCheckpointStatistics = api.getJobCheckpointStatistics(jobId, checkpointId, jobDetails.getVertices().get(0).getId().toString());

                    // then
                    assertThat(taskCheckpointStatistics).isNotNull();
                    System.out.println(new Gson().toJson(taskCheckpointStatistics));
                    assertThat(taskCheckpointStatistics.getStatus().getValue()).isEqualTo(CheckpointStatistics.StatusEnum.COMPLETED.getValue());
                });
        }
    }

    @Nested
    class JobManager {
        @Test
        public void shouldReturnMetrics() throws ApiException {
            // when
            Object metrics = api.getJobManagerMetrics("Status.JVM.CPU.Time");

            // then
            assertThat(metrics).isNotNull();
            System.out.println(new Gson().toJson(metrics));
        }
    }

    @Nested
    class TaskManager {
        @Test
        public void shouldReturnMetrics() throws ApiException {
            // when
            TaskManagersInfo taskManagerInfo = api.getTaskManagersOverview();

            // then
            assertThat(taskManagerInfo).isNotNull();
            System.out.println(new Gson().toJson(taskManagerInfo));
            assertThat(taskManagerInfo.getTaskmanagers()).isNotNull();
            assertThat(taskManagerInfo.getTaskmanagers()).hasSize(1);
            final String taskManagerId = taskManagerInfo.getTaskmanagers().get(0).getId().toString();

            // and when
            TaskManagerDetailsInfo taskManagerDetails = api.getTaskManagerDetails(taskManagerId);

            // then
            assertThat(taskManagerDetails).isNotNull();
            System.out.println(new Gson().toJson(taskManagerDetails));
            assertThat(taskManagerDetails.getMetrics()).isNotNull();
            assertThat(taskManagerDetails.getMetrics().getHeapMax()).isGreaterThan(0);

            // and when
            Object metrics = api.getTaskManagerMetrics(taskManagerId, "Status.JVM.CPU.Time");

            // then
            assertThat(metrics).isNotNull();
            System.out.println(new Gson().toJson(metrics));

            // and when
            Object aggregatedMetrics = api.getTaskManagerAggregatedMetrics("Status.JVM.CPU.Time", "sum", taskManagerId);

            // then
            assertThat(aggregatedMetrics).isNotNull();
            System.out.println(new Gson().toJson(aggregatedMetrics));
        }
    }
}
