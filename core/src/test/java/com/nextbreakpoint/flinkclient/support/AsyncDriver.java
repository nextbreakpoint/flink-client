/*
 * This file is part of FlinkClient
 * https://github.com/nextbreakpoint/flinkclient
 */
package com.nextbreakpoint.flinkclient.support;

import com.nextbreakpoint.flinkclient.api.ApiException;
import com.nextbreakpoint.flinkclient.api.FlinkApi;
import com.nextbreakpoint.flinkclient.model.AggregatedMetricsResponseBody;
import com.nextbreakpoint.flinkclient.model.AggregationMode;
import com.nextbreakpoint.flinkclient.model.AsynchronousOperationResult;
import com.nextbreakpoint.flinkclient.model.CheckpointConfigInfo;
import com.nextbreakpoint.flinkclient.model.CheckpointStatistics;
import com.nextbreakpoint.flinkclient.model.CheckpointingStatistics;
import com.nextbreakpoint.flinkclient.model.ClusterOverviewWithVersion;
import com.nextbreakpoint.flinkclient.model.ConfigurationInfoEntry;
import com.nextbreakpoint.flinkclient.model.DashboardConfiguration;
import com.nextbreakpoint.flinkclient.model.FailureLabel;
import com.nextbreakpoint.flinkclient.model.JarListInfo;
import com.nextbreakpoint.flinkclient.model.JarPlanRequestBody;
import com.nextbreakpoint.flinkclient.model.JarRunRequestBody;
import com.nextbreakpoint.flinkclient.model.JarRunResponseBody;
import com.nextbreakpoint.flinkclient.model.JarUploadResponseBody;
import com.nextbreakpoint.flinkclient.model.JobAccumulatorsInfo;
import com.nextbreakpoint.flinkclient.model.JobConfigInfo;
import com.nextbreakpoint.flinkclient.model.JobDetailsInfo;
import com.nextbreakpoint.flinkclient.model.JobExceptionsInfoWithHistory;
import com.nextbreakpoint.flinkclient.model.JobExecutionResultResponseBody;
import com.nextbreakpoint.flinkclient.model.JobIdsWithStatusOverview;
import com.nextbreakpoint.flinkclient.model.JobPlanInfo;
import com.nextbreakpoint.flinkclient.model.JobVertexAccumulatorsInfo;
import com.nextbreakpoint.flinkclient.model.JobVertexBackPressureInfo;
import com.nextbreakpoint.flinkclient.model.JobVertexDetailsInfo;
import com.nextbreakpoint.flinkclient.model.JobVertexTaskManagersInfo;
import com.nextbreakpoint.flinkclient.model.MetricCollectionResponseBody;
import com.nextbreakpoint.flinkclient.model.MultipleJobsDetails;
import com.nextbreakpoint.flinkclient.model.SavepointDisposalRequest;
import com.nextbreakpoint.flinkclient.model.SavepointTriggerRequestBody;
import com.nextbreakpoint.flinkclient.model.SubtaskExecutionAttemptAccumulatorsInfo;
import com.nextbreakpoint.flinkclient.model.SubtaskExecutionAttemptDetailsInfo;
import com.nextbreakpoint.flinkclient.model.SubtasksAllAccumulatorsInfo;
import com.nextbreakpoint.flinkclient.model.SubtasksTimesInfo;
import com.nextbreakpoint.flinkclient.model.TaskCheckpointStatisticsWithSubtaskDetails;
import com.nextbreakpoint.flinkclient.model.TaskManagerDetailsInfo;
import com.nextbreakpoint.flinkclient.model.TaskManagersInfo;
import com.nextbreakpoint.flinkclient.model.TerminationMode;
import com.nextbreakpoint.flinkclient.model.TriggerResponse;

import java.io.File;
import java.util.List;

import static com.nextbreakpoint.flinkclient.support.AsyncUtils.await;
import static org.assertj.core.api.Assertions.assertThat;

public class AsyncDriver implements Driver {
    private final FlinkApi api;

    public AsyncDriver(FlinkApi api) {
        this.api = api;
    }

    @Override
    public DashboardConfiguration getDashboardConfiguration() throws ApiException {
        final TestCallback<DashboardConfiguration> callback = new TestCallback<>();
        api.getDashboardConfigurationAsync(callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public ClusterOverviewWithVersion getClusterOverview() throws ApiException {
        final TestCallback<ClusterOverviewWithVersion> callback = new TestCallback<>();
        api.getClusterOverviewAsync(callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public JarUploadResponseBody uploadJar(File file) throws ApiException {
        final TestCallback<JarUploadResponseBody> callback = new TestCallback<>();
        api.uploadJarAsync(file, callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public JarListInfo getJarList() throws ApiException {
        final TestCallback<JarListInfo> callback = new TestCallback<>();
        api.getJarListAsync(callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public void deleteJar(String jarId) throws ApiException {
        final TestCallback<Void> callback = new TestCallback<>();
        api.deleteJarAsync(jarId, callback);
        if (callback.getException() != null) {
            throw callback.getException();
        }
        await(() -> assertThat(callback.isCompleted()).isTrue());
    }

    @Override
    public JobPlanInfo generatePlanFromJar(String jarId, JarPlanRequestBody body, String programArgs, String programArg, String entryClass, Integer parallelism) throws ApiException {
        final TestCallback<JobPlanInfo> callback = new TestCallback<>();
        api.generatePlanFromJarAsync(jarId, body, programArgs, programArg, entryClass, parallelism, callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public JarRunResponseBody submitJobFromJar(String jarId, JarRunRequestBody body, Boolean allowNonRestoredState, String savepointPath, String programArgs, String programArg, String entryClass, Integer parallelism) throws ApiException {
        final TestCallback<JarRunResponseBody> callback = new TestCallback<>();
        api.submitJobFromJarAsync(jarId, body, allowNonRestoredState, savepointPath, programArgs, programArg, entryClass, parallelism, callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public void cancelJob(String jobId, TerminationMode terminationMode) throws ApiException {
        final TestCallback<Void> callback = new TestCallback<>();
        api.cancelJobAsync(jobId, terminationMode, callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
    }

    @Override
    public JobIdsWithStatusOverview getJobIdsWithStatusesOverview() throws ApiException {
        final TestCallback<JobIdsWithStatusOverview> callback = new TestCallback<>();
        api.getJobIdsWithStatusesOverviewAsync(callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public MultipleJobsDetails getJobsOverview() throws ApiException {
        final TestCallback<MultipleJobsDetails> callback = new TestCallback<>();
        api.getJobsOverviewAsync(callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public AggregatedMetricsResponseBody getAggregatedJobMetrics(String get, AggregationMode agg, String jobs) throws ApiException {
        final TestCallback<AggregatedMetricsResponseBody> callback = new TestCallback<>();
        api.getAggregatedJobMetricsAsync(get, agg, jobs, callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public JobConfigInfo getJobConfig(String jobId) throws ApiException {
        final TestCallback<JobConfigInfo> callback = new TestCallback<>();
        api.getJobConfigAsync(jobId, callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public JobPlanInfo getJobPlan(String jobId) throws ApiException {
        final TestCallback<JobPlanInfo> callback = new TestCallback<>();
        api.getJobPlanAsync(jobId, callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public MetricCollectionResponseBody getJobMetrics(String jobId, String get) throws ApiException {
        final TestCallback<MetricCollectionResponseBody> callback = new TestCallback<>();
        api.getJobMetricsAsync(jobId, get, callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public JobDetailsInfo getJobDetails(String jobId) throws ApiException {
        final TestCallback<JobDetailsInfo> callback = new TestCallback<>();
        api.getJobDetailsAsync(jobId, callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public JobExecutionResultResponseBody getJobExecutionResult(String jobId) throws ApiException {
        final TestCallback<JobExecutionResultResponseBody> callback = new TestCallback<>();
        api.getJobExecutionResultAsync(jobId, callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public JobExceptionsInfoWithHistory getJobExceptions(String jobId, Integer maxExceptions, FailureLabel failureLabelFilter) throws ApiException {
        final TestCallback<JobExceptionsInfoWithHistory> callback = new TestCallback<>();
        api.getJobExceptionsAsync(jobId, maxExceptions, failureLabelFilter, callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public JobAccumulatorsInfo getJobAccumulators(String jobId, boolean includeSerializedValue) throws ApiException {
        final TestCallback<JobAccumulatorsInfo> callback = new TestCallback<>();
        api.getJobAccumulatorsAsync(jobId, includeSerializedValue, callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public CheckpointingStatistics getCheckpointingStatistics(String jobId) throws ApiException {
        final TestCallback<CheckpointingStatistics> callback = new TestCallback<>();
        api.getCheckpointingStatisticsAsync(jobId, callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public MetricCollectionResponseBody getJobVertexMetrics(String jobId, String vertexId, String get) throws ApiException {
        final TestCallback<MetricCollectionResponseBody> callback = new TestCallback<>();
        api.getJobVertexMetricsAsync(jobId, vertexId, get, callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public JobVertexDetailsInfo getJobVertexDetails(String jobId, String vertexId) throws ApiException {
        final TestCallback<JobVertexDetailsInfo> callback = new TestCallback<>();
        api.getJobVertexDetailsAsync(jobId, vertexId, callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public JobVertexAccumulatorsInfo getJobVertexAccumulators(String jobId, String vertexId) throws ApiException {
        final TestCallback<JobVertexAccumulatorsInfo> callback = new TestCallback<>();
        api.getJobVertexAccumulatorsAsync(jobId, vertexId, callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public JobVertexBackPressureInfo getJobVertexBackPressure(String jobId, String vertexId) throws ApiException {
        final TestCallback<JobVertexBackPressureInfo> callback = new TestCallback<>();
        api.getJobVertexBackPressureAsync(jobId, vertexId, callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public JobVertexTaskManagersInfo getJobVertexTaskManagers(String jobId, String vertexId) throws ApiException {
        final TestCallback<JobVertexTaskManagersInfo> callback = new TestCallback<>();
        api.getJobVertexTaskManagersAsync(jobId, vertexId, callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public SubtaskExecutionAttemptDetailsInfo getSubtaskCurrentAttemptDetails(String jobId, String vertexId, Integer subtaskIndex) throws ApiException {
        final TestCallback<SubtaskExecutionAttemptDetailsInfo> callback = new TestCallback<>();
        api.getSubtaskCurrentAttemptDetailsAsync(jobId, vertexId, subtaskIndex, callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public SubtaskExecutionAttemptAccumulatorsInfo getSubtaskExecutionAttemptAccumulators(String jobId, String vertexId, Integer subtaskIndex, Integer attempt) throws ApiException {
        final TestCallback<SubtaskExecutionAttemptAccumulatorsInfo> callback = new TestCallback<>();
        api.getSubtaskExecutionAttemptAccumulatorsAsync(jobId, vertexId, subtaskIndex, attempt, callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public MetricCollectionResponseBody getSubtaskMetrics(String jobId, String vertexId, Integer subtaskIndex, String get) throws ApiException {
        final TestCallback<MetricCollectionResponseBody> callback = new TestCallback<>();
        api.getSubtaskMetricsAsync(jobId, vertexId, subtaskIndex, get, callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public AggregatedMetricsResponseBody getAggregatedSubtaskMetrics(String jobId, String vertexId, String get, AggregationMode agg, String subtasks) throws ApiException {
        final TestCallback<AggregatedMetricsResponseBody> callback = new TestCallback<>();
        api.getAggregatedSubtaskMetricsAsync(jobId, vertexId, get, agg, subtasks, callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public SubtasksTimesInfo getSubtasksTimes(String jobId, String vertexId) throws ApiException {
        final TestCallback<SubtasksTimesInfo> callback = new TestCallback<>();
        api.getSubtasksTimesAsync(jobId, vertexId, callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public SubtasksAllAccumulatorsInfo getSubtasksAllAccumulators(String jobId, String vertexId) throws ApiException {
        final TestCallback<SubtasksAllAccumulatorsInfo> callback = new TestCallback<>();
        api.getSubtasksAllAccumulatorsAsync(jobId, vertexId, callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public TriggerResponse triggerSavepoint(String jobId, SavepointTriggerRequestBody body) throws ApiException {
        final TestCallback<TriggerResponse> callback = new TestCallback<>();
        api.triggerSavepointAsync(jobId, body, callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public AsynchronousOperationResult getSavepointStatus(String jobId, String triggerId) throws ApiException {
        final TestCallback<AsynchronousOperationResult> callback = new TestCallback<>();
        api.getSavepointStatusAsync(jobId, triggerId, callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public TriggerResponse triggerSavepointDisposal(SavepointDisposalRequest body) throws ApiException {
        final TestCallback<TriggerResponse> callback = new TestCallback<>();
        api.triggerSavepointDisposalAsync(body, callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public AsynchronousOperationResult getSavepointDisposalStatus(String triggerId) throws ApiException {
        final TestCallback<AsynchronousOperationResult> callback = new TestCallback<>();
        api.getSavepointDisposalStatusAsync(triggerId, callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public TriggerResponse rescaleJob(String jobId, Integer parallelism) throws ApiException {
        final TestCallback<TriggerResponse> callback = new TestCallback<>();
        api.rescaleJobAsync(jobId, parallelism, callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public AsynchronousOperationResult getRescalingStatus(String jobId, String triggerId) throws ApiException {
        final TestCallback<AsynchronousOperationResult> callback = new TestCallback<>();
        api.getRescalingStatusAsync(jobId, triggerId, callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public CheckpointConfigInfo getCheckpointConfig(String jobId) throws ApiException {
        final TestCallback<CheckpointConfigInfo> callback = new TestCallback<>();
        api.getCheckpointConfigAsync(jobId, callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public CheckpointStatistics getCheckpointStatisticDetails(String jobId, Long checkpointId) throws ApiException {
        final TestCallback<CheckpointStatistics> callback = new TestCallback<>();
        api.getCheckpointStatisticDetailsAsync(jobId, checkpointId, callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public TaskCheckpointStatisticsWithSubtaskDetails getTaskCheckpointStatistics(String jobId, Long checkpointId, String vertexId) throws ApiException {
        final TestCallback<TaskCheckpointStatisticsWithSubtaskDetails> callback = new TestCallback<>();
        api.getTaskCheckpointStatisticsAsync(jobId, checkpointId, vertexId, callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public MetricCollectionResponseBody getJobManagerMetrics(String get) throws ApiException {
        final TestCallback<MetricCollectionResponseBody> callback = new TestCallback<>();
        api.getJobManagerMetricsAsync(get, callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public List<ConfigurationInfoEntry> getClusterConfigurationInfo() throws ApiException {
        final TestCallback<List<ConfigurationInfoEntry>> callback = new TestCallback<>();
        api.getClusterConfigurationInfoAsync(callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public TaskManagersInfo getTaskManagers() throws ApiException {
        final TestCallback<TaskManagersInfo> callback = new TestCallback<>();
        api.getTaskManagersAsync(callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public TaskManagerDetailsInfo getTaskManagerDetails(String taskManagerId) throws ApiException {
        final TestCallback<TaskManagerDetailsInfo> callback = new TestCallback<>();
        api.getTaskManagerDetailsAsync(taskManagerId, callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public MetricCollectionResponseBody getTaskManagerMetrics(String taskManagerId, String get) throws ApiException {
        final TestCallback<MetricCollectionResponseBody> callback = new TestCallback<>();
        api.getTaskManagerMetricsAsync(taskManagerId, get, callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }

    @Override
    public AggregatedMetricsResponseBody getAggregatedTaskManagerMetrics(String get, AggregationMode agg, String taskManagerId) throws ApiException {
        final TestCallback<AggregatedMetricsResponseBody> callback = new TestCallback<>();
        api.getAggregatedTaskManagerMetricsAsync(get, agg, taskManagerId, callback);
        await(() -> assertThat(callback.isCompleted()).isTrue());
        if (callback.getException() != null) {
            throw callback.getException();
        }
        return callback.getResult();
    }
}
