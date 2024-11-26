/*
 * This file is part of Flink Client
 * https://github.com/nextbreakpoint/flink-client
 */
package com.nextbreakpoint.flink.client.support;

import com.nextbreakpoint.flink.client.api.FlinkApi;
import com.nextbreakpoint.flink.client.api.ApiException;
import com.nextbreakpoint.flink.client.model.AggregatedMetricsResponseBody;
import com.nextbreakpoint.flink.client.model.AggregationMode;
import com.nextbreakpoint.flink.client.model.AsynchronousOperationResult;
import com.nextbreakpoint.flink.client.model.CheckpointConfigInfo;
import com.nextbreakpoint.flink.client.model.CheckpointStatistics;
import com.nextbreakpoint.flink.client.model.CheckpointingStatistics;
import com.nextbreakpoint.flink.client.model.ClusterOverviewWithVersion;
import com.nextbreakpoint.flink.client.model.ConfigurationInfoEntry;
import com.nextbreakpoint.flink.client.model.DashboardConfiguration;
import com.nextbreakpoint.flink.client.model.FailureLabel;
import com.nextbreakpoint.flink.client.model.JarListInfo;
import com.nextbreakpoint.flink.client.model.JarPlanRequestBody;
import com.nextbreakpoint.flink.client.model.JarRunRequestBody;
import com.nextbreakpoint.flink.client.model.JarRunResponseBody;
import com.nextbreakpoint.flink.client.model.JarUploadResponseBody;
import com.nextbreakpoint.flink.client.model.JobAccumulatorsInfo;
import com.nextbreakpoint.flink.client.model.JobConfigInfo;
import com.nextbreakpoint.flink.client.model.JobDetailsInfo;
import com.nextbreakpoint.flink.client.model.JobExceptionsInfoWithHistory;
import com.nextbreakpoint.flink.client.model.JobExecutionResultResponseBody;
import com.nextbreakpoint.flink.client.model.JobIdsWithStatusOverview;
import com.nextbreakpoint.flink.client.model.JobPlanInfo;
import com.nextbreakpoint.flink.client.model.JobVertexAccumulatorsInfo;
import com.nextbreakpoint.flink.client.model.JobVertexBackPressureInfo;
import com.nextbreakpoint.flink.client.model.JobVertexDetailsInfo;
import com.nextbreakpoint.flink.client.model.JobVertexTaskManagersInfo;
import com.nextbreakpoint.flink.client.model.MetricCollectionResponseBody;
import com.nextbreakpoint.flink.client.model.MultipleJobsDetails;
import com.nextbreakpoint.flink.client.model.SavepointDisposalRequest;
import com.nextbreakpoint.flink.client.model.SavepointTriggerRequestBody;
import com.nextbreakpoint.flink.client.model.SubtaskExecutionAttemptAccumulatorsInfo;
import com.nextbreakpoint.flink.client.model.SubtaskExecutionAttemptDetailsInfo;
import com.nextbreakpoint.flink.client.model.SubtasksAllAccumulatorsInfo;
import com.nextbreakpoint.flink.client.model.SubtasksTimesInfo;
import com.nextbreakpoint.flink.client.model.TaskCheckpointStatisticsWithSubtaskDetails;
import com.nextbreakpoint.flink.client.model.TaskManagerDetailsInfo;
import com.nextbreakpoint.flink.client.model.TaskManagersInfo;
import com.nextbreakpoint.flink.client.model.TerminationMode;
import com.nextbreakpoint.flink.client.model.TriggerResponse;

import java.io.File;
import java.util.List;

import static com.nextbreakpoint.flink.client.support.AsyncUtils.await;
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
