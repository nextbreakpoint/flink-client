/*
 * This file is part of Flink Client
 * https://github.com/nextbreakpoint/flink-client
 */
package com.nextbreakpoint.flink.client.support;

import com.nextbreakpoint.flink.client.api.ApiException;
import com.nextbreakpoint.flink.client.api.FlinkApi;
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

public class SyncDriver implements Driver {
    private final FlinkApi api;

    public SyncDriver(FlinkApi api) {
        this.api = api;
    }

    @Override
    public DashboardConfiguration getDashboardConfiguration() throws ApiException {
        return api.getDashboardConfiguration();
    }

    @Override
    public ClusterOverviewWithVersion getClusterOverview() throws ApiException {
        return api.getClusterOverview();
    }

    @Override
    public JarUploadResponseBody uploadJar(File file) throws ApiException {
        return api.uploadJar(file);
    }

    @Override
    public JarListInfo getJarList() throws ApiException {
        return api.getJarList();
    }

    @Override
    public void deleteJar(String jarId) throws ApiException {
        api.deleteJar(jarId);
    }

    @Override
    public JobPlanInfo generatePlanFromJar(String jarId, JarPlanRequestBody body, String programArgs, String programArg, String entryClass, Integer parallelism) throws ApiException {
        return api.generatePlanFromJar(jarId, body, programArgs, programArg, entryClass, parallelism);
    }

    @Override
    public JarRunResponseBody submitJobFromJar(String jarId, JarRunRequestBody body, Boolean allowNonRestoredState, String savepointPath, String programArgs, String programArg, String entryClass, Integer parallelism) throws ApiException {
        return api.submitJobFromJar(jarId, body, allowNonRestoredState, savepointPath, programArgs, programArg, entryClass, parallelism);
    }

    @Override
    public void cancelJob(String jobId, TerminationMode terminationMode) throws ApiException {
        api.cancelJob(jobId, terminationMode);
    }

    @Override
    public JobIdsWithStatusOverview getJobIdsWithStatusesOverview() throws ApiException {
        return api.getJobIdsWithStatusesOverview();
    }

    @Override
    public MultipleJobsDetails getJobsOverview() throws ApiException {
        return api.getJobsOverview();
    }

    @Override
    public AggregatedMetricsResponseBody getAggregatedJobMetrics(String get, AggregationMode agg, String jobs) throws ApiException {
        return api.getAggregatedJobMetrics(get, agg, jobs);
    }

    @Override
    public JobConfigInfo getJobConfig(String jobId) throws ApiException {
        return api.getJobConfig(jobId);
    }

    @Override
    public JobPlanInfo getJobPlan(String jobId) throws ApiException {
        return api.getJobPlan(jobId);
    }

    @Override
    public MetricCollectionResponseBody getJobMetrics(String jobId, String get) throws ApiException {
        return api.getJobMetrics(jobId, get);
    }

    @Override
    public JobDetailsInfo getJobDetails(String jobId) throws ApiException {
        return api.getJobDetails(jobId);
    }

    @Override
    public JobExecutionResultResponseBody getJobExecutionResult(String jobId) throws ApiException {
        return api.getJobExecutionResult(jobId);
    }

    @Override
    public JobExceptionsInfoWithHistory getJobExceptions(String jobId, Integer maxExceptions, FailureLabel failureLabelFilter) throws ApiException {
        return api.getJobExceptions(jobId, maxExceptions, failureLabelFilter);
    }

    @Override
    public JobAccumulatorsInfo getJobAccumulators(String jobId, boolean includeSerializedValue) throws ApiException {
        return api.getJobAccumulators(jobId, includeSerializedValue);
    }

    @Override
    public CheckpointingStatistics getCheckpointingStatistics(String jobId) throws ApiException {
        return api.getCheckpointingStatistics(jobId);
    }

    @Override
    public MetricCollectionResponseBody getJobVertexMetrics(String jobId, String vertexId, String get) throws ApiException {
        return api.getJobVertexMetrics(jobId, vertexId, get);
    }

    @Override
    public JobVertexDetailsInfo getJobVertexDetails(String jobId, String vertexId) throws ApiException {
        return api.getJobVertexDetails(jobId, vertexId);
    }

    @Override
    public JobVertexAccumulatorsInfo getJobVertexAccumulators(String jobId, String vertexId) throws ApiException {
        return api.getJobVertexAccumulators(jobId, vertexId);
    }

    @Override
    public JobVertexBackPressureInfo getJobVertexBackPressure(String jobId, String vertexId) throws ApiException {
        return api.getJobVertexBackPressure(jobId, vertexId);
    }

    @Override
    public JobVertexTaskManagersInfo getJobVertexTaskManagers(String jobId, String vertexId) throws ApiException {
        return api.getJobVertexTaskManagers(jobId, vertexId);
    }

    @Override
    public SubtaskExecutionAttemptDetailsInfo getSubtaskCurrentAttemptDetails(String jobId, String vertexId, Integer subtaskIndex) throws ApiException {
        return api.getSubtaskCurrentAttemptDetails(jobId, vertexId, subtaskIndex);
    }

    @Override
    public SubtaskExecutionAttemptAccumulatorsInfo getSubtaskExecutionAttemptAccumulators(String jobId, String vertexId, Integer subtaskIndex, Integer attempt) throws ApiException {
        return api.getSubtaskExecutionAttemptAccumulators(jobId, vertexId, subtaskIndex, attempt);
    }

    @Override
    public MetricCollectionResponseBody getSubtaskMetrics(String jobId, String vertexId, Integer subtaskIndex, String get) throws ApiException {
        return api.getSubtaskMetrics(jobId, vertexId, subtaskIndex, get);
    }

    @Override
    public AggregatedMetricsResponseBody getAggregatedSubtaskMetrics(String jobId, String vertexId, String get, AggregationMode agg, String subtasks) throws ApiException {
        return api.getAggregatedSubtaskMetrics(jobId, vertexId, get, agg, subtasks);
    }

    @Override
    public SubtasksTimesInfo getSubtasksTimes(String jobId, String vertexId) throws ApiException {
        return api.getSubtasksTimes(jobId, vertexId);
    }

    @Override
    public SubtasksAllAccumulatorsInfo getSubtasksAllAccumulators(String jobId, String vertexId) throws ApiException {
        return api.getSubtasksAllAccumulators(jobId, vertexId);
    }

    @Override
    public TriggerResponse triggerSavepoint(String jobId, SavepointTriggerRequestBody body) throws ApiException {
        return api.triggerSavepoint(jobId, body);
    }

    @Override
    public AsynchronousOperationResult getSavepointStatus(String jobId, String triggerId) throws ApiException {
        return api.getSavepointStatus(jobId, triggerId);
    }

    @Override
    public TriggerResponse triggerSavepointDisposal(SavepointDisposalRequest body) throws ApiException {
        return api.triggerSavepointDisposal(body);
    }

    @Override
    public AsynchronousOperationResult getSavepointDisposalStatus(String triggerId) throws ApiException {
        return api.getSavepointDisposalStatus(triggerId);
    }

    @Override
    public TriggerResponse rescaleJob(String jobId, Integer parallelism) throws ApiException {
        return api.rescaleJob(jobId, parallelism);
    }

    @Override
    public AsynchronousOperationResult getRescalingStatus(String jobId, String triggerId) throws ApiException {
        return api.getRescalingStatus(jobId, triggerId);
    }

    @Override
    public CheckpointConfigInfo getCheckpointConfig(String jobId) throws ApiException {
        return api.getCheckpointConfig(jobId);
    }

    @Override
    public CheckpointStatistics getCheckpointStatisticDetails(String jobId, Long checkpointId) throws ApiException {
        return api.getCheckpointStatisticDetails(jobId, checkpointId);
    }

    @Override
    public TaskCheckpointStatisticsWithSubtaskDetails getTaskCheckpointStatistics(String jobId, Long checkpointId, String vertexId) throws ApiException {
        return api.getTaskCheckpointStatistics(jobId, checkpointId, vertexId);
    }

    @Override
    public MetricCollectionResponseBody getJobManagerMetrics(String get) throws ApiException {
        return api.getJobManagerMetrics(get);
    }

    @Override
    public List<ConfigurationInfoEntry> getClusterConfigurationInfo() throws ApiException {
        return api.getClusterConfigurationInfo();
    }

    @Override
    public TaskManagersInfo getTaskManagers() throws ApiException {
        return api.getTaskManagers();
    }

    @Override
    public TaskManagerDetailsInfo getTaskManagerDetails(String taskManagerId) throws ApiException {
        return api.getTaskManagerDetails(taskManagerId);
    }

    @Override
    public MetricCollectionResponseBody getTaskManagerMetrics(String taskManagerId, String get) throws ApiException {
        return api.getTaskManagerMetrics(taskManagerId, get);
    }

    @Override
    public AggregatedMetricsResponseBody getAggregatedTaskManagerMetrics(String get, AggregationMode agg, String taskManagerId) throws ApiException {
        return api.getAggregatedTaskManagerMetrics(get, agg, taskManagerId);
    }
}
