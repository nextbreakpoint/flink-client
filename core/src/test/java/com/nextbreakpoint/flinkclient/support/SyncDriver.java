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
