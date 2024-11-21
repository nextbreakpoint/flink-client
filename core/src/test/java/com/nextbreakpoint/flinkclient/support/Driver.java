/*
 * This file is part of FlinkClient
 * https://github.com/nextbreakpoint/flinkclient
 */
package com.nextbreakpoint.flinkclient.support;

import com.nextbreakpoint.flinkclient.api.ApiException;
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

public interface Driver {
    DashboardConfiguration getDashboardConfiguration() throws ApiException;

    ClusterOverviewWithVersion getClusterOverview() throws ApiException;

    JarUploadResponseBody uploadJar(File file) throws ApiException;

    JarListInfo getJarList() throws ApiException;

    void deleteJar(String jarId) throws ApiException;

    JobPlanInfo generatePlanFromJar(String jarId, JarPlanRequestBody body, String programArgs, String programArg, String entryClass, Integer parallelism) throws ApiException;

    JarRunResponseBody submitJobFromJar(String jarId, JarRunRequestBody body, Boolean allowNonRestoredState, String savepointPath, String programArgs, String programArg, String entryClass, Integer parallelism) throws ApiException;

    void cancelJob(String jobId, TerminationMode terminationMode) throws ApiException;

    JobIdsWithStatusOverview getJobIdsWithStatusesOverview() throws ApiException;

    MultipleJobsDetails getJobsOverview() throws ApiException;

    AggregatedMetricsResponseBody getAggregatedJobMetrics(String get, AggregationMode agg, String jobs) throws ApiException;

    JobConfigInfo getJobConfig(String jobId) throws ApiException;

    JobPlanInfo getJobPlan(String jobId) throws ApiException;

    MetricCollectionResponseBody getJobMetrics(String jobId, String get) throws ApiException;

    JobDetailsInfo getJobDetails(String jobId) throws ApiException;

    JobExecutionResultResponseBody getJobExecutionResult(String jobId) throws ApiException;

    JobExceptionsInfoWithHistory getJobExceptions(String jobId, Integer maxExceptions, FailureLabel failureLabelFilter) throws ApiException;

    JobAccumulatorsInfo getJobAccumulators(String jobId, boolean includeSerializedValue) throws ApiException;

    CheckpointingStatistics getCheckpointingStatistics(String jobId) throws ApiException;

    MetricCollectionResponseBody getJobVertexMetrics(String jobId, String vertexId, String get) throws ApiException;

    JobVertexDetailsInfo getJobVertexDetails(String jobId, String vertexId) throws ApiException;

    JobVertexAccumulatorsInfo getJobVertexAccumulators(String jobId, String vertexId) throws ApiException;

    JobVertexBackPressureInfo getJobVertexBackPressure(String jobId, String vertexId) throws ApiException;

    JobVertexTaskManagersInfo getJobVertexTaskManagers(String jobId, String vertexId) throws ApiException;

    SubtaskExecutionAttemptDetailsInfo getSubtaskCurrentAttemptDetails(String jobId, String vertexId, Integer subtaskIndex) throws ApiException;

    MetricCollectionResponseBody getSubtaskMetrics(String jobId, String vertexId, Integer subtaskIndex, String get) throws ApiException;

    AggregatedMetricsResponseBody getAggregatedSubtaskMetrics(String jobId, String vertexId, String get, AggregationMode agg, String subtasks) throws ApiException;

    SubtasksTimesInfo getSubtasksTimes(String jobId, String vertexId) throws ApiException;

    SubtasksAllAccumulatorsInfo getSubtasksAllAccumulators(String jobId, String vertexId) throws ApiException;

    SubtaskExecutionAttemptAccumulatorsInfo getSubtaskExecutionAttemptAccumulators(String jobId, String vertexId, Integer subtaskIndex, Integer attempt) throws ApiException;

    TriggerResponse triggerSavepoint(String jobId, SavepointTriggerRequestBody body) throws ApiException;

    AsynchronousOperationResult getSavepointStatus(String jobId, String triggerId) throws ApiException;

    TriggerResponse triggerSavepointDisposal(SavepointDisposalRequest body) throws ApiException;

    AsynchronousOperationResult getSavepointDisposalStatus(String triggerId) throws ApiException;

    TriggerResponse rescaleJob(String jobId, Integer parallelism) throws ApiException;

    AsynchronousOperationResult getRescalingStatus(String jobId, String triggerId) throws ApiException;

    CheckpointConfigInfo getCheckpointConfig(String jobId) throws ApiException;

    CheckpointStatistics getCheckpointStatisticDetails(String jobId, Long checkpointId) throws ApiException;

    TaskCheckpointStatisticsWithSubtaskDetails getTaskCheckpointStatistics(String jobId, Long checkpointId, String vertexId) throws ApiException;

    MetricCollectionResponseBody getJobManagerMetrics(String get) throws ApiException;

    List<ConfigurationInfoEntry> getClusterConfigurationInfo() throws ApiException;

    TaskManagersInfo getTaskManagers() throws ApiException;

    TaskManagerDetailsInfo getTaskManagerDetails(String taskManagerId) throws ApiException;

    MetricCollectionResponseBody getTaskManagerMetrics(String taskManagerId, String get) throws ApiException;

    AggregatedMetricsResponseBody getAggregatedTaskManagerMetrics(String get, AggregationMode agg, String taskManagerId) throws ApiException;
}
