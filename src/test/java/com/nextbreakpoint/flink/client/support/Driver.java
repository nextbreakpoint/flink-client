/*
 * This file is part of Flink Client
 * https://github.com/nextbreakpoint/flink-client
 */
package com.nextbreakpoint.flink.client.support;

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
