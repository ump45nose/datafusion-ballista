// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Binds the [`ballista_flight_sql`] frontend to this scheduler.
//!
//! The frontend does not know about `SchedulerServer`; this is the only place
//! the two meet.

use std::sync::Arc;

use ballista_core::JobId;
use ballista_core::error::{BallistaError, Result};
use ballista_core::extension::SessionConfigExt;
use ballista_core::serde::protobuf::{JobStatus, RunningJob, SuccessfulJob, job_status};
use ballista_flight_sql::backend::{PipelinedOutput, QueryBackend, QueryResult};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion_proto::logical_plan::AsLogicalPlan;
use datafusion_proto::physical_plan::AsExecutionPlan;

use crate::scheduler_server::SchedulerServer;

/// Buffer for the job status stream. Statuses are consumed as fast as they
/// arrive, so this only has to absorb bursts.
const STATUS_BUFFER: usize = 16;

#[async_trait::async_trait]
impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> QueryBackend
    for SchedulerServer<T, U>
{
    async fn session(&self, session_id: &str) -> Result<Arc<SessionContext>> {
        let config = self.state.session_manager.produce_config();
        self.state
            .session_manager
            .create_or_update_session(session_id, &config)
            .await
    }

    async fn close_session(&self, session_id: &str) -> Result<()> {
        self.state.session_manager.remove_session(session_id).await
    }

    async fn execute(
        &self,
        job_name: &str,
        ctx: Arc<SessionContext>,
        plan: LogicalPlan,
    ) -> Result<QueryResult> {
        // Subscribe before submitting so no status can be missed, and follow
        // the status stream rather than polling `get_job_status`.
        let (subscriber, mut statuses) =
            tokio::sync::mpsc::channel::<JobStatus>(STATUS_BUFFER);
        let job_id = self
            .submit_job(job_name, ctx.clone(), &plan, Some(subscriber))
            .await?;

        while let Some(status) = statuses.recv().await {
            match status.status {
                // A pipelined job streams its result while it runs, and cannot
                // finish until the result is read, so hand it over as soon as
                // its final stage is placed. One small enough to finish first
                // still serves its result from the same places.
                Some(job_status::Status::Running(RunningJob {
                    pipelined_output_locations,
                    ..
                }))
                | Some(job_status::Status::Successful(SuccessfulJob {
                    pipelined_output_locations,
                    ..
                })) if !pipelined_output_locations.is_empty() => {
                    let output_stage = output_stage_id(
                        pipelined_output_locations
                            .iter()
                            .filter_map(|location| location.partition_id.as_ref())
                            .map(|partition_id| partition_id.stage_id),
                    );
                    let schema = self
                        .result_schema(&job_id, &ctx, &plan, output_stage)
                        .await?;
                    return Ok(QueryResult {
                        job_id: job_id.to_string(),
                        schema,
                        partitions: vec![],
                        pipelined: Some(PipelinedOutput {
                            partitions: pipelined_output_locations,
                            registration_timeout: ctx
                                .state()
                                .config()
                                .ballista_config()
                                .pipelined_registration_timeout(),
                        }),
                    });
                }
                Some(job_status::Status::Successful(SuccessfulJob {
                    partition_location,
                    ..
                })) => {
                    let output_stage = output_stage_id(
                        partition_location
                            .iter()
                            .filter_map(|location| location.partition_id.as_ref())
                            .map(|partition_id| partition_id.stage_id),
                    );
                    let schema = self
                        .result_schema(&job_id, &ctx, &plan, output_stage)
                        .await?;
                    return Ok(QueryResult {
                        job_id: job_id.to_string(),
                        schema,
                        partitions: partition_location,
                        pipelined: None,
                    });
                }
                Some(job_status::Status::Failed(failed)) => {
                    return Err(BallistaError::General(format!(
                        "job {job_id} failed: {}",
                        failed.error
                    )));
                }
                // Queued and Running are progress reports, not outcomes.
                _ => continue,
            }
        }

        Err(BallistaError::General(format!(
            "job {job_id} ended without reporting a final status"
        )))
    }
}

impl<T: 'static + AsLogicalPlan, U: 'static + AsExecutionPlan> SchedulerServer<T, U> {
    /// The schema of a finished job's result files.
    ///
    /// This has to be the schema the shuffle files actually carry, which is
    /// the physical plan's rather than the logical plan's: the two disagree
    /// about nullability often enough to matter (`version()` is nullable
    /// logically and not-null in the data; a list literal goes the other way),
    /// and a Flight SQL client that is promised one schema and handed another,
    /// ADBC among them, rejects the result outright.
    ///
    /// The job's final stage is the plan that wrote those files, so its schema
    /// is read from there. `output_stage` is the id of that stage as recorded
    /// in the result's output locations. It has to come from there: the
    /// adaptive graph never fills in `output_links`, so every one of its
    /// stages looks final, and picking one from the stage map would return an
    /// arbitrary (possibly intermediate) stage's schema. Without an id, a stage
    /// is used only if it is the single stage with no output links.
    ///
    /// Only if no stage can be identified, or the graph is no longer
    /// available, is the plan planned again, which for a listing table means
    /// listing its files again.
    async fn result_schema(
        &self,
        job_id: &JobId,
        ctx: &SessionContext,
        plan: &LogicalPlan,
        output_stage: Option<usize>,
    ) -> Result<SchemaRef> {
        let graph = self
            .state
            .task_manager
            .get_job_execution_graph(job_id)
            .await?;
        let final_stage_schema = graph.and_then(|graph| {
            let stages = graph.stages();
            let stage = match output_stage {
                Some(stage_id) => stages.get(&stage_id),
                None => {
                    let mut final_stages = stages
                        .values()
                        .filter(|stage| stage.output_links().is_empty());
                    match (final_stages.next(), final_stages.next()) {
                        (Some(stage), None) => Some(stage),
                        _ => None,
                    }
                }
            };
            stage.map(|stage| stage.plan().schema())
        });

        match final_stage_schema {
            Some(schema) => Ok(schema),
            None => Ok(ctx.state().create_physical_plan(plan).await?.schema()),
        }
    }
}

/// The id of the stage that wrote a job's result, read from the stage ids of
/// its output locations. `None` when there are no locations or they disagree.
fn output_stage_id(stage_ids: impl IntoIterator<Item = u32>) -> Option<usize> {
    let mut stage_ids = stage_ids.into_iter();
    let first = stage_ids.next()?;
    stage_ids
        .all(|stage_id| stage_id == first)
        .then_some(first as usize)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use ballista_core::error::Result;
    use ballista_core::serde::protobuf::{SuccessfulJob, job_status};
    use datafusion::arrow::array::{ArrayRef, Int32Array, RecordBatch};
    use datafusion::arrow::datatypes::Schema;

    use super::output_stage_id;
    use crate::config::SchedulerConfig;
    use crate::test_utils::{SchedulerTest, TestMetricsCollector};

    #[test]
    fn output_stage_id_needs_one_agreed_stage() {
        assert_eq!(output_stage_id([]), None);
        assert_eq!(output_stage_id([3, 3, 3]), Some(3));
        assert_eq!(output_stage_id([3, 2]), None);
    }

    // Under the adaptive planner every stage has empty `output_links`, so the
    // result schema has to come from the stage named by the output locations
    // rather than from whichever stage the stage map yields first.
    #[tokio::test]
    async fn result_schema_is_the_final_stage_schema_under_adaptive_planner() -> Result<()>
    {
        let mut test = SchedulerTest::new(
            SchedulerConfig::default(),
            Arc::new(TestMetricsCollector::default()),
            4,
            1,
            None,
        )
        .await?;
        test.set_adaptive_planner(true);

        let ctx = test.ctx().await?;
        let batch = RecordBatch::try_from_iter([
            (
                "a",
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef,
            ),
            (
                "c",
                Arc::new(Int32Array::from(vec![1, 1, 2, 2])) as ArrayRef,
            ),
        ])?;
        ctx.register_batch("t", batch)?;
        let plan = ctx
            .sql("SELECT c, min(a) FROM t GROUP BY c")
            .await?
            .logical_plan()
            .clone();
        let (status, job_id) = test.run("job", &plan).await?;
        let Some(job_status::Status::Successful(SuccessfulJob {
            partition_location,
            ..
        })) = status.status
        else {
            panic!("job did not succeed: {status:?}");
        };

        let output_stage = output_stage_id(
            partition_location
                .iter()
                .filter_map(|location| location.partition_id.as_ref())
                .map(|partition_id| partition_id.stage_id),
        );
        assert!(
            output_stage.is_some(),
            "no output stage in {partition_location:?}"
        );

        let scheduler = test.scheduler();
        let graph = scheduler
            .state
            .task_manager
            .get_job_execution_graph(&job_id)
            .await?
            .expect("graph of the finished job");
        let stages = graph.stages();
        // The ambiguity this guards against: more than one stage looks final.
        assert!(
            stages
                .values()
                .filter(|stage| stage.output_links().is_empty())
                .count()
                > 1
        );

        let expected = ctx.state().create_physical_plan(&plan).await?.schema();
        let schema = scheduler
            .result_schema(&job_id, &ctx, &plan, output_stage)
            .await?;
        assert_eq!(field_names(&schema), field_names(&expected));

        // Every stage that is not the output stage has a different schema
        // (the partial aggregate), which the old lookup could return.
        for (stage_id, stage) in stages {
            if Some(*stage_id) != output_stage {
                assert_ne!(field_names(&stage.plan().schema()), field_names(&expected));
            }
        }
        Ok(())
    }

    fn field_names(schema: &Schema) -> Vec<String> {
        schema.fields().iter().map(|f| f.name().clone()).collect()
    }
}
