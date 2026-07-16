use async_trait::async_trait;

use crate::runtime::model::RequestId;

#[async_trait]
pub trait RuntimeBoundaryHooks: Send + Sync {
    async fn before_dispatch(&self) {}

    async fn ingress_committed(&self, _request_id: &RequestId) {}

    async fn incorporation_committed(&self, _request_ids: &[RequestId]) {}
}

#[derive(Default)]
pub struct NoopRuntimeBoundaryHooks;

#[async_trait]
impl RuntimeBoundaryHooks for NoopRuntimeBoundaryHooks {}
