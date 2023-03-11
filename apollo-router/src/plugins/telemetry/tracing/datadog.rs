//! Configuration for datadog tracing.
use lazy_static::lazy_static;
use std::collections::HashMap;
use opentelemetry::Key;
use opentelemetry::Value;
use opentelemetry::sdk::trace::BatchSpanProcessor;
use opentelemetry::sdk::trace::Builder;
use schemars::JsonSchema;
use serde::Deserialize;
use serde::Serialize;
use tower::BoxError;

use super::agent_endpoint;
use super::deser_endpoint;
use super::AgentEndpoint;
use crate::plugins::telemetry::config::GenericWith;
use crate::plugins::telemetry::config::Trace;
use crate::plugins::telemetry::tracing::BatchProcessorConfig;
use crate::plugins::telemetry::tracing::SpanProcessorExt;
use crate::plugins::telemetry::tracing::TracingConfigurator;

lazy_static! {
    static ref SPAN_NAME_MAPPING: HashMap<&'static str, &'static str> = {
        let mut map = HashMap::new();
        map.insert("request", "supergraph.request");
        map.insert("router", "supergraph.router");
        map.insert("parse_query", "supergraph.parse_query");
        map.insert("supergraph", "supergraph.operation");
        map.insert("query_planning", "supergraph.query_planning");
        map.insert("execution", "supergraph.execute");
        map.insert("fetch", "supergraph.fetch");
        map.insert("subgraph", "subgraph.operation");
        map.insert("subgraph_request", "subgraph.request");
        map
    };

    static ref SPAN_RESOURCE_ATTRIBUTE_MAPPING: HashMap<&'static str, &'static str> = {
        let mut map = HashMap::new();
        map.insert("request", "http.route");
        map.insert("supergraph", "graphql.operation.name");
        map.insert("query_planning", "graphql.operation.name");
        map.insert("subgraph", "graphql.operation.name");
        map.insert("subgraph_request", "graphql.operation.name");
        map
    };
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct Config {
    /// The endpoint to send to
    #[serde(deserialize_with = "deser_endpoint")]
    #[schemars(schema_with = "agent_endpoint")]
    pub(crate) endpoint: AgentEndpoint,

    /// batch processor configuration
    #[serde(default)]
    pub(crate) batch_processor: BatchProcessorConfig,
}

impl TracingConfigurator for Config {
    fn apply(&self, builder: Builder, trace_config: &Trace) -> Result<Builder, BoxError> {
        tracing::info!("configuring Datadog tracing: {}", self.batch_processor);
        let url = match &self.endpoint {
            AgentEndpoint::Default(_) => None,
            AgentEndpoint::Url(s) => Some(s),
        };
        let exporter = opentelemetry_datadog::new_pipeline()
            .with(&url, |b, e| {
                b.with_agent_endpoint(e.to_string().trim_end_matches('/'))
            })
            .with_service_name(trace_config.service_name.clone())
            .with_name_mapping(|span, _model_config|
                SPAN_NAME_MAPPING.get(span.name.as_ref())
                    .unwrap_or(&"apollo_router")
            )
            .with_resource_mapping(|span, _model_config|
                SPAN_RESOURCE_ATTRIBUTE_MAPPING
                    .get(span.name.as_ref())
                    .and_then(|key| span.attributes.get(&Key::from_static_str(key)))
                    .and_then(|value| match value {
                        Value::String(value) => Some(value.as_str()),
                        _ => None,
                    })
                    .unwrap_or(span.name.as_ref())

            )
            .with_trace_config(trace_config.into())
            .build_exporter()?;

        Ok(builder.with_span_processor(
            BatchSpanProcessor::builder(exporter, opentelemetry::runtime::Tokio)
                .with_batch_config(self.batch_processor.clone().into())
                .build()
                .filtered(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use reqwest::Url;

    use super::*;
    use crate::plugins::telemetry::tracing::AgentDefault;

    #[test]
    fn endpoint_configuration() {
        let config: Config = serde_yaml::from_str("endpoint: default").unwrap();
        assert_eq!(
            AgentEndpoint::Default(AgentDefault::Default),
            config.endpoint
        );

        let config: Config = serde_yaml::from_str("endpoint: collector:1234").unwrap();
        assert_eq!(
            AgentEndpoint::Url(Url::parse("http://collector:1234").unwrap()),
            config.endpoint
        );

        let config: Config = serde_yaml::from_str("endpoint: https://collector:1234").unwrap();
        assert_eq!(
            AgentEndpoint::Url(Url::parse("https://collector:1234").unwrap()),
            config.endpoint
        );
    }
}
