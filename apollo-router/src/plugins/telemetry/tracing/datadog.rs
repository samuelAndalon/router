//! Configuration for datadog tracing.
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
            .with_name_mapping(|span, _model_config|{
                if span.name == "request" {
                    return "supergraph.request";
                } else if span.name == "router" {
                    return "supergraph.router";
                } else if span.name == "request" {
                    return "supergraph.parse_query";
                } else if span.name == "supergraph" {
                    return "supergraph.operation";
                } else if span.name == "query_planning" {
                    return "supergraph.query_planning";
                } else if span.name == "execution" {
                    return "supergraph.execute";
                } else if span.name == "fetch" {
                    return "supergraph.fetch";
                } else if span.name == "subgraph" {
                    return "subgraph.operation";
                } else if span.name == "subgraph_request" {
                    return "subgraph.service";
                }
                return "apollo_router";
            })
            .with_resource_mapping(|span, _model_config|{
                if span.name == "request" {
                    let value =
                        span.attributes
                            .get(&Key::from_static_str("http.method"))
                            .unwrap();
                    return match value {
                        Value::String(value) => value.as_str(),
                        _ => span.name.as_ref()
                    }
                } else if span.name == "router" {
                    return span.name.as_ref();
                } else if span.name == "request" {
                    return span.name.as_ref();
                } else if span.name == "supergraph" {
                    let value = span.attributes
                        .get(&Key::from_static_str("graphql.operation.name"))
                        .unwrap();
                    return match value {
                        Value::String(value) => value.as_str(),
                        _ => span.name.as_ref()
                    }
                } else if span.name == "query_planning" {
                    let value = span.attributes
                        .get(&Key::from_static_str("graphql.operation.name"))
                        .unwrap();
                    return match value {
                        Value::String(value) => value.as_str(),
                        _ => span.name.as_ref()
                    }
                } else if span.name == "execution" {
                    return span.name.as_ref();
                } else if span.name == "fetch" {
                    return span.name.as_ref();
                } else if span.name == "subgraph" {
                    let value = span.attributes
                        .get(&Key::from_static_str("graphql.operation.name"))
                        .unwrap();
                    return match value {
                        Value::String(value) => value.as_str(),
                        _ => span.name.as_ref()
                    }
                } else if span.name == "subgraph_request" {
                    let value = span.attributes
                        .get(&Key::from_static_str("apollo.subgraph.name"))
                        .unwrap();
                    return match value {
                        Value::String(value) => value.as_str(),
                        _ => span.name.as_ref()
                    }
                }
                return span.name.as_ref();
            })
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
