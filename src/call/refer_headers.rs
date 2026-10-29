//! Pluggable transformation of the headers an incoming REFER carries onto the
//! transfer INVITE.
//!
//! By default every custom header received on the REFER (`Header::Other` plus
//! the typed `User-to-User`, with `X-Referred-*` excluded) is forwarded
//! verbatim onto the transfer INVITE. Embeddings that need different
//! behaviour — e.g. resolving carrier routing data through an HTTP service
//! before dialing — install a [`ReferHeaderResolver`] on the
//! [`AppStateBuilder`](crate::app::AppStateBuilder); with no resolver
//! configured the pass-through behaviour is unchanged.

use std::collections::HashMap;

use async_trait::async_trait;

/// Input given to a [`ReferHeaderResolver`].
#[derive(Debug, Clone, Default)]
pub struct ReferHeaderContext {
    /// Session id of the call that received the REFER.
    pub session_id: String,
    /// Target parsed from the REFER's `Refer-To` header.
    pub refer_target: String,
    /// Custom headers carried by the REFER (`Header::Other` + the typed
    /// `User-to-User`, `X-Referred-*` already excluded). With no resolver
    /// installed these are forwarded onto the transfer INVITE unchanged.
    pub headers: HashMap<String, String>,
}

/// Decides which headers the transfer INVITE carries.
///
/// The returned map *replaces* [`ReferHeaderContext::headers`]: implementations
/// that only rewrite a subset must copy the untouched headers through
/// themselves. Returning an empty map strips all extra headers from the
/// transfer INVITE. Errors abort the rewrite and fall back to the
/// pass-through set (the INVITE still goes out with the original headers).
#[async_trait]
pub trait ReferHeaderResolver: Send + Sync {
    async fn resolve(
        &self,
        context: &ReferHeaderContext,
    ) -> anyhow::Result<HashMap<String, String>>;
}

/// Default behaviour: pass every REFER header through unchanged.
#[derive(Debug, Clone, Copy, Default)]
pub struct PassThroughReferHeaders;

#[async_trait]
impl ReferHeaderResolver for PassThroughReferHeaders {
    async fn resolve(
        &self,
        context: &ReferHeaderContext,
    ) -> anyhow::Result<HashMap<String, String>> {
        Ok(context.headers.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A resolver that swaps User-to-User for custom routing headers.
    struct SwapResolver;

    #[async_trait]
    impl ReferHeaderResolver for SwapResolver {
        async fn resolve(
            &self,
            context: &ReferHeaderContext,
        ) -> anyhow::Result<HashMap<String, String>> {
            let mut out = HashMap::new();
            for (k, v) in &context.headers {
                if k.eq_ignore_ascii_case("User-to-User") {
                    out.insert("X-queue".to_string(), "2000".to_string());
                } else {
                    out.insert(k.clone(), v.clone());
                }
            }
            Ok(out)
        }
    }

    #[tokio::test]
    async fn pass_through_returns_headers_unchanged() {
        let mut headers = HashMap::new();
        headers.insert("User-to-User".to_string(), "abc".to_string());
        let ctx = ReferHeaderContext {
            session_id: "s.1".into(),
            refer_target: "sip:100@pbx".into(),
            headers: headers.clone(),
        };
        let resolved = PassThroughReferHeaders.resolve(&ctx).await.unwrap();
        assert_eq!(resolved, headers);
    }

    #[tokio::test]
    async fn custom_resolver_can_replace_headers() {
        let mut headers = HashMap::new();
        headers.insert("User-to-User".to_string(), "abc".to_string());
        headers.insert("X-ACC-Bizdata".to_string(), "keep".to_string());
        let ctx = ReferHeaderContext {
            session_id: "s.1".into(),
            refer_target: "sip:100@pbx".into(),
            headers,
        };
        let resolved = SwapResolver.resolve(&ctx).await.unwrap();
        assert_eq!(resolved.get("X-queue").map(String::as_str), Some("2000"));
        assert_eq!(resolved.get("X-ACC-Bizdata").map(String::as_str), Some("keep"));
        assert!(resolved.get("User-to-User").is_none());
    }
}
