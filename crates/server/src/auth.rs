use async_trait::async_trait;
use axum::{
    extract::{FromRef, FromRequestParts},
    http::{request::Parts, StatusCode},
    response::{IntoResponse, Response},
};
use crate::state::AppState;

#[allow(dead_code)]
pub struct ApiKey(pub String);

#[derive(Debug)]
pub enum AuthError {
    Missing,
    Invalid,
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        let status = StatusCode::UNAUTHORIZED;
        let msg = match self {
            AuthError::Missing => "missing x-api-key header",
            AuthError::Invalid => "invalid API key",
        };
        (status, msg).into_response()
    }
}

#[async_trait]
impl<S> FromRequestParts<S> for ApiKey
where
    S: Send + Sync,
    AppState: FromRef<S>,
{
	type Rejection = AuthError;
    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let app_state = AppState::from_ref(state);

        let header_value = parts
            .headers
            .get("x-api-key")
            .ok_or(AuthError::Missing)?;

        let key_str = header_value.to_str().map_err(|_| AuthError::Invalid)?;
        let key = key_str.to_string();

        if !app_state.api_keys.contains(&key) {
            return Err(AuthError::Invalid);
        }

        Ok(ApiKey(key))
    }
}
