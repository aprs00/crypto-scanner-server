use crate::{
    config::Exchange,
    model::{Spec, decimal},
    service::{Kind, Request, Service},
};
use axum::{
    Json, Router,
    extract::{
        DefaultBodyLimit, Query, State, WebSocketUpgrade,
        rejection::JsonRejection,
        ws::{Message, WebSocket},
    },
    http::{HeaderMap, HeaderValue, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use futures_util::SinkExt;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::HashSet, time::Duration};
use tower_http::compression::CompressionLayer;
use tower_http::cors::CorsLayer;

pub fn router(service: Service) -> Router {
    let origins: Vec<HeaderValue> = service
        .config
        .origins
        .iter()
        .filter_map(|s| s.parse().ok())
        .collect();
    Router::new()
        .route("/api/v1/footprints/markets", get(markets))
        .route("/api/v1/footprints/chart-series", post(series))
        .route("/api/v1/footprints/chart-bars", get(bars))
        .route("/ws/v1/charts", get(websocket))
        .route("/ws/v1/charts/", get(websocket))
        .layer(DefaultBodyLimit::max(4096))
        .layer(
            CorsLayer::new()
                .allow_origin(origins)
                .allow_methods([Method::GET, Method::POST])
                .allow_headers([axum::http::header::CONTENT_TYPE]),
        )
        .layer(CompressionLayer::new())
        .with_state(service)
}
pub struct ApiError {
    pub code: String,
    pub status: StatusCode,
}
impl From<anyhow::Error> for ApiError {
    fn from(error: anyhow::Error) -> Self {
        let message = error.to_string();
        let (code, status) = match message.as_str() {
            "UNKNOWN_FOOTPRINT_MARKET" => ("UNKNOWN_MARKET", StatusCode::NOT_FOUND),
            "UNKNOWN_CHART_SERIES" => ("UNKNOWN_CHART_SERIES", StatusCode::NOT_FOUND),
            "SERIES_CAPACITY_REACHED" => ("SERIES_CAPACITY_REACHED", StatusCode::TOO_MANY_REQUESTS),
            "CHART_SERIES_NOT_READY" => ("CHART_SERIES_NOT_READY", StatusCode::CONFLICT),
            "CHART_SERIES_REPLACED" => ("CHART_SERIES_REPLACED", StatusCode::CONFLICT),
            "INVALID_LOOKBACK_DAYS" => ("INVALID_LOOKBACK_DAYS", StatusCode::BAD_REQUEST),
            "INVALID_CHART_PARAMETERS" => ("INVALID_CHART_PARAMETERS", StatusCode::BAD_REQUEST),
            "CHART_BAR_TOO_DENSE" => ("CHART_BAR_TOO_DENSE", StatusCode::UNPROCESSABLE_ENTITY),
            "CHART_VALUE_OVERFLOW" => ("CHART_VALUE_OVERFLOW", StatusCode::UNPROCESSABLE_ENTITY),
            _ => {
                tracing::warn!(%error,"chart request unavailable");
                ("CHART_UNAVAILABLE", StatusCode::SERVICE_UNAVAILABLE)
            }
        };
        Self {
            code: code.into(),
            status,
        }
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut response = (
            self.status,
            Json(json!({"code":self.code,"error":self.code})),
        )
            .into_response();
        response
            .headers_mut()
            .insert("cache-control", HeaderValue::from_static("no-store"));
        if self.status == StatusCode::TOO_MANY_REQUESTS {
            response
                .headers_mut()
                .insert("retry-after", HeaderValue::from_static("60"));
        }
        response
    }
}
fn bad(code: &str) -> ApiError {
    ApiError {
        code: code.into(),
        status: StatusCode::BAD_REQUEST,
    }
}
async fn markets(State(service): State<Service>) -> Result<Json<Value>, ApiError> {
    let mut result = Vec::new();
    for exchange in Exchange::ALL {
        let spec = service
            .store
            .latest(exchange.id())
            .await?
            .map(|t| t.spec)
            .unwrap_or_else(|| Spec::initial(exchange));
        result.push(json!({"marketKey":exchange.key(),"exchange":exchange.name(),"symbol":exchange.symbol(),"baseAsset":"BTC","currentTickSize":decimal(spec.tick as u64)}));
    }
    Ok(Json(json!({"markets":result})))
}
pub fn parse_request(value: &Value) -> Result<Request, ApiError> {
    if !value.is_object() {
        return Err(bad("INVALID_JSON"));
    }
    let exchange = Exchange::from_key(value["marketKey"].as_str().unwrap_or_default())?;
    let kind = match value["chartType"].as_str() {
        Some("tick") => Kind::Tick,
        Some("trev") => Kind::Trev,
        _ => return Err(bad("INVALID_CHART_TYPE")),
    };
    let a = value[if kind == Kind::Tick {
        "tradesPerBar"
    } else {
        "continuationTicks"
    }]
    .as_u64()
    .ok_or_else(|| bad("INVALID_CHART_PARAMETERS"))?;
    let b = if kind == Kind::Tick {
        0
    } else {
        value["reversalTicks"]
            .as_u64()
            .ok_or_else(|| bad("INVALID_CHART_PARAMETERS"))?
    };
    let lookback = u32::try_from(
        value["lookbackDays"]
            .as_u64()
            .ok_or_else(|| bad("INVALID_LOOKBACK_DAYS"))?,
    )
    .map_err(|_| bad("INVALID_LOOKBACK_DAYS"))?;
    let request = Request {
        exchange,
        kind,
        a,
        b,
        lookback,
    };
    request.validate()?;
    Ok(request)
}
async fn series(
    State(service): State<Service>,
    payload: Result<Json<Value>, JsonRejection>,
) -> Result<Response, ApiError> {
    let Json(value) = payload.map_err(|e| ApiError {
        code: if e.status() == StatusCode::PAYLOAD_TOO_LARGE {
            "REQUEST_TOO_LARGE"
        } else {
            "INVALID_JSON"
        }
        .into(),
        status: e.status(),
    })?;
    let result = service.request(parse_request(&value)?).await?;
    let status = if matches!(result.status.as_str(), "queued" | "building") {
        StatusCode::ACCEPTED
    } else {
        StatusCode::OK
    };
    let mut response = (status, Json(service.response(&result).await?)).into_response();
    response
        .headers_mut()
        .insert("cache-control", HeaderValue::from_static("no-store"));
    if status == StatusCode::ACCEPTED {
        response
            .headers_mut()
            .insert("retry-after", HeaderValue::from_static("1"));
    }
    Ok(response)
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Range {
    series_key: String,
    before_bar_index: Option<u64>,
    limit: Option<usize>,
    lookback_days: Option<u32>,
}
async fn bars(
    State(service): State<Service>,
    query: Result<Query<Range>, axum::extract::rejection::QueryRejection>,
) -> Result<Response, ApiError> {
    let Query(range) = query.map_err(|_| bad("INVALID_CHART_RANGE"))?;
    let limit = range.limit.unwrap_or(500);
    if !(1..=500).contains(&limit) {
        return Err(bad("INVALID_CHART_RANGE"));
    }
    let lookback = range.lookback_days.unwrap_or(5);
    if !(1..=30).contains(&lookback) {
        return Err(bad("INVALID_LOOKBACK_DAYS"));
    }
    let s = service
        .get(&range.series_key, true)
        .await?
        .ok_or_else(|| ApiError {
            code: "UNKNOWN_CHART_SERIES".into(),
            status: StatusCode::NOT_FOUND,
        })?;
    if !s.active {
        return Err(anyhow::anyhow!("CHART_SERIES_REPLACED").into());
    }
    if s.status != "ready" {
        return Ok((StatusCode::CONFLICT,Json(json!({"code":"CHART_SERIES_NOT_READY","error":"Chart history is building","seriesKey":s.key,"status":s.status,"errorCode":s.error}))).into_response());
    }
    Ok(Json(
        service
            .snapshot(&s, range.before_bar_index, limit, lookback)
            .await?,
    )
    .into_response())
}
async fn websocket(
    State(service): State<Service>,
    headers: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Result<Response, ApiError> {
    if let Some(origin) = headers.get("origin")
        && !origin.to_str().ok().is_some_and(|origin| {
            service
                .config
                .origins
                .iter()
                .any(|allowed| allowed == origin)
        })
    {
        return Err(ApiError {
            code: "ORIGIN_NOT_ALLOWED".into(),
            status: StatusCode::FORBIDDEN,
        });
    }
    Ok(upgrade
        .max_message_size(2048)
        .max_frame_size(2048)
        .on_upgrade(move |socket| session(socket, service)))
}
async fn send(socket: &mut WebSocket, value: &Value) -> bool {
    tokio::time::timeout(
        Duration::from_secs(5),
        socket.send(Message::Text(value.to_string().into())),
    )
    .await
    .is_ok_and(|r| r.is_ok())
}
async fn session(mut socket: WebSocket, service: Service) {
    // Register before subscription acknowledgement. HTTP resnapshot after the
    // acknowledgement closes the initial HTTP-to-WebSocket race.
    let mut updates = service.updates.subscribe();
    let mut keys = HashSet::<String>::new();
    let mut heartbeat = tokio::time::interval(Duration::from_secs(20));
    let mut pong_at = tokio::time::Instant::now();
    loop {
        tokio::select! {
                    _=service.health.stop.cancelled()=>break,
                    _=heartbeat.tick()=>{
                        if pong_at.elapsed()>Duration::from_secs(50){break}
                        if tokio::time::timeout(Duration::from_secs(5),socket.send(Message::Ping(Vec::new().into()))).await.is_err(){break}
                        for key in &keys {if service.get(key,true).await.is_err(){break}}
                    },
                    update=updates.recv()=>match update {
                        Ok(value)=>{if let Some(key)=value["seriesKey"].as_str()&& keys.contains(key){if !send(&mut socket,&value).await{break}
        if value["code"]=="CHART_SERIES_REPLACED"{keys.remove(key);}}},
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_))=>{let _=send(&mut socket,&json!({"type":"error","code":"CHART_RESYNC_REQUIRED"})).await;break},
                        Err(_)=>break,
                    },
                    message=socket.recv()=>{
                        let text=match message {Some(Ok(Message::Text(text)))=>text,Some(Ok(Message::Pong(_)))=>{pong_at=tokio::time::Instant::now();continue},Some(Ok(Message::Ping(bytes)))=>{if socket.send(Message::Pong(bytes)).await.is_err(){break}continue},_=>break};
                        let Ok(value)=serde_json::from_str::<Value>(&text)else{break};let Some(key)=value["seriesKey"].as_str().filter(|k|uuid::Uuid::parse_str(k).is_ok())else{break};
                        let response=match value["op"].as_str() {
                            Some("subscribe")=>{
                                let status=service.get(key,true).await;
                                let error=match status {
                                    Ok(Some(s)) if s.active && s.status=="ready"=>None,
                                    Ok(Some(s)) if !s.active=>Some("CHART_SERIES_REPLACED"),
                                    Ok(Some(_))=>Some("CHART_SERIES_NOT_READY"),
                                    Ok(None)=>Some("UNKNOWN_CHART_SERIES"),Err(_)=>Some("CHART_UNAVAILABLE"),
                                };
                                if let Some(code)=error {json!({"type":"error","seriesKey":key,"code":code})}
                                else if keys.len()>=16 && !keys.contains(key){json!({"type":"error","seriesKey":key,"code":"TOO_MANY_CHART_SUBSCRIPTIONS"})}
                                else {keys.insert(key.into());json!({"type":"subscribed","seriesKey":key})}
                            },
                            Some("unsubscribe")=>{keys.remove(key);continue},
                            _=>json!({"type":"error","seriesKey":key,"code":"UNKNOWN_OPERATION"}),
                        };if !send(&mut socket,&response).await{break}
                    }
                }
    }
    let _ = tokio::time::timeout(Duration::from_secs(1), socket.close()).await;
}
