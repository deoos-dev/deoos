//! Schedule cursors and recoverable fire intents use the same S3 conditional writes as tasks.
use super::*;

type Error = (StatusCode, String);

#[derive(Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
enum Missed {
    Latest,
    Catchup,
}
fn latest() -> Missed {
    Missed::Latest
}
#[derive(Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
enum Overlap {
    Skip,
    Allow,
}
fn skip() -> Overlap {
    Overlap::Skip
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Source {
    Cadence,
    Backfill,
}
#[derive(Clone, Serialize, Deserialize)]
struct Pending {
    task_id: String,
    due_ms: u64,
    source: Source,
    next_ms: u64,
}
#[derive(Clone, Serialize, Deserialize)]
struct Backfill {
    start_ms: u64,
    end_ms: u64,
    next_ms: u64,
    remaining: u64,
}
#[derive(Clone, Serialize, Deserialize)]
struct Schedule {
    version: u32,
    id: String,
    handler: String,
    inputs: Value,
    interval_ms: u64,
    first_due_ms: u64,
    next_due_ms: u64,
    missed: Missed,
    overlap: Overlap,
    max_attempts: u32,
    retry_ms: u64,
    paused: bool,
    active_task: Option<String>,
    pending: Option<Pending>,
    backfill: Option<Backfill>,
    revision: String,
}
#[derive(Deserialize)]
struct Create {
    id: String,
    handler: String,
    #[serde(default)]
    inputs: Value,
    interval_ms: u64,
    first_due_ms: Option<u64>,
    #[serde(default = "latest")]
    missed: Missed,
    #[serde(default = "skip")]
    overlap: Overlap,
    #[serde(default = "default_attempts")]
    max_attempts: u32,
    #[serde(default)]
    retry_ms: u64,
}
#[derive(Deserialize)]
struct BackfillRequest {
    start_ms: u64,
    end_ms: u64,
    #[serde(default = "backfill_limit")]
    limit: u64,
}
fn backfill_limit() -> u64 {
    100
}

fn key(e: &Engine, id: &str) -> Key {
    Key::from(format!("{}/schedules/{id}/state.json", e.prefix))
}
async fn read(e: &Engine, id: &str) -> Result<(Schedule, UpdateVersion), Error> {
    if !valid(id) {
        return Err(bad("invalid schedule id"));
    }
    let response = e.store.get(&key(e, id)).await.map_err(storage)?;
    let version = UpdateVersion {
        e_tag: response.meta.e_tag.clone(),
        version: response.meta.version.clone(),
    };
    let bytes = response.bytes().await.map_err(storage)?;
    let schedule: Schedule = serde_json::from_slice(&bytes).map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "invalid stored schedule".into(),
        )
    })?;
    if schedule.version != PROTOCOL_VERSION {
        return Err(conflict("unsupported stored schedule protocol version"));
    }
    Ok((schedule, version))
}
async fn write(e: &Engine, schedule: &Schedule, mode: PutMode) -> Result<(), Error> {
    let payload = Bytes::from(serde_json::to_vec(schedule).unwrap());
    match e
        .store
        .put_opts(
            &key(e, &schedule.id),
            payload.into(),
            PutOptions {
                mode,
                ..Default::default()
            },
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(error) => {
            if let Ok((actual, _)) = read(e, &schedule.id).await
                && actual.revision == schedule.revision
            {
                return Ok(());
            }
            Err(storage(error))
        }
    }
}
async fn list(e: &Engine) -> Result<Vec<Schedule>, Error> {
    let prefix = Key::from(format!("{}/schedules", e.prefix));
    let expected = format!("{prefix}/");
    let objects: Vec<_> = e
        .store
        .list(Some(&prefix))
        .try_collect()
        .await
        .map_err(storage)?;
    let mut schedules = Vec::new();
    for object in objects {
        let path = object.location.to_string();
        if let Some(id) = path
            .strip_prefix(&expected)
            .and_then(|v| v.strip_suffix("/state.json"))
            .filter(|v| valid(v))
        {
            schedules.push(read(e, id).await?.0);
        }
    }
    Ok(schedules)
}
fn same_definition(existing: &Schedule, request: &Create) -> bool {
    existing.handler == request.handler
        && normalized_json(existing.inputs.clone()) == normalized_json(request.inputs.clone())
        && existing.interval_ms == request.interval_ms
        && request
            .first_due_ms
            .is_none_or(|anchor| anchor == existing.first_due_ms)
        && existing.missed == request.missed
        && existing.overlap == request.overlap
        && existing.max_attempts == request.max_attempts
        && existing.retry_ms == request.retry_ms
}
async fn create(e: &Engine, request: Create) -> Result<Value, Error> {
    if !valid(&request.id)
        || !valid(&request.handler)
        || request.interval_ms == 0
        || request.max_attempts == 0
    {
        return Err(bad(
            "invalid schedule id, handler, interval or attempt budget",
        ));
    }
    after_ms(request.retry_ms)?;
    let anchor = request.first_due_ms.unwrap_or_else(now);
    anchor
        .checked_add(request.interval_ms)
        .ok_or_else(|| bad("schedule timestamp overflow"))?;
    let schedule = Schedule {
        version: PROTOCOL_VERSION,
        id: request.id.clone(),
        handler: request.handler.clone(),
        inputs: request.inputs.clone(),
        interval_ms: request.interval_ms,
        first_due_ms: anchor,
        next_due_ms: anchor,
        missed: request.missed.clone(),
        overlap: request.overlap.clone(),
        max_attempts: request.max_attempts,
        retry_ms: request.retry_ms,
        paused: false,
        active_task: None,
        pending: None,
        backfill: None,
        revision: Uuid::new_v4().to_string(),
    };
    match write(e, &schedule, PutMode::Create).await {
        Ok(()) => Ok(json!(schedule)),
        Err((StatusCode::CONFLICT, _)) => {
            let existing = read(e, &request.id).await?.0;
            if !same_definition(&existing, &request) {
                return Err(conflict(
                    "schedule ID already used with different definition",
                ));
            }
            Ok(json!(existing))
        }
        Err(error) => Err(error),
    }
}
async fn control(e: &Engine, id: &str, paused: bool) -> Result<Value, Error> {
    for _ in 0..16 {
        let (mut schedule, version) = read(e, id).await?;
        if schedule.paused == paused {
            return Ok(json!(schedule));
        }
        schedule.paused = paused;
        schedule.revision = Uuid::new_v4().to_string();
        match write(e, &schedule, PutMode::Update(version)).await {
            Ok(()) => return Ok(json!(schedule)),
            Err((StatusCode::CONFLICT, _)) => continue,
            Err(error) => return Err(error),
        }
    }
    Err(conflict("contention; retry"))
}
fn aligned_at_or_after(schedule: &Schedule, at: u64) -> Result<u64, Error> {
    if at <= schedule.first_due_ms {
        return Ok(schedule.first_due_ms);
    }
    let elapsed = at - schedule.first_due_ms;
    let intervals = elapsed.div_ceil(schedule.interval_ms);
    intervals
        .checked_mul(schedule.interval_ms)
        .and_then(|offset| schedule.first_due_ms.checked_add(offset))
        .ok_or_else(|| bad("backfill timestamp overflow"))
}
async fn backfill(e: &Engine, id: &str, request: BackfillRequest) -> Result<Value, Error> {
    if request.start_ms >= request.end_ms
        || request.end_ms > now()
        || !(1..=1000).contains(&request.limit)
    {
        return Err(bad(
            "backfill requires historical [start_ms,end_ms) and limit 1..1000",
        ));
    }
    for _ in 0..16 {
        let (mut schedule, version) = read(e, id).await?;
        let first = aligned_at_or_after(&schedule, request.start_ms)?;
        if first >= request.end_ms {
            return Err(bad("backfill range contains no scheduled occurrences"));
        }
        let count = (request.end_ms - 1 - first) / schedule.interval_ms + 1;
        if count > request.limit {
            return Err(bad("backfill exceeds requested limit"));
        }
        if let Some(existing) = &schedule.backfill {
            return if existing.start_ms == request.start_ms && existing.end_ms == request.end_ms {
                Ok(json!(schedule))
            } else {
                Err(conflict("another backfill is active"))
            };
        }
        schedule.backfill = Some(Backfill {
            start_ms: request.start_ms,
            end_ms: request.end_ms,
            next_ms: first,
            remaining: count,
        });
        schedule.revision = Uuid::new_v4().to_string();
        match write(e, &schedule, PutMode::Update(version)).await {
            Ok(()) => return Ok(json!(schedule)),
            Err((StatusCode::CONFLICT, _)) => continue,
            Err(error) => return Err(error),
        }
    }
    Err(conflict("contention; retry"))
}
fn fire_id(id: &str, at: u64) -> String {
    let digest = format!("{:x}", Sha256::digest(format!("{id}/{at}").as_bytes()));
    format!("schedule-{}", &digest[..32])
}
async fn active(e: &Engine, schedule: &Schedule) -> Result<bool, Error> {
    let Some(id) = &schedule.active_task else {
        return Ok(false);
    };
    match e.read(id).await {
        Ok((task, _)) => Ok(!["completed", "failed", "cancelled"].contains(&task.status.as_str())),
        Err((StatusCode::NOT_FOUND, _)) => Ok(true),
        Err(error) => Err(error),
    }
}
async fn tick_one(e: &Engine, id: &str) -> Result<(), Error> {
    for _ in 0..16 {
        let (mut schedule, version) = read(e, id).await?;
        if let Some(pending) = schedule.pending.clone() {
            let _ = submit(
                State(e.clone()),
                Json(Submit {
                    id: pending.task_id.clone(),
                    handler: schedule.handler.clone(),
                    inputs: schedule.inputs.clone(),
                    max_attempts: schedule.max_attempts,
                    retry_ms: schedule.retry_ms,
                    schedule: Some(ScheduleRun {
                        id: schedule.id.clone(),
                        scheduled_at: pending.due_ms,
                    }),
                }),
            )
            .await?;
            match pending.source {
                Source::Cadence => schedule.next_due_ms = pending.next_ms,
                Source::Backfill => {
                    let job = schedule
                        .backfill
                        .as_mut()
                        .ok_or_else(|| conflict("pending backfill has no cursor"))?;
                    job.next_ms = pending.next_ms;
                    job.remaining -= 1;
                    if job.remaining == 0 {
                        schedule.backfill = None;
                    }
                }
            }
            schedule.active_task = Some(pending.task_id);
            schedule.pending = None;
            schedule.revision = Uuid::new_v4().to_string();
            match write(e, &schedule, PutMode::Update(version)).await {
                Ok(()) => return Ok(()),
                Err((StatusCode::CONFLICT, _)) => continue,
                Err(error) => return Err(error),
            }
        }
        if schedule.paused {
            return Ok(());
        }
        let busy = schedule.overlap == Overlap::Skip && active(e, &schedule).await?;
        let timestamp = now();
        let candidate = if let Some(job) = &schedule.backfill {
            if busy {
                return Ok(());
            }
            Some((job.next_ms, Source::Backfill))
        } else if schedule.next_due_ms <= timestamp {
            let latest_due = schedule.first_due_ms
                + ((timestamp - schedule.first_due_ms) / schedule.interval_ms)
                    * schedule.interval_ms;
            if busy {
                if schedule.missed == Missed::Catchup {
                    return Ok(());
                }
                schedule.next_due_ms = latest_due
                    .checked_add(schedule.interval_ms)
                    .ok_or_else(|| bad("schedule timestamp overflow"))?;
                None
            } else {
                Some((
                    if schedule.missed == Missed::Latest {
                        latest_due
                    } else {
                        schedule.next_due_ms
                    },
                    Source::Cadence,
                ))
            }
        } else {
            return Ok(());
        };
        if let Some((due_ms, source)) = candidate {
            let next_ms = due_ms
                .checked_add(schedule.interval_ms)
                .ok_or_else(|| bad("schedule timestamp overflow"))?;
            schedule.pending = Some(Pending {
                task_id: fire_id(&schedule.id, due_ms),
                due_ms,
                source,
                next_ms,
            });
        }
        schedule.revision = Uuid::new_v4().to_string();
        match write(e, &schedule, PutMode::Update(version)).await {
            Ok(()) if schedule.pending.is_some() => continue,
            Ok(()) => return Ok(()),
            Err((StatusCode::CONFLICT, _)) => continue,
            Err(error) => return Err(error),
        }
    }
    Err(conflict("contention; retry"))
}
pub(super) async fn tick(e: &Engine, handlers: &[String]) -> Result<(), Error> {
    let prefix = Key::from(format!("{}/schedules", e.prefix));
    let expected = format!("{prefix}/");
    let objects: Vec<_> = e
        .store
        .list(Some(&prefix))
        .try_collect()
        .await
        .map_err(storage)?;
    let candidates: Vec<_> = objects
        .iter()
        .filter_map(|object| {
            object
                .location
                .as_ref()
                .strip_prefix(&expected)
                .and_then(|path| path.strip_suffix("/state.json"))
                .filter(|id| valid(id))
                .map(|id| (id, object))
        })
        .collect();
    let present = candidates.iter().map(|(id, _)| *id).collect();
    e.discovery.lock().unwrap().retain_schedules(&present);
    let mut schedules = Vec::new();
    for (id, object) in candidates {
        if e.discovery
            .lock()
            .unwrap()
            .skip_schedule(id, object, handlers, now())
        {
            continue;
        }
        let (schedule, version) = match read(e, id).await {
            Ok(value) => value,
            Err((StatusCode::NOT_FOUND, _)) => continue,
            Err(error) => return Err(error),
        };
        // Persisted fire intents precede pause and deadlines. Backfills also
        // require tick_one's fresh overlap/cursor checks rather than a hint.
        let dormant_until = if schedule.pending.is_some() {
            0
        } else if schedule.paused {
            u64::MAX
        } else if schedule.backfill.is_some() {
            0
        } else {
            schedule.next_due_ms
        };
        e.discovery.lock().unwrap().remember_schedule(
            id,
            &schedule.handler,
            &version,
            dormant_until,
        );
        if handlers.contains(&schedule.handler) && dormant_until <= now() {
            schedules.push(schedule);
        }
    }
    if !schedules.is_empty() {
        let start = (Uuid::new_v4().as_u128() % schedules.len() as u128) as usize;
        schedules.rotate_left(start);
    }
    for schedule in schedules.iter().take(10) {
        tick_one(e, &schedule.id).await?;
    }
    Ok(())
}
pub(super) async fn dispatch(
    e: &Engine,
    method: &str,
    parts: &[&str],
    data: Value,
) -> Result<Value, Error> {
    let decode = |error: serde_json::Error| bad(&error.to_string());
    if parts.len() == 1 {
        return match method {
            "GET" => Ok(json!({"schedules":list(e).await?})),
            "POST" => create(e, serde_json::from_value(data).map_err(decode)?).await,
            _ => Err(bad("unknown schedule route")),
        };
    }
    let id = parts[1];
    if parts.len() == 2 && method == "GET" {
        return Ok(json!(read(e, id).await?.0));
    }
    if parts.len() == 3 && method == "POST" {
        return match parts[2] {
            "pause" => control(e, id, true).await,
            "resume" => control(e, id, false).await,
            "backfill" => backfill(e, id, serde_json::from_value(data).map_err(decode)?).await,
            _ => Err(bad("unknown schedule route")),
        };
    }
    Err(bad("unknown schedule route"))
}
