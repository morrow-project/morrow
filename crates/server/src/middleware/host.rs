use super::types::*;
use std::sync::Arc;
use wasmtime::{Caller, Extern, Linker, StoreLimits, StoreLimitsBuilder};

pub(super) struct HostState {
    pub manifest: Arc<MiddlewareManifest>,
    pub stage: MiddlewareStage,
    pub message: MiddlewareMessage,
    pub emitted: Vec<EmittedMessage>,
    pub denied: Option<String>,
    pub allocated: usize,
    pub initial_size: usize,
    pub limits: StoreLimits,
}

impl HostState {
    pub(super) fn new(
        manifest: Arc<MiddlewareManifest>,
        stage: MiddlewareStage,
        message: MiddlewareMessage,
    ) -> Self {
        let initial_size = message.size();
        let limits = StoreLimitsBuilder::new()
            .memory_size(manifest.budget.max_memory_bytes)
            .build();
        Self {
            manifest,
            stage,
            message,
            emitted: Vec::new(),
            denied: None,
            allocated: 0,
            initial_size,
            limits,
        }
    }

    fn require(&mut self, capability: Capability) -> bool {
        if self.manifest.capabilities.contains(&capability) {
            true
        } else {
            self.denied = Some(format!("undeclared capability {capability:?}"));
            false
        }
    }

    fn allocate(&mut self, bytes: usize) -> bool {
        self.allocated = self.allocated.saturating_add(bytes);
        if self.allocated > self.manifest.budget.max_host_allocation_bytes {
            self.denied = Some("host allocation budget exceeded".to_string());
            false
        } else {
            true
        }
    }
}

pub(super) fn add_host_functions(linker: &mut Linker<HostState>) -> wasmtime::Result<()> {
    linker.func_wrap(
        "broker",
        "get-field",
        |mut caller: Caller<'_, HostState>, field: i32, ptr: i32, capacity: i32| -> i32 {
            if !caller.data_mut().require(Capability::ReadMessage) {
                return -1;
            }
            let bytes = match field {
                0 => caller.data().message.subject.as_bytes().to_vec(),
                1 => caller.data().message.key.clone().unwrap_or_default(),
                2 => match serde_json::to_vec(&caller.data().message.headers) {
                    Ok(headers) => headers,
                    Err(_) => return -1,
                },
                3 => caller.data().message.payload.clone(),
                4 => caller
                    .data()
                    .message
                    .reply_to
                    .as_deref()
                    .unwrap_or_default()
                    .as_bytes()
                    .to_vec(),
                _ => {
                    caller.data_mut().denied = Some("unknown message field".to_string());
                    return -1;
                }
            };
            if !caller.data_mut().allocate(bytes.len()) {
                return -1;
            }
            let Ok(capacity) = usize::try_from(capacity) else {
                caller.data_mut().denied = Some("negative guest output capacity".to_string());
                return -1;
            };
            if bytes.len() > capacity || write_guest(&mut caller, ptr, &bytes).is_none() {
                if caller.data().denied.is_none() {
                    caller.data_mut().denied = Some("guest output buffer is too small".to_string());
                }
                return -1;
            }
            i32::try_from(bytes.len()).unwrap_or(-1)
        },
    )?;
    linker.func_wrap(
        "broker",
        "set-field",
        |mut caller: Caller<'_, HostState>, field: i32, ptr: i32, len: i32| -> i32 {
            let Some(bytes) = read_guest(&mut caller, ptr, len) else {
                return -1;
            };
            let state = caller.data_mut();
            let result = match field {
                0 if state.require(Capability::WriteSubject)
                    && state.stage.may_mutate_subject() =>
                {
                    String::from_utf8(bytes)
                        .ok()
                        .filter(|subject| protocol::subject::validate_subject(subject))
                        .map(|subject| state.message.subject = subject)
                }
                1 if state.require(Capability::WriteKey) && state.stage.may_mutate_subject() => {
                    state.message.key = Some(bytes);
                    Some(())
                }
                2 if state.require(Capability::WriteHeaders)
                    && state.stage.may_mutate_persisted_fields() =>
                {
                    serde_json::from_slice(&bytes)
                        .ok()
                        .map(|headers| state.message.headers = headers)
                }
                3 if state.require(Capability::WritePayload)
                    && state.stage.may_mutate_persisted_fields() =>
                {
                    state.message.payload = bytes;
                    Some(())
                }
                _ => None,
            };
            if result.is_none() && state.denied.is_none() {
                state.denied = Some("field mutation is not allowed at this stage".to_string());
            }
            if result.is_some() { 0 } else { -1 }
        },
    )?;
    linker.func_wrap(
        "broker",
        "emit",
        |mut caller: Caller<'_, HostState>,
         subject_ptr: i32,
         subject_len: i32,
         ptr: i32,
         len: i32| {
            let Some(subject) = read_guest(&mut caller, subject_ptr, subject_len)
                .and_then(|bytes| String::from_utf8(bytes).ok())
            else {
                return -1;
            };
            let Some(payload) = read_guest(&mut caller, ptr, len) else {
                return -1;
            };
            let state = caller.data_mut();
            if !state.require(Capability::SecondaryPublish)
                || !protocol::subject::validate_subject(&subject)
                || state.emitted.len() >= state.manifest.budget.max_emitted_messages
            {
                if state.denied.is_none() {
                    state.denied = Some("emitted-message budget exceeded".to_string());
                }
                return -1;
            }
            state.emitted.push(EmittedMessage { subject, payload });
            0
        },
    )?;
    linker.func_wrap(
        "broker",
        "host-call",
        |mut caller: Caller<'_, HostState>, capability: i32| -> i32 {
            let required = match capability {
                3 => Capability::Clock,
                4 => Capability::Random,
                5 => Capability::Telemetry,
                0..=2 => {
                    caller.data_mut().denied =
                        Some("named host capability requires an allow-listed name".to_string());
                    return -1;
                }
                _ => {
                    caller.data_mut().denied = Some("unknown host capability".to_string());
                    return -1;
                }
            };
            if caller.data_mut().require(required) {
                0
            } else {
                -1
            }
        },
    )?;
    linker.func_wrap(
        "broker",
        "named-host-call",
        |mut caller: Caller<'_, HostState>, capability: i32, ptr: i32, len: i32| -> i32 {
            let Some(name) =
                read_guest(&mut caller, ptr, len).and_then(|bytes| String::from_utf8(bytes).ok())
            else {
                return -1;
            };
            let state = caller.data_mut();
            let (required, allowed) = match capability {
                0 => (Capability::NamedKv, state.manifest.named_kv.contains(&name)),
                1 => (Capability::Secrets, state.manifest.secrets.contains(&name)),
                2 => (
                    Capability::AllowListedHttp,
                    state.manifest.http_allow_lists.contains(&name),
                ),
                _ => {
                    state.denied = Some("unknown named host capability".to_string());
                    return -1;
                }
            };
            if !state.require(required) {
                return -1;
            }
            if !allowed {
                state.denied = Some(format!("host resource {name:?} is not allow-listed"));
                return -1;
            }
            0
        },
    )?;
    Ok(())
}

fn write_guest(caller: &mut Caller<'_, HostState>, ptr: i32, bytes: &[u8]) -> Option<()> {
    let ptr = usize::try_from(ptr).ok()?;
    let Extern::Memory(memory) = caller.get_export("memory")? else {
        caller.data_mut().denied = Some("middleware has no exported memory".to_string());
        return None;
    };
    if memory.write(&mut *caller, ptr, bytes).is_err() {
        caller.data_mut().denied = Some("middleware memory access is out of bounds".to_string());
        return None;
    }
    Some(())
}

fn read_guest(caller: &mut Caller<'_, HostState>, ptr: i32, len: i32) -> Option<Vec<u8>> {
    let (ptr, len) = (usize::try_from(ptr).ok()?, usize::try_from(len).ok()?);
    if !caller.data_mut().allocate(len) {
        return None;
    }
    let Extern::Memory(memory) = caller.get_export("memory")? else {
        caller.data_mut().denied = Some("middleware has no exported memory".to_string());
        return None;
    };
    let mut bytes = vec![0; len];
    if memory.read(&*caller, ptr, &mut bytes).is_err() {
        caller.data_mut().denied = Some("middleware memory access is out of bounds".to_string());
        return None;
    }
    Some(bytes)
}
