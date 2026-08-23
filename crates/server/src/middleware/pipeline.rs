use super::{host::*, types::*};
use protocol::subject::SubjectTrie;
use std::{
    sync::{
        Arc, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use wasmtime::{
    Config, Engine, ExternType, InstancePre, Linker, Module, ModuleExport, PoolingAllocationConfig,
    Store,
};

const DEFAULT_EXECUTION_POOL_SIZE: u32 = 64;
const MAX_POOLED_MEMORY_BYTES: usize = 256 * 1024 * 1024;

#[derive(Clone)]
struct CompiledMiddleware {
    manifest: Arc<MiddlewareManifest>,
    instance: InstancePre<HostState>,
    process_export: ModuleExport,
    #[cfg(test)]
    module: Module,
}

#[derive(Clone, Copy)]
enum InstantiationPath {
    Prepared,
    #[cfg(test)]
    Unprepared,
}

struct PipelineGeneration {
    id: u64,
    modules: Vec<CompiledMiddleware>,
    interests: SubjectTrie<usize>,
}

struct Registry {
    current: Arc<PipelineGeneration>,
    previous: Option<Arc<PipelineGeneration>>,
}

struct ExecutionFailure {
    error: MiddlewareError,
    message: MiddlewareMessage,
}

#[derive(Clone)]
pub struct MiddlewareRuntime {
    engine: Engine,
    registry: Arc<RwLock<Registry>>,
    _ticker_lifetime: Arc<()>,
    metrics: Arc<MiddlewareMetrics>,
}

#[derive(Default)]
struct MiddlewareMetrics {
    executions_total: AtomicU64,
    drops_total: AtomicU64,
    rejects_total: AtomicU64,
    failures_total: AtomicU64,
}

impl MiddlewareRuntime {
    pub fn new() -> Result<Self, MiddlewareError> {
        Self::with_pool_size(DEFAULT_EXECUTION_POOL_SIZE)
    }

    fn with_pool_size(pool_size: u32) -> Result<Self, MiddlewareError> {
        let mut config = Config::new();
        config.consume_fuel(true);
        config.epoch_interruption(true);
        let mut pool = PoolingAllocationConfig::new();
        pool.total_core_instances(pool_size)
            .total_memories(pool_size)
            .max_memory_size(MAX_POOLED_MEMORY_BYTES)
            .max_unused_warm_slots(pool_size)
            .linear_memory_keep_resident(0);
        config.allocation_strategy(pool);
        let engine = Engine::new(&config).map_err(runtime_error)?;
        let ticker_lifetime = Arc::new(());
        let ticker_guard = Arc::downgrade(&ticker_lifetime);
        let ticker_engine = engine.clone();
        std::thread::Builder::new()
            .name("morrow-wasm-deadline".to_string())
            .spawn(move || {
                while ticker_guard.upgrade().is_some() {
                    std::thread::sleep(Duration::from_millis(1));
                    ticker_engine.increment_epoch();
                }
            })
            .map_err(runtime_error)?;
        let empty = Arc::new(PipelineGeneration {
            id: 0,
            modules: Vec::new(),
            interests: SubjectTrie::default(),
        });
        Ok(Self {
            engine,
            registry: Arc::new(RwLock::new(Registry {
                current: empty,
                previous: None,
            })),
            _ticker_lifetime: ticker_lifetime,
            metrics: Arc::new(MiddlewareMetrics::default()),
        })
    }

    pub fn install(
        &self,
        modules: Vec<(MiddlewareManifest, Vec<u8>)>,
    ) -> Result<u64, MiddlewareError> {
        let mut compiled = Vec::with_capacity(modules.len());
        let mut interests = SubjectTrie::default();
        let mut linker = Linker::new(&self.engine);
        add_host_functions(&mut linker).map_err(runtime_error)?;
        for (index, (manifest, bytes)) in modules.into_iter().enumerate() {
            if !protocol::subject::validate_subscription(&manifest.subject) {
                return Err(MiddlewareError(format!(
                    "middleware {} has an invalid subject scope",
                    manifest.name
                )));
            }
            if manifest.budget.max_memory_bytes > MAX_POOLED_MEMORY_BYTES {
                return Err(MiddlewareError(format!(
                    "middleware {} memory budget exceeds the execution pool limit",
                    manifest.name
                )));
            }
            let module = Module::new(&self.engine, bytes).map_err(runtime_error)?;
            let process_export = validate_process_export(&manifest, &module)?;
            let instance = linker.instantiate_pre(&module).map_err(runtime_error)?;
            interests.insert(&manifest.subject, index);
            compiled.push(CompiledMiddleware {
                manifest: Arc::new(manifest),
                instance,
                process_export,
                #[cfg(test)]
                module,
            });
        }
        let mut registry = self.registry.write().unwrap();
        let generation = registry.current.id.saturating_add(1);
        let next = Arc::new(PipelineGeneration {
            id: generation,
            modules: compiled,
            interests,
        });
        registry.previous = Some(registry.current.clone());
        registry.current = next;
        Ok(generation)
    }

    pub fn rollback(&self) -> Result<u64, MiddlewareError> {
        let mut registry = self.registry.write().unwrap();
        let previous = registry
            .previous
            .take()
            .ok_or_else(|| MiddlewareError("no middleware generation to roll back".to_string()))?;
        let old = std::mem::replace(&mut registry.current, previous);
        registry.previous = Some(old);
        Ok(registry.current.id)
    }

    pub fn current_generation(&self) -> u64 {
        self.registry.read().unwrap().current.id
    }

    pub fn metrics_snapshot(&self) -> (u64, u64, u64, u64) {
        (
            self.metrics.executions_total.load(Ordering::Relaxed),
            self.metrics.drops_total.load(Ordering::Relaxed),
            self.metrics.rejects_total.load(Ordering::Relaxed),
            self.metrics.failures_total.load(Ordering::Relaxed),
        )
    }

    pub fn process(
        &self,
        stage: MiddlewareStage,
        message: MiddlewareMessage,
        recursion_depth: usize,
    ) -> Result<MiddlewareOutcome, MiddlewareError> {
        let generation = self.registry.read().unwrap().current.clone();
        let mut message = message;
        let mut emitted = Vec::new();
        for index in generation.interests.matching(&message.subject) {
            let middleware = &generation.modules[index];
            if middleware.manifest.stage != stage {
                continue;
            }
            if recursion_depth > middleware.manifest.budget.max_recursion_depth {
                return self.failure(
                    middleware.manifest.failure_policy,
                    generation.id,
                    message,
                    emitted,
                    "middleware recursion budget exceeded",
                );
            }
            let span = tracing::info_span!(
                "morrow.middleware",
                generation = generation.id,
                stage = stage.code(),
            );
            let _entered = span.enter();
            match self.execute(middleware, stage, message, InstantiationPath::Prepared) {
                Ok((decision, updated, mut secondary)) => {
                    self.metrics
                        .executions_total
                        .fetch_add(1, Ordering::Relaxed);
                    message = updated;
                    emitted.append(&mut secondary);
                    if decision != MiddlewareDecision::Continue {
                        match decision {
                            MiddlewareDecision::Drop => {
                                self.metrics.drops_total.fetch_add(1, Ordering::Relaxed);
                            }
                            MiddlewareDecision::Reject => {
                                self.metrics.rejects_total.fetch_add(1, Ordering::Relaxed);
                            }
                            MiddlewareDecision::Continue => {}
                        }
                        return Ok(MiddlewareOutcome {
                            generation: generation.id,
                            decision,
                            message,
                            emitted,
                        });
                    }
                }
                Err(failure) => {
                    self.metrics.failures_total.fetch_add(1, Ordering::Relaxed);
                    message = failure.message;
                    return self.failure(
                        middleware.manifest.failure_policy,
                        generation.id,
                        message,
                        emitted,
                        &failure.error.0,
                    );
                }
            }
        }
        Ok(MiddlewareOutcome {
            generation: generation.id,
            decision: MiddlewareDecision::Continue,
            message,
            emitted,
        })
    }

    fn execute(
        &self,
        middleware: &CompiledMiddleware,
        stage: MiddlewareStage,
        message: MiddlewareMessage,
        instantiation_path: InstantiationPath,
    ) -> Result<(MiddlewareDecision, MiddlewareMessage, Vec<EmittedMessage>), ExecutionFailure>
    {
        let state = HostState::new(middleware.manifest.clone(), stage, message);
        let mut store = Store::new(&self.engine, state);
        store.limiter(|state| &mut state.limits);
        if let Err(error) = store.set_fuel(middleware.manifest.budget.max_fuel) {
            return Err(ExecutionFailure {
                error: runtime_error(error),
                message: store.into_data().original(),
            });
        }
        let deadline_ticks = middleware
            .manifest
            .budget
            .deadline
            .as_nanos()
            .div_ceil(1_000_000)
            .max(1)
            .min(u128::from(u64::MAX)) as u64;
        store.set_epoch_deadline(deadline_ticks);
        store.epoch_deadline_trap();
        let started = Instant::now();
        let execution = (|| {
            let instance = match instantiation_path {
                InstantiationPath::Prepared => middleware
                    .instance
                    .instantiate(&mut store)
                    .map_err(instantiation_error)?,
                #[cfg(test)]
                InstantiationPath::Unprepared => {
                    let mut linker = Linker::new(&self.engine);
                    add_host_functions(&mut linker).map_err(runtime_error)?;
                    linker
                        .instantiate(&mut store, &middleware.module)
                        .map_err(instantiation_error)?
                }
            };
            let process = instance
                .get_module_export(&mut store, &middleware.process_export)
                .and_then(wasmtime::Extern::into_func)
                .ok_or_else(|| MiddlewareError("prepared process export is missing".to_string()))?
                .typed::<i32, i32>(&store)
                .map_err(runtime_error)?;
            let code = process
                .call(&mut store, stage.code())
                .map_err(runtime_error)?;
            if started.elapsed() > middleware.manifest.budget.deadline {
                return Err(MiddlewareError("middleware deadline exceeded".to_string()));
            }
            Ok(code)
        })();
        let state = store.into_data();
        let code = match execution {
            Ok(code) => code,
            Err(error) => {
                return Err(ExecutionFailure {
                    error,
                    message: state.original(),
                });
            }
        };
        if let Some(error) = state.denied.clone() {
            return Err(ExecutionFailure {
                error: MiddlewareError(error),
                message: state.original(),
            });
        }
        if state.message_size()
            > state
                .initial_size
                .saturating_add(middleware.manifest.budget.max_output_growth_bytes)
        {
            return Err(ExecutionFailure {
                error: MiddlewareError("middleware output-growth budget exceeded".to_string()),
                message: state.original(),
            });
        }
        let decision = match code {
            0 => MiddlewareDecision::Continue,
            1 => MiddlewareDecision::Drop,
            2 => MiddlewareDecision::Reject,
            _ => {
                return Err(ExecutionFailure {
                    error: MiddlewareError("invalid middleware decision".to_string()),
                    message: state.original(),
                });
            }
        };
        let (message, emitted, _, _) = state.finish();
        Ok((decision, message, emitted))
    }

    #[cfg(test)]
    pub(super) fn with_pool_size_for_test(pool_size: u32) -> Result<Self, MiddlewareError> {
        Self::with_pool_size(pool_size)
    }

    #[cfg(test)]
    pub(super) fn occupy_execution_slot_for_test(
        &self,
    ) -> Result<Store<HostState>, MiddlewareError> {
        let generation = self.registry.read().unwrap().current.clone();
        let middleware = generation
            .modules
            .first()
            .ok_or_else(|| MiddlewareError("no middleware installed".to_string()))?;
        let mut store = Store::new(
            &self.engine,
            HostState::new(
                middleware.manifest.clone(),
                middleware.manifest.stage,
                MiddlewareMessage {
                    subject: "orders/pool-test".to_string(),
                    key: None,
                    headers: Vec::new(),
                    payload: Vec::new(),
                    reply_to: None,
                },
            ),
        );
        middleware
            .instance
            .instantiate(&mut store)
            .map_err(instantiation_error)?;
        Ok(store)
    }

    #[cfg(test)]
    pub(super) fn process_unprepared_for_test(
        &self,
        stage: MiddlewareStage,
        message: MiddlewareMessage,
    ) -> Result<MiddlewareOutcome, MiddlewareError> {
        let generation = self.registry.read().unwrap().current.clone();
        let middleware = generation
            .modules
            .first()
            .ok_or_else(|| MiddlewareError("no middleware installed".to_string()))?;
        self.execute(middleware, stage, message, InstantiationPath::Unprepared)
            .map(|(decision, message, emitted)| MiddlewareOutcome {
                generation: generation.id,
                decision,
                message,
                emitted,
            })
            .map_err(|failure| failure.error)
    }

    fn failure(
        &self,
        policy: FailurePolicy,
        generation: u64,
        message: MiddlewareMessage,
        emitted: Vec<EmittedMessage>,
        error: &str,
    ) -> Result<MiddlewareOutcome, MiddlewareError> {
        match policy {
            FailurePolicy::FailOpen => Ok(MiddlewareOutcome {
                generation,
                decision: MiddlewareDecision::Continue,
                message,
                emitted,
            }),
            FailurePolicy::Drop => Ok(MiddlewareOutcome {
                generation,
                decision: MiddlewareDecision::Drop,
                message,
                emitted,
            }),
            FailurePolicy::FailClosed => Err(MiddlewareError(error.to_string())),
        }
    }
}

impl Default for MiddlewareRuntime {
    fn default() -> Self {
        Self::new().expect("default middleware engine configuration is valid")
    }
}

fn runtime_error(error: impl std::fmt::Display) -> MiddlewareError {
    MiddlewareError(error.to_string())
}

fn instantiation_error(error: impl std::fmt::Display) -> MiddlewareError {
    let error = error.to_string();
    if error.contains("maximum concurrent limit") {
        MiddlewareError("middleware execution pool is busy".to_string())
    } else {
        MiddlewareError(error)
    }
}

fn validate_process_export(
    manifest: &MiddlewareManifest,
    module: &Module,
) -> Result<ModuleExport, MiddlewareError> {
    let Some(ExternType::Func(function)) = module.get_export("process") else {
        return Err(MiddlewareError(format!(
            "middleware {} must export process(i32) -> i32",
            manifest.name
        )));
    };
    let mut parameters = function.params();
    let mut results = function.results();
    if !parameters.next().is_some_and(|value| value.is_i32())
        || parameters.next().is_some()
        || !results.next().is_some_and(|value| value.is_i32())
        || results.next().is_some()
    {
        return Err(MiddlewareError(format!(
            "middleware {} must export process(i32) -> i32",
            manifest.name
        )));
    }
    module.get_export_index("process").ok_or_else(|| {
        MiddlewareError(format!(
            "middleware {} must export process(i32) -> i32",
            manifest.name
        ))
    })
}
