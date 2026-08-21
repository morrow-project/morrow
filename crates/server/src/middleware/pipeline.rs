use super::{host::*, types::*};
use protocol::subject::SubjectTrie;
use std::{
    sync::{Arc, RwLock},
    time::Instant,
};
use wasmtime::{Config, Engine, Linker, Module, Store};

#[derive(Clone)]
struct CompiledMiddleware {
    manifest: Arc<MiddlewareManifest>,
    module: Module,
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

#[derive(Clone)]
pub struct MiddlewareRuntime {
    engine: Engine,
    registry: Arc<RwLock<Registry>>,
}

impl MiddlewareRuntime {
    pub fn new() -> Result<Self, MiddlewareError> {
        let mut config = Config::new();
        config.consume_fuel(true);
        let engine = Engine::new(&config).map_err(runtime_error)?;
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
        })
    }

    pub fn install(
        &self,
        modules: Vec<(MiddlewareManifest, Vec<u8>)>,
    ) -> Result<u64, MiddlewareError> {
        let mut compiled = Vec::with_capacity(modules.len());
        let mut interests = SubjectTrie::default();
        for (index, (manifest, bytes)) in modules.into_iter().enumerate() {
            if !protocol::subject::validate_subscription(&manifest.subject) {
                return Err(MiddlewareError(format!(
                    "middleware {} has an invalid subject scope",
                    manifest.name
                )));
            }
            let module = Module::new(&self.engine, bytes).map_err(runtime_error)?;
            interests.insert(&manifest.subject, index);
            compiled.push(CompiledMiddleware {
                manifest: Arc::new(manifest),
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
            match self.execute(middleware, stage, message.clone()) {
                Ok((decision, updated, mut secondary)) => {
                    message = updated;
                    emitted.append(&mut secondary);
                    if decision != MiddlewareDecision::Continue {
                        return Ok(MiddlewareOutcome {
                            generation: generation.id,
                            decision,
                            message,
                            emitted,
                        });
                    }
                }
                Err(err) => {
                    return self.failure(
                        middleware.manifest.failure_policy,
                        generation.id,
                        message,
                        emitted,
                        &err.0,
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
    ) -> Result<(MiddlewareDecision, MiddlewareMessage, Vec<EmittedMessage>), MiddlewareError> {
        let state = HostState::new(middleware.manifest.clone(), stage, message);
        let mut store = Store::new(&self.engine, state);
        store.limiter(|state| &mut state.limits);
        store
            .set_fuel(middleware.manifest.budget.max_fuel)
            .map_err(runtime_error)?;
        let mut linker = Linker::new(&self.engine);
        add_host_functions(&mut linker).map_err(runtime_error)?;
        let instance = linker
            .instantiate(&mut store, &middleware.module)
            .map_err(runtime_error)?;
        let process = instance
            .get_typed_func::<i32, i32>(&mut store, "process")
            .map_err(runtime_error)?;
        let started = Instant::now();
        let code = process
            .call(&mut store, stage.code())
            .map_err(runtime_error)?;
        if started.elapsed() > middleware.manifest.budget.deadline {
            return Err(MiddlewareError("middleware deadline exceeded".to_string()));
        }
        let state = store.into_data();
        if let Some(denied) = state.denied {
            return Err(MiddlewareError(denied));
        }
        if state.message.size()
            > state
                .initial_size
                .saturating_add(state.manifest.budget.max_output_growth_bytes)
        {
            return Err(MiddlewareError(
                "middleware output-growth budget exceeded".to_string(),
            ));
        }
        let decision = match code {
            0 => MiddlewareDecision::Continue,
            1 => MiddlewareDecision::Drop,
            2 => MiddlewareDecision::Reject,
            _ => return Err(MiddlewareError("invalid middleware decision".to_string())),
        };
        Ok((decision, state.message, state.emitted))
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
