use super::*;

impl CliConfig {
    pub fn load(path: impl AsRef<Path>, use_defaults_if_missing: bool) -> Result<Self> {
        let path = path.as_ref();
        let contents = match fs::read_to_string(path) {
            Ok(contents) => contents,
            Err(err) if use_defaults_if_missing && err.kind() == std::io::ErrorKind::NotFound => {
                return Self::from_json(&serde_json::Value::Object(Default::default()));
            }
            Err(err) => {
                return Err(CliError::with_source(
                    format!("reading {}", path.display()),
                    err,
                ));
            }
        };
        let value: serde_json::Value = serde_json::from_str(&contents)
            .map_err(|err| CliError::with_source(format!("parsing {}", path.display()), err))?;
        Self::from_json(&value)
    }

    pub(super) fn with_server(mut self, server: Option<SocketAddr>) -> Self {
        if let Some(server) = server {
            self.server = server;
        }
        self
    }

    pub(super) fn has_transport_security(&self) -> bool {
        self.tls.is_some() || self.auth.is_some()
    }

    pub fn from_json(value: &serde_json::Value) -> Result<Self> {
        if !value.is_object() {
            return Err(CliError::msg("client config must be a JSON object"));
        }
        let server = get_string(value, "server")?
            .unwrap_or(DEFAULT_SERVER)
            .parse()
            .map_err(|err| {
                CliError::with_source("config field server must be a socket address", err)
            })?;
        let max_payload = get_u64(value, "max_payload")?
            .unwrap_or(DEFAULT_MAX_PAYLOAD as u64)
            .try_into()
            .map_err(|err| CliError::with_source("config field max_payload is too large", err))?;
        let tls = parse_tls(value)?;
        let auth = parse_auth(value)?;
        let connect = parse_connect(value)?;
        Ok(Self {
            server,
            max_payload,
            tls,
            auth,
            connect,
        })
    }

    pub(super) async fn connect_client(&self) -> Result<Client> {
        let options = self.client_options()?;
        Ok(Client::connect_with_options(&options).await?)
    }

    pub(super) fn client_options_for(&self, suffix: &str) -> Result<ClientOptions> {
        let mut options = self.client_options()?;
        let durable_id = format!(
            "{}-{suffix}",
            self.connect.durable_id.as_deref().unwrap_or("morrow-cli")
        );
        // Keep the configured authenticated client ID: the server's allowlist
        // may contain that exact ID but not generated benchmark variants.
        // Durable IDs remain unique per worker so concurrent consumers do not
        // contend for the same durable state.
        options.durable_id = Some(durable_id);
        Ok(options)
    }

    pub(super) fn client_options(&self) -> Result<ClientOptions> {
        let auth = self
            .auth
            .as_ref()
            .map(|auth| ClientAuth::from_seed_hex(&auth.client_id, &auth.private_key_seed_hex))
            .transpose()?;
        let tls = self.tls.as_ref().map(|tls| ClientTlsOptions {
            server_name: tls.server_name.clone(),
            ca_cert_file: tls.ca_cert_file.clone(),
        });
        Ok(ClientOptions {
            addr: self.server,
            max_payload: self.max_payload,
            tls,
            auth,
            durable_id: self.connect.durable_id.clone(),
            verbose: self.connect.verbose,
            ack_timeout_ms: self.connect.ack_timeout_ms,
            max_in_flight: self.connect.max_in_flight,
            ack_contract_version: None,
        })
    }
}

pub(super) fn parse_tls(value: &serde_json::Value) -> Result<Option<CliTlsConfig>> {
    let Some(tls) = value.get("tls") else {
        return Ok(None);
    };
    if tls.is_null() {
        return Ok(None);
    }
    let enabled = get_bool(tls, "enabled")?.unwrap_or(false);
    if !enabled {
        return Ok(None);
    }
    let server_name = get_string(tls, "server_name")?
        .unwrap_or("localhost")
        .to_string();
    let ca_cert_file = get_string(tls, "ca_cert_file")?.ok_or_else(|| {
        CliError::msg("config field tls.ca_cert_file is required when TLS is enabled")
    })?;
    Ok(Some(CliTlsConfig {
        server_name,
        ca_cert_file: PathBuf::from(ca_cert_file),
    }))
}

pub(super) fn parse_auth(value: &serde_json::Value) -> Result<Option<CliAuthConfig>> {
    let Some(auth) = value.get("auth") else {
        return Ok(None);
    };
    if auth.is_null() {
        return Ok(None);
    }
    let enabled = get_bool(auth, "enabled")?.unwrap_or(false);
    if !enabled {
        return Ok(None);
    }
    let client_id = get_string(auth, "client_id")?
        .ok_or_else(|| {
            CliError::msg("config field auth.client_id is required when auth is enabled")
        })?
        .to_string();
    let private_key_seed_hex = get_string(auth, "private_key_seed_hex")?
        .ok_or_else(|| {
            CliError::msg("config field auth.private_key_seed_hex is required when auth is enabled")
        })?
        .to_string();
    ClientAuth::from_seed_hex(&client_id, &private_key_seed_hex)?;
    Ok(Some(CliAuthConfig {
        client_id,
        private_key_seed_hex,
    }))
}

pub(super) fn parse_connect(value: &serde_json::Value) -> Result<CliConnectConfig> {
    let connect = value.get("connect").unwrap_or(&serde_json::Value::Null);
    if !connect.is_null() && !connect.is_object() {
        return Err(CliError::msg("config field connect must be an object"));
    }
    Ok(CliConnectConfig {
        durable_id: get_string(connect, "durable_id")?.map(str::to_string),
        verbose: get_bool(connect, "verbose")?.unwrap_or(false),
        ack_timeout_ms: get_u64(connect, "ack_timeout_ms")?.unwrap_or(DEFAULT_ACK_TIMEOUT_MS),
        max_in_flight: get_u64(connect, "max_in_flight")?
            .unwrap_or(DEFAULT_MAX_IN_FLIGHT as u64)
            .try_into()
            .map_err(|err| {
                CliError::with_source("config field connect.max_in_flight is too large", err)
            })?,
    })
}

pub(super) fn get_string<'a>(value: &'a serde_json::Value, key: &str) -> Result<Option<&'a str>> {
    match value.get(key) {
        Some(serde_json::Value::String(value)) => Ok(Some(value)),
        Some(_) => Err(CliError::msg(format!(
            "config field {key} must be a string"
        ))),
        None => Ok(None),
    }
}

pub(super) fn get_u64(value: &serde_json::Value, key: &str) -> Result<Option<u64>> {
    match value.get(key) {
        Some(serde_json::Value::Number(value)) => value
            .as_u64()
            .ok_or_else(|| CliError::msg(format!("config field {key} must be an unsigned integer")))
            .map(Some),
        Some(_) => Err(CliError::msg(format!(
            "config field {key} must be an unsigned integer"
        ))),
        None => Ok(None),
    }
}

pub(super) fn get_bool(value: &serde_json::Value, key: &str) -> Result<Option<bool>> {
    match value.get(key) {
        Some(serde_json::Value::Bool(value)) => Ok(Some(*value)),
        Some(_) => Err(CliError::msg(format!(
            "config field {key} must be a boolean"
        ))),
        None => Ok(None),
    }
}
