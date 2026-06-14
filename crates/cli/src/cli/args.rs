use super::*;

impl Args {
    pub fn parse(args: impl IntoIterator<Item = String>) -> Result<Self> {
        let mut args = args.into_iter();
        let _program = args.next();
        let mut config_path = PathBuf::from(DEFAULT_CONFIG_PATH);
        let mut rest = Vec::new();
        while let Some(arg) = args.next() {
            if arg == "--config" {
                let value = args
                    .next()
                    .ok_or_else(|| CliError::msg("--config requires a path"))?;
                config_path = PathBuf::from(value);
            } else {
                rest.push(arg);
                rest.extend(args);
                break;
            }
        }
        let command = parse_command(rest)?;
        Ok(Self {
            config_path,
            command,
        })
    }
}
