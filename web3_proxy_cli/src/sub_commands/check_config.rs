use std::fs;
use web3_proxy::config::TopConfig;
use web3_proxy::prelude::anyhow;
use web3_proxy::prelude::argh::{self, FromArgs};
use web3_proxy::prelude::tracing::info;

#[derive(FromArgs, PartialEq, Eq, Debug)]
/// Check the config for any problems.
#[argh(subcommand, name = "check_config")]
pub struct CheckConfigSubCommand {
    #[argh(positional)]
    /// path to the configuration toml.
    path: String,
}

impl CheckConfigSubCommand {
    pub async fn main(self) -> anyhow::Result<()> {
        info!("Loading config @ {}", self.path);
        let top_config: String = fs::read_to_string(self.path)?;
        let mut top_config = TopConfig::from_toml_str(&top_config)?;

        top_config.clean();

        info!("config: {:#?}", top_config);

        // TODO: check min_synced_rpcs is a reasonable amount

        if top_config.app.redirect_public_url.is_none() {
            info!("app.redirect_public_url is None. Public users will get an error page instead of a redirect")
        }

        // TODO: print num warnings and have a flag to fail even on warnings
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use web3_proxy::prelude::tokio;

    use super::*;

    #[tokio::test]
    async fn check_example_toml() {
        let path = env::current_dir().expect("path");

        let parent = path.parent().expect("always a parent");

        let config_path = parent.join("config").join("example.toml");

        let config_path_str = config_path.to_str().expect("always a valid path");

        let check_config_command =
            CheckConfigSubCommand::from_args(&["check_config"], &[config_path_str])
                .expect("the command should have run");

        let check_config_result = check_config_command.main().await;

        println!("{:?}", check_config_result);

        check_config_result.expect("the config should pass all checks");
    }
}
