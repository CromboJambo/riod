//! Default service profiles for riod-managed wrapped services.

/// Default service profiles written by `riod init`, keyed by service name.
pub const DEFAULT_SERVICE_PROFILES: &[(&str, &str)] = &[(
    "llama-server-flint",
    r#"name = "llama-server-flint"
purpose = "inference"
description = "Flint GGUF inference server via llama.cpp"

[resources]
gpu_count = 1
ram_mb = 24576
cpu_percent = 90
ttl_secs = 3600

[command]
binary = "/home/crombo/.unsloth/llama.cpp/llama-server"
args = [
    "-m",
    "/home/crombo/models/hf/hub/models--unsloth--flint-GGUF/snapshots/20468edfc38ab43d6914cc88b08cbecb05d0fc62/flint-UD-Q3_K_XL.gguf",
    "--port",
    "40153",
]"#,
)];

/// Returns the default service profiles as (name, toml_content) pairs.
pub fn default_service_profiles() -> Vec<(String, String)> {
    let mut out = Vec::new();
    for (name, content) in DEFAULT_SERVICE_PROFILES {
        out.push((name.to_string(), content.to_string()));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_service_profiles_parse() {
        let profiles = default_service_profiles();
        assert_eq!(profiles.len(), 1);
        let (name, content) = &profiles[0];
        assert_eq!(name, "llama-server-flint");
        let value: toml::Value =
            toml::from_str(content).expect("default service profile should parse as valid TOML");
        assert!(value.get("name").is_some());
        assert!(value.get("command").is_some());
        assert!(value.get("resources").is_some());
    }
}
