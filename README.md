# Iceberg Data Generator

A iceberg data generator which can generate specified numbers and sizes of data files, position delete files, and equality delete files based on TOML configuration files for test.

## Usage Workflow

1. **Preparation Phase** - Create table structure and generate data
   ```bash
   cargo run -- -c config.toml 
   ```

2. **Cleanup Phase** - Delete test data (optional)
   ```bash
   cargo run cleanup -- -c config.toml cleanup
   ```

## Configuration File

The application is configured through a `config.toml` file. The format can refer `config.toml`.
