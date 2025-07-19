use std::path::Path;
use std::sync::Arc;
use std::{collections::HashMap, fs};

use anyhow::{Result, anyhow};
use arrow::array::RecordBatch;
use clap::{Parser, Subcommand};
use iceberg::arrow::arrow_schema_to_schema;
use iceberg::io::{S3_ACCESS_KEY_ID, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY};
use iceberg::spec::{DataFile, DataFileFormat, Schema};
use iceberg::transaction::Transaction;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::base_writer::equality_delete_writer::{
    EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig,
};
use iceberg::writer::base_writer::sort_position_delete_writer::{
    POSITION_DELETE_SCHEMA, PositionDeleteInput, SortPositionDeleteWriterBuilder,
};
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};

use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use tokio::main;

use crate::fix_schema_generator::FixSchemaGenerator;

mod fix_schema_generator;

#[derive(Parser)]
#[command(name = "iceberg_data_generator")]
#[command(about = "Iceberg Data Generator - Generate and manage Iceberg table data")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,

    /// 配置文件路径
    #[arg(short, long, default_value = "config.toml")]
    config: String,
}

#[derive(Subcommand)]
enum Commands {
    Prepare,
    Cleanup,
}

// 配置结构体
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Config {
    catalog: CatalogConfig,
    table: TableConfig,
    data_files: FileConfig,
    pos_delete_files: FileConfig,
    equality_delete_files: FileConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CatalogConfig {
    #[serde(rename = "type")]
    catalog_type: String,
    uri: String,
    s3_endpoint: String,
    s3_access_key_id: String,
    s3_secret_access_key: String,
    s3_region: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TableConfig {
    namespace: String,
    table_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Copy)]
struct FileConfig {
    rows_per_file: usize,
    file_count: usize,
}

trait DataGenerator {
    fn schema(&self) -> Schema;
    fn equality_delete_ids(&self) -> Vec<i32>;
    fn generate_data_per_file(&mut self, file_nth: usize) -> Option<RecordBatch>;
    fn register_data_file(&mut self, data_file: Vec<DataFile>);
    fn generate_pos_delete_per_file(&mut self, file_nth: usize)
    -> Option<Vec<PositionDeleteInput>>;
    fn generate_equality_delete_per_file(&mut self, file_nth: usize) -> Option<RecordBatch>;
}

struct IcebergDataGeneratorApp {
    config: Config,
    data_generator: Box<dyn DataGenerator>,
}

impl IcebergDataGeneratorApp {
    fn new(config_path: &str) -> Result<Self> {
        let config_content = fs::read_to_string(config_path)?;
        let config: Config = toml::from_str(&config_content)?;
        let data_generator = Box::new(FixSchemaGenerator::new(
            config.data_files,
            config.pos_delete_files,
            config.equality_delete_files,
        ));

        Ok(Self {
            config,
            data_generator,
        })
    }

    async fn generate_data(&mut self, catalog: Arc<dyn Catalog>) -> Result<()> {
        println!("🗑️ Generating Data files...");
        let table = catalog
            .load_table(&TableIdent::new(
                NamespaceIdent::new(self.config.table.namespace.clone()),
                self.config.table.table_name.clone(),
            ))
            .await?;
        let trascation = Transaction::new(&table);
        let mut append_action = trascation.fast_append(None, None, vec![]).unwrap();
        let mut all_data_files = vec![];
        for i in 0..self.config.data_files.file_count {
            println!(
                "Generating data file {} / {}...",
                i + 1,
                self.config.data_files.file_count
            );

            let record_batch = self.data_generator.generate_data_per_file(i).unwrap();

            let parquet_writer_builder = ParquetWriterBuilder::new(
                WriterProperties::default(),
                Arc::new(self.data_generator.schema().clone()),
                table.file_io().clone(),
                DefaultLocationGenerator::new(table.metadata().clone()).unwrap(),
                DefaultFileNameGenerator::new(i.to_string(), None, DataFileFormat::Parquet),
            );
            let data_file_writer_builder = DataFileWriterBuilder::new(
                parquet_writer_builder,
                None,
                table.metadata().default_partition_spec_id(),
            );
            let mut data_file_writer = data_file_writer_builder.build().await?;
            data_file_writer.write(record_batch).await?;
            let data_files = data_file_writer.close().await?;

            all_data_files.extend(data_files.clone());

            append_action.add_data_files(data_files.clone())?;
        }
        let transaction = append_action.apply().await?;
        transaction.commit(&*catalog).await?;
        self.data_generator.register_data_file(all_data_files);
        Ok(())
    }

    async fn generate_pos_delete_data(&mut self, catalog: Arc<dyn Catalog>) -> Result<()> {
        println!("🗑️ Generating Position Delete files...");

        let table = catalog
            .load_table(&TableIdent::new(
                NamespaceIdent::new(self.config.table.namespace.clone()),
                self.config.table.table_name.clone(),
            ))
            .await?;
        let transaction = Transaction::new(&table);
        let mut append_action = transaction.fast_append(None, None, vec![]).unwrap();
        let file_name_generator = DefaultFileNameGenerator::new(
            "pos_delete".to_string(),
            None,
            DataFileFormat::Parquet,
        );
        for i in 0..self.config.pos_delete_files.file_count {
            println!(
                "Generating position delete file {} / {}...",
                i + 1,
                self.config.pos_delete_files.file_count
            );

            let parquet_writer_builder = ParquetWriterBuilder::new(
                WriterProperties::default(),
                POSITION_DELETE_SCHEMA.clone(),
                table.file_io().clone(),
                DefaultLocationGenerator::new(table.metadata().clone()).unwrap(),
                file_name_generator.clone(),
            );

            let pos_delete_writer_builder = SortPositionDeleteWriterBuilder::new(
                parquet_writer_builder,
                self.config.pos_delete_files.rows_per_file,
                None,
                None,
            );

            let mut pos_delete_writer = pos_delete_writer_builder.build().await?;

            let pos_delete_inputs = self.data_generator.generate_pos_delete_per_file(i).unwrap();
            for pos_delete_input in pos_delete_inputs {
                pos_delete_writer.write(pos_delete_input).await?;
            }

            let pos_delete_files = pos_delete_writer.close().await?;

            append_action.add_data_files(pos_delete_files)?;
        }
        let transaction = append_action.apply().await?;
        transaction.commit(&*catalog).await?;

        Ok(())
    }

    async fn generate_equality_delete_data(&mut self, catalog: Arc<dyn Catalog>) -> Result<()> {
        println!("🗑️ Generating Equality Delete files...");

        let table = catalog
            .load_table(&TableIdent::new(
                NamespaceIdent::new(self.config.table.namespace.clone()),
                self.config.table.table_name.clone(),
            ))
            .await?;

        let transaction = Transaction::new(&table);
        let mut append_action = transaction.fast_append(None, None, vec![]).unwrap();
        let file_name_generator = DefaultFileNameGenerator::new(
            "equality_delete".to_string(),
            None,
            DataFileFormat::Parquet,
        );
        for i in 0..self.config.equality_delete_files.file_count {
            println!(
                "Generating equality delete file {} / {}...",
                i + 1,
                self.config.equality_delete_files.file_count
            );

            let equality_delete_config = EqualityDeleteWriterConfig::new(
                self.data_generator.equality_delete_ids(),
                Arc::new(self.data_generator.schema().clone()),
                None,
                table.metadata().default_partition_spec_id(),
            )?;
            let parquet_writer_builder = ParquetWriterBuilder::new(
                WriterProperties::default(),
                Arc::new(
                    arrow_schema_to_schema(&equality_delete_config.projected_arrow_schema_ref())
                        .unwrap(),
                ),
                table.file_io().clone(),
                DefaultLocationGenerator::new(table.metadata().clone()).unwrap(),
                file_name_generator.clone(),
            );
            let equality_delete_writer_builder = EqualityDeleteFileWriterBuilder::new(
                parquet_writer_builder,
                equality_delete_config,
            );
            let mut equality_delete_writer = equality_delete_writer_builder.build().await?;

            let record_batch = self
                .data_generator
                .generate_equality_delete_per_file(i)
                .unwrap();
            equality_delete_writer.write(record_batch).await?;
            let equality_delete_files = equality_delete_writer.close().await?;
            append_action.add_data_files(equality_delete_files)?;
        }
        let transaction = append_action.apply().await?;
        transaction.commit(&*catalog).await?;

        Ok(())
    }

    async fn create_catalog(&self) -> Result<Arc<dyn Catalog>> {
        let catalog_config = RestCatalogConfig::builder()
            .uri(self.config.catalog.uri.clone())
            .props(HashMap::from([
                (
                    S3_ENDPOINT.to_string(),
                    self.config.catalog.s3_endpoint.clone(),
                ),
                (
                    S3_ACCESS_KEY_ID.to_string(),
                    self.config.catalog.s3_access_key_id.clone(),
                ),
                (
                    S3_SECRET_ACCESS_KEY.to_string(),
                    self.config.catalog.s3_secret_access_key.clone(),
                ),
                (S3_REGION.to_string(), self.config.catalog.s3_region.clone()),
            ]))
            .build();
        let catalog = RestCatalog::new(catalog_config);
        Ok(Arc::new(catalog))
    }

    fn print_config(&self) {
        println!("🔧 Starting preparation and data generation...");
        println!("📋 Configuration:");
        println!("   - Catalog type: {}", self.config.catalog.catalog_type);
        println!("   - Catalog URI: {}", self.config.catalog.uri);
        println!("   - S3 Endpoint: {}", self.config.catalog.s3_endpoint);
        println!("   - S3 Region: {}", self.config.catalog.s3_region);
        println!(
            "   - S3 Access Key ID: {}",
            self.config.catalog.s3_access_key_id
        );
        println!(
            "   - S3 Secret Access Key: {}",
            self.config.catalog.s3_secret_access_key
        );
        println!("   - Namespace: {}", self.config.table.namespace);
        println!("   - Table name: {}", self.config.table.table_name);
        println!("📋 Data generation configuration:");
        println!(
            "   - Data files: {} files, {} rows each",
            self.config.data_files.file_count, self.config.data_files.rows_per_file
        );
        println!(
            "   - Position Delete files: {} files, {} rows each",
            self.config.pos_delete_files.file_count, self.config.pos_delete_files.rows_per_file
        );
        println!(
            "   - Equality Delete files: {} files, {} rows each",
            self.config.equality_delete_files.file_count,
            self.config.equality_delete_files.rows_per_file
        );
    }

    fn print_summary(&self) {
        println!("🎉 Data generation completed!");
        println!("📊 Summary:");
        println!(
            "   - Total rows: {}",
            self.config.data_files.file_count * self.config.data_files.rows_per_file
                - self.config.pos_delete_files.file_count
                    * self.config.pos_delete_files.rows_per_file
                - self.config.equality_delete_files.file_count
                    * self.config.equality_delete_files.rows_per_file
        );
    }

    async fn prepare(&mut self) -> Result<()> {
        self.print_config();

        let catalog = self.create_catalog().await?;

        println!("🔧 Creating namespace and table...");
        let namespace = NamespaceIdent::new(self.config.table.namespace.clone());
        catalog.create_namespace(&namespace, HashMap::new()).await?;
        println!("🔧 Namespace created");
        let table = TableIdent::new(namespace.clone(), self.config.table.table_name.clone());
        if !catalog.table_exists(&table).await? {
            catalog
                .create_table(
                    &namespace,
                    TableCreation::builder()
                        .name(self.config.table.table_name.clone())
                        .schema(self.data_generator.schema().clone())
                        .build(),
                )
                .await?;
        }
        println!("🔧 Table created");

        self.generate_data(catalog.clone()).await?;
        self.generate_pos_delete_data(catalog.clone()).await?;
        self.generate_equality_delete_data(catalog.clone()).await?;

        self.print_summary();

        Ok(())
    }

    async fn cleanup(&self) -> Result<()> {
        println!("🧹 Starting cleanup...");
        println!("📋 Target:");
        println!(
            "   - Table: {}.{}",
            self.config.table.namespace, self.config.table.table_name
        );

        let catalog = self.create_catalog().await?;

        let table = TableIdent::new(
            NamespaceIdent::new(self.config.table.namespace.clone()),
            self.config.table.table_name.clone(),
        );
        catalog.drop_table(&table).await?;

        catalog.drop_namespace(&table.namespace).await?;

        Ok(())
    }
}

#[main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    if !Path::new(&cli.config).exists() {
        eprintln!("❌ Configuration file {} does not exist", cli.config);
        eprintln!(
            "💡 Please refer to the config.toml example file in the project to create configuration"
        );
        return Err(anyhow!("Configuration file not found"));
    }

    let mut app = IcebergDataGeneratorApp::new(&cli.config)?;

    match &cli.command {
        Some(Commands::Prepare) | None => {
            app.prepare().await?;
        }
        Some(Commands::Cleanup) => {
            app.cleanup().await?;
        }
    }

    Ok(())
}
