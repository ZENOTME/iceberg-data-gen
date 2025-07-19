use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Int32Array, RecordBatch, StringArray};
use iceberg::{
    arrow::schema_to_arrow_schema,
    spec::{DataFile, NestedField, PrimitiveType, Schema, Type},
    writer::base_writer::{
        equality_delete_writer::EqualityDeleteWriterConfig,
        sort_position_delete_writer::PositionDeleteInput,
    },
};

use crate::{DataGenerator, FileConfig};

pub struct FixSchemaGenerator {
    schema: Schema,

    data_file_config: FileConfig,
    pos_delete_file_config: FileConfig,
    equality_delete_file_config: FileConfig,

    data_files: Vec<DataFile>,
    cur_file: usize,
    cur_row: usize,
    cur_global_row: i64,
}

impl FixSchemaGenerator {
    pub fn new(
        data_file_config: FileConfig,
        pos_delete_file_config: FileConfig,
        equality_delete_file_config: FileConfig,
    ) -> Self {
        let schema = Schema::builder()
            .with_schema_id(1)
            .with_identifier_field_ids(vec![2])
            .with_fields(vec![
                NestedField::required(1, "foo", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::required(2, "bar", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::required(3, "baz", Type::Primitive(PrimitiveType::Boolean)).into(),
            ])
            .build()
            .unwrap();
        Self {
            schema,
            data_file_config,
            pos_delete_file_config,
            equality_delete_file_config,
            data_files: vec![],
            cur_file: 0,
            cur_row: 0,
            cur_global_row: 0,
        }
    }

    fn next_row(&mut self) {
        if self.cur_file >= self.data_files.len() {
            return;
        }
        if self.cur_row >= self.data_files[self.cur_file].record_count() as usize {
            return;
        }
        self.cur_row += 1;
        self.cur_global_row += 1;
        if self.cur_row >= self.data_files[self.cur_file].record_count() as usize {
            self.cur_file += 1;
            self.cur_row = 0;
        }
    }

    fn is_end(&self) -> bool {
        self.cur_file >= self.data_files.len()
    }
}

impl DataGenerator for FixSchemaGenerator {
    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn generate_data_per_file(&mut self, file_nth: usize) -> Option<arrow::array::RecordBatch> {
        let arrow_schema = schema_to_arrow_schema(&self.schema.clone()).unwrap();
        let bias = file_nth * self.data_file_config.rows_per_file as usize;

        let foo_data: Vec<Option<String>> = (0..self.data_file_config.rows_per_file)
            .map(|i| Some((i + bias).to_string()))
            .collect();
        let foo_array = StringArray::from(foo_data);

        let bar_data: Vec<i32> = (0..self.data_file_config.rows_per_file)
            .map(|i| (i + bias) as i32)
            .collect();
        let bar_array = Int32Array::from(bar_data);

        let baz_data: Vec<Option<bool>> = (0..self.data_file_config.rows_per_file)
            .map(|_| Some(true))
            .collect();
        let baz_array = BooleanArray::from(baz_data);

        let columns: Vec<ArrayRef> = vec![
            Arc::new(foo_array),
            Arc::new(bar_array),
            Arc::new(baz_array),
        ];

        let record_batch = RecordBatch::try_new(Arc::new(arrow_schema), columns).unwrap();
        Some(record_batch)
    }

    fn register_data_file(&mut self, data_file: Vec<DataFile>) {
        self.data_files.extend(data_file);
    }

    fn generate_pos_delete_per_file(
        &mut self,
        _file_nth: usize,
    ) -> Option<Vec<PositionDeleteInput>> {
        let mut res = vec![];

        for _ in 0..self.pos_delete_file_config.rows_per_file {
            if self.is_end() {
                break;
            }
            res.push(PositionDeleteInput {
                path: self.data_files[self.cur_file].file_path().to_string(),
                offset: self.cur_row as i64,
            });
            self.next_row();
        }

        Some(res)
    }

    fn equality_delete_ids(&self) -> Vec<i32> {
        vec![1, 2]
    }

    fn generate_equality_delete_per_file(
        &mut self,
        _file_nth: usize,
    ) -> Option<arrow::array::RecordBatch> {
        let equality_delete_config = EqualityDeleteWriterConfig::new(
            self.equality_delete_ids(),
            Arc::new(self.schema.clone()),
            None,
            self.data_files[0].partition_spec_id(),
        )
        .unwrap();
        let equality_delete_schema = equality_delete_config.projected_arrow_schema_ref().clone();

        let mut foo_data: Vec<Option<String>> = vec![];
        let mut bar_data: Vec<i32> = vec![];

        for _ in 0..self.equality_delete_file_config.rows_per_file {
            if self.is_end() {
                break;
            }
            let equality_delete_input = self.cur_global_row;
            foo_data.push(Some(equality_delete_input.to_string()));
            bar_data.push(equality_delete_input as i32);
            self.next_row();
        }
        let record_batch = RecordBatch::try_new(
            equality_delete_schema,
            vec![
                Arc::new(StringArray::from(foo_data)),
                Arc::new(Int32Array::from(bar_data)),
            ],
        ).unwrap();

        Some(record_batch)
    }
}
