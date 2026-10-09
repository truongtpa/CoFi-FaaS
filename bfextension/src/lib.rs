use duckdb::vscalar::{VScalar, ScalarFunctionSignature};
use duckdb::core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId};
use duckdb::vtab::arrow::WritableVector;
use duckdb::Connection;
use fastbloom_rs::{BloomFilter, FilterBuilder, Membership};
use libduckdb_sys::duckdb_string_t;
use std::sync::{Arc, RwLock};

lazy_static::lazy_static! {
    static ref GLOBAL_BF: RwLock<Option<Arc<BloomFilter>>> = RwLock::new(None);
}

unsafe fn read_duck_str(raw: &duckdb_string_t) -> &str {
    unsafe {
        let len = raw.value.inlined.length as usize;
        let ptr = if len <= 12 {
            raw.value.inlined.inlined.as_ptr() as *const u8
        } else {
            raw.value.pointer.ptr as *const u8
        };
        std::str::from_utf8_unchecked(std::slice::from_raw_parts(ptr, len))
    }
}

fn make_conn() -> Result<Connection, Box<dyn std::error::Error>> {
    let conn = Connection::open_in_memory()?;
    conn.execute_batch("INSTALL parquet; LOAD parquet;")?;
    Ok(conn)
}

pub struct BloomBuild;

impl VScalar for BloomBuild {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let vec0 = input.flat_vector(0);
        let vec1 = input.flat_vector(1);
        let vec2 = input.flat_vector(2);
        let vec3 = input.flat_vector(3);
        let vec4 = input.flat_vector(4);

        let parquet_path = unsafe { read_duck_str(&vec0.as_slice_with_len::<duckdb_string_t>(1)[0]) }.to_owned();
        let column       = unsafe { read_duck_str(&vec1.as_slice_with_len::<duckdb_string_t>(1)[0]) }.to_owned();
        let output_path  = unsafe { read_duck_str(&vec2.as_slice_with_len::<duckdb_string_t>(1)[0]) }.to_owned();
        let est_elements = vec3.as_slice_with_len::<i64>(1)[0] as u64;
        let error_rate   = vec4.as_slice_with_len::<f64>(1)[0];

        let conn = make_conn()?;
        let sql = format!(
            "SELECT DISTINCT {column} FROM read_parquet('{parquet_path}') WHERE {column} IS NOT NULL"
        );

        let mut bf = BloomFilter::new(FilterBuilder::new(est_elements, error_rate));
        let mut stmt = conn.prepare(&sql)?;
        let mut rows = stmt.query([])?;

        while let Some(row) = rows.next()? {
            let val: i64 = row.get(0)?;
            bf.add(&val.to_le_bytes());
        }

        if let Some(parent) = std::path::Path::new(&output_path).parent() {
            std::fs::create_dir_all(parent)?;
        }
        bf.save_to_file_with_hashes(&output_path);

        let mut out_vec = output.flat_vector();
        out_vec.as_mut_slice::<bool>()[0] = true;
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
                LogicalTypeHandle::from(LogicalTypeId::Varchar),
                LogicalTypeHandle::from(LogicalTypeId::Bigint),
                LogicalTypeHandle::from(LogicalTypeId::Double),
            ],
            LogicalTypeHandle::from(LogicalTypeId::Boolean),
        )]
    }
}

pub struct BloomLoad;

impl VScalar for BloomLoad {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let binding = input.flat_vector(0);
        let path = unsafe { read_duck_str(&binding.as_slice_with_len::<duckdb_string_t>(1)[0]) }.to_owned();

        let bf = BloomFilter::from_file_with_hashes(&path);
        *GLOBAL_BF.write().unwrap() = Some(Arc::new(bf));

        let mut out_vec = output.flat_vector();
        out_vec.as_mut_slice::<bool>()[0] = true;
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)],
            LogicalTypeHandle::from(LogicalTypeId::Boolean),
        )]
    }
}

pub struct BloomContains;

impl VScalar for BloomContains {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let len = input.len();

        let in_binding = input.flat_vector(0);
        let values = in_binding.as_slice_with_len::<i64>(len);

        let guard = GLOBAL_BF.read().unwrap();
        let bf = guard.as_ref().ok_or("bloom_load() chưa được gọi")?;

        let mut out_binding = output.flat_vector();
        let results = out_binding.as_mut_slice::<bool>();

        for i in 0..len {
            results[i] = bf.contains(&values[i].to_le_bytes());
        }
        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        vec![ScalarFunctionSignature::exact(
            vec![LogicalTypeHandle::from(LogicalTypeId::Bigint)],
            LogicalTypeHandle::from(LogicalTypeId::Boolean),
        )]
    }
}

#[duckdb_ext_macros::duckdb_extension(name = "bfextension")]
fn extension_init(conn: Connection) -> Result<(), Box<dyn std::error::Error>> {
    conn.register_scalar_function_with_state::<BloomBuild>("bloom_build", &())?;
    conn.register_scalar_function_with_state::<BloomLoad>("bloom_load", &())?;
    conn.register_scalar_function_with_state::<BloomContains>("bloom_contains", &())?;
    Ok(())
}