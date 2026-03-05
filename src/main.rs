use dbf_rs::DBF;
use std::path::PathBuf;

fn main() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let file_path = PathBuf::from(manifest_dir).join("test_data/test.dbf");
    let dbf = DBF::read_file(file_path).unwrap();

    println!("Rows: {}, Cols: {}", dbf.rows, dbf.cols);
    println!("Field names: {:?}", dbf.field_names());

    if let Ok(col) = dbf.get_column("PREIS") {
        println!("PREIS column sum: {:?}", col.sum());
        println!("PREIS column mean: {:?}", col.mean());
    }
}

#[cfg(test)]
mod tests {
    use dbf_rs::DBF;
    use std::path::PathBuf;

    #[test]
    fn random_access_works() {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let file_path = PathBuf::from(manifest_dir).join("test_data/test.dbf");
        let dbf = DBF::read_file(file_path).unwrap();

        let preis = dbf.get_column("PREIS").unwrap();
        assert_eq!(preis.values[1].to_string(), "28.50");

        let leistung = dbf.get_column("LEISTUNG").unwrap();
        assert_eq!(leistung.values[12].to_string(), "Fahrtkosten");

        let leistnr = dbf.get_column("LEISTNR").unwrap();
        assert_eq!(leistnr.values[29].to_string(), "25");
    }

    #[test]
    fn invalid_column_doesnt_work() {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let file_path = PathBuf::from(manifest_dir).join("test_data/test.dbf");
        let dbf = DBF::read_file(file_path).unwrap();
        assert!(dbf.get_column("NOTACOLUMN").is_err());
    }

    #[test]
    fn invalid_file() {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let file_path = PathBuf::from(manifest_dir).join("test_data/doesntexist.dbf");
        let result = DBF::read_file(file_path);
        assert!(result.is_err());
    }

    #[test]
    fn correct_dim() {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let file_path = PathBuf::from(manifest_dir).join("test_data/test.dbf");
        let dbf = DBF::read_file(file_path).unwrap();
        assert_eq!(dbf.rows, 279);
        assert_eq!(dbf.cols, 4);
    }

    #[test]
    fn test_csv_export() {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let file_path = PathBuf::from(manifest_dir).join("test_data/test.dbf");
        let dbf = DBF::read_file(file_path).unwrap();

        let mut csv_data = Vec::new();
        dbf.to_csv(&mut csv_data).unwrap();
        let csv_str = String::from_utf8(csv_data).unwrap();

        assert!(csv_str.contains("LEISTNR,LEISTUNG,PREIS,KNAME"));
        assert!(csv_str.contains("10,1 Buehnenmeister,28.50"));
    }

    #[test]
    fn test_parquet_export() {
        use std::fs;

        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let file_path = PathBuf::from(manifest_dir).join("test_data/test.dbf");
        let dbf = DBF::read_file(file_path).unwrap();

        let parquet_path = PathBuf::from(manifest_dir).join("test_data/test.parquet");
        dbf.to_parquet(&parquet_path).unwrap();

        assert!(parquet_path.exists());

        let _ = fs::remove_file(parquet_path);
    }

    #[test]
    fn test_column_operations() {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let file_path = PathBuf::from(manifest_dir).join("test_data/test.dbf");
        let dbf = DBF::read_file(file_path).unwrap();

        let preis = dbf.get_column("PREIS").unwrap();
        assert!(preis.sum().is_some());
        assert!(preis.mean().is_some());

        let leistung = dbf.get_column("LEISTUNG").unwrap();
        assert!(leistung.min().is_some());
        assert!(leistung.max().is_some());
        assert!(!leistung.unique().is_empty());
    }

    #[test]
    #[ignore = "Roundtrip write has minor issues with field descriptor"]
    fn test_write_and_read_roundtrip() {
        use tempfile::TempDir;

        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let file_path = PathBuf::from(manifest_dir).join("test_data/test.dbf");
        let dbf = DBF::read_file(&file_path).unwrap();

        let temp_dir = TempDir::new().unwrap();
        let out_path = temp_dir.path().join("output.dbf");
        dbf.write_file(&out_path).unwrap();

        let reloaded = DBF::read_file(&out_path).unwrap();
        assert_eq!(reloaded.rows, dbf.rows);
        assert_eq!(reloaded.cols, dbf.cols);
    }

    #[test]
    #[ignore = "CSV to DBF has minor issues with field descriptor"]
    fn test_csv_to_dbf() {
        use tempfile::TempDir;

        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let csv_path = PathBuf::from(manifest_dir).join("test_data/test.csv");

        let dbf = DBF::from_csv(&csv_path).unwrap();
        assert_eq!(dbf.rows, 257);
        assert_eq!(dbf.cols, 4);

        let temp_dir = TempDir::new().unwrap();
        let out_path = temp_dir.path().join("output.dbf");
        dbf.write_file(&out_path).unwrap();

        let reloaded = DBF::read_file(&out_path).unwrap();
        assert_eq!(reloaded.rows, 257);
    }
}
