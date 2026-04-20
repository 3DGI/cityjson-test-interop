use cityjson::CityModelType;
use cityjson::v2_0::{
    AttributeValue, CityModelIdentifier, CityObject, CityObjectIdentifier, CityObjectType,
    GeometryDraft, OwnedCityModel, RingDraft, SurfaceDraft,
};
use cityjson_arrow::{ExportOptions, ImportOptions, read_stream, write_stream};
use cityjson_json::{WriteOptions, to_vec};
use cityjson_parquet::{ParquetDatasetReader, ParquetDatasetWriter};
use std::env;
use std::fs::File;
use std::io::{self, Write};
use std::path::Path;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn main() -> Result<()> {
    let args = env::args().collect::<Vec<_>>();
    if args.len() != 3 {
        return Err("usage: cityjson-test-interop-rust-tools <command> <path>".into());
    }

    match args[1].as_str() {
        "read-arrow-json" => read_arrow_json(&args[2]),
        "read-dataset-json" => read_dataset_json(&args[2]),
        "write-arrow-fixture" => write_arrow_fixture(&args[2]),
        "write-dataset-fixture" => write_dataset_fixture(&args[2]),
        other => Err(format!("unknown command {other}").into()),
    }
}

fn read_arrow_json(path: impl AsRef<Path>) -> Result<()> {
    let file = File::open(path)?;
    let model = read_stream(file, &ImportOptions::default())?;
    write_model_json(&model)
}

fn read_dataset_json(path: impl AsRef<Path>) -> Result<()> {
    let model = ParquetDatasetReader.read_dir(path)?;
    write_model_json(&model)
}

fn write_arrow_fixture(path: impl AsRef<Path>) -> Result<()> {
    let model = fixture_model()?;
    let mut file = File::create(path)?;
    write_stream(&mut file, &model, &ExportOptions::default())?;
    Ok(())
}

fn write_dataset_fixture(path: impl AsRef<Path>) -> Result<()> {
    let model = fixture_model()?;
    ParquetDatasetWriter.write_dir(path, &model)?;
    Ok(())
}

fn write_model_json(model: &OwnedCityModel) -> Result<()> {
    let bytes = to_vec(model, &WriteOptions::default())?;
    io::stdout().write_all(&bytes)?;
    Ok(())
}

fn fixture_model() -> Result<OwnedCityModel> {
    let mut model = OwnedCityModel::new(CityModelType::CityJSON);
    model
        .metadata_mut()
        .set_identifier(CityModelIdentifier::new("rust-interop-fixture".to_string()));
    model
        .metadata_mut()
        .set_title("Rust interop fixture".to_string());

    let geometry = GeometryDraft::multi_surface(
        None,
        [SurfaceDraft::new(
            RingDraft::new([
                [0.0, 0.0, 0.0],
                [1.0, 0.0, 0.0],
                [1.0, 1.0, 0.0],
                [0.0, 1.0, 0.0],
                [0.0, 0.0, 0.0],
            ]),
            [],
        )],
    )
    .insert_into(&mut model)?;

    let mut building = CityObject::new(
        CityObjectIdentifier::new("rust-building-1".to_string()),
        CityObjectType::Building,
    );
    building.add_geometry(geometry);
    building.attributes_mut().insert(
        "name".to_string(),
        AttributeValue::String("Rust Building".to_string()),
    );
    building
        .attributes_mut()
        .insert("height".to_string(), AttributeValue::Float(12.5));

    let mut part = CityObject::new(
        CityObjectIdentifier::new("rust-part-1".to_string()),
        CityObjectType::BuildingPart,
    );
    part.attributes_mut().insert(
        "name".to_string(),
        AttributeValue::String("Rust Annex".to_string()),
    );

    let building_handle = model.cityobjects_mut().add(building)?;
    let part_handle = model.cityobjects_mut().add(part)?;
    model
        .cityobjects_mut()
        .get_mut(building_handle)
        .expect("building was inserted")
        .add_child(part_handle);
    model
        .cityobjects_mut()
        .get_mut(part_handle)
        .expect("part was inserted")
        .add_parent(building_handle);

    Ok(model)
}
