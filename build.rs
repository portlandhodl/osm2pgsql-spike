//! Build script: compile the OSM PBF .proto files (proto/fileformat.proto,
//! proto/osmformat.proto, fetched from openstreetmap/OSM-binary) into Rust
//! code with prost-build.
//!
//! protoc is not installed on the build host, so we point prost-build at the
//! vendored protoc binary shipped by protoc-bin-vendored.

fn main() {
    let protoc = protoc_bin_vendored::protoc_bin_path()
        .expect("protoc-bin-vendored must provide a protoc binary");
    // prost-build locates protoc through the PROTOC environment variable.
    std::env::set_var("PROTOC", protoc);

    let protos = ["proto/fileformat.proto", "proto/osmformat.proto"];
    for p in &protos {
        println!("cargo:rerun-if-changed={p}");
    }

    prost_build::compile_protos(&protos, &["proto"])
        .expect("failed to compile OSM PBF proto files with prost-build");
}
