fn main() {
    println!("cargo:rerun-if-changed=include/talon.h");
    println!("cargo:rerun-if-changed=tests/c_api_smoke.c");

    cc::Build::new()
        .file("tests/c_api_smoke.c")
        .include("include")
        .flag_if_supported("-std=c11")
        .compile("talon_c_api_smoke");
}
