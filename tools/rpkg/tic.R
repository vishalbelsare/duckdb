# installs dependencies, runs R CMD check, runs covr::codecov()
do_package_checks(error_on = "warning")

if (Sys.info()[["sysname"]] == "Windows") {
  get_stage("after_success") %>%
    add_code_step(tools::write_PACKAGES(type = "win.binary")) %>%
    add_code_step(system2("C:/Python37-x64/python.exe", c("../upload-s3.py rstats duckdb_*.zip", "rstats duckdb_*.zip"))) %>%
    add_code_step(system2("C:/Python37-x64/python.exe", c("../upload-s3.py rstats duckdb_*.zip", "rstats/bin/windows/contrib/4.0 duckdb_*.zip PACKAGES*"))) %>%
    add_code_step(system2("C:/Python37-x64/python.exe", c("../../scripts/asset-upload.py", "duckdb_r_windows.zip=duckdb_*.zip")))
} else if (Sys.info()[["sysname"]] == "Darwin") {
  get_stage("after_success") %>%
    add_code_step(tools::write_PACKAGES(type = "mac.binary")) %>%
    add_code_step(system2("python3", c("../upload-s3.py", "rstats", "duckdb_*.tgz"))) %>%
    add_code_step(system2("python3", c("../upload-s3.py", "rstats/bin/macosx/contrib/4.0", "duckdb_*.tgz","PACKAGES*"))) %>%
    add_code_step(system2("python3", c("../../scripts/asset-upload.py", "duckdb_r_osx.tgz=duckdb_*.tgzz")))

} else if (Sys.info()[["sysname"]] == "Linux") {
  get_stage("after_success") %>%
    add_code_step(tools::write_PACKAGES(type = "source")) %>%
    add_code_step(system2("python3", c("../upload-s3.py", "rstats", "duckdb_*.tar.gz"))) %>%
    add_code_step(system2("python3", c("../upload-s3.py", "rstats/src/contrib", "duckdb_*.tar.gz","PACKAGES*"))) %>%
    add_code_step(system2("python3", c("../../scripts/asset-upload.py", "duckdb_r_src.tar.gz=duckdb_*.tar.gz")))
}
