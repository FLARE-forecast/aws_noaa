library(gdalcubes)
library(gefs4cast)

#readRenviron("/home/rstudio/.Renviron")

#gdalcubes::gdalcubes_options(parallel=2*parallel::detectCores())
gdalcubes::gdalcubes_options(parallel=64)

sites <- readr::read_csv("site_list_v2.csv")

Sys.setenv("GEFS_VERSION"="v12")

dates <- seq(as.Date("2024-02-01"), Sys.Date()-1, by=1)

message("GEFS v12 stage1")
s3 <- gefs_s3_dir("stage1", path = "flare/drivers/met", endpoint = "https://amnh1.osn.mghpcc.org", bucket = "bio230121-bucket01")

have_dates <- gsub("reference_datetime=", "", s3$ls())
missing_dates <- dates[!(as.character(dates) %in% have_dates)]

#gefs_to_parquet(missing_dates, path = s3, sites = sites, cycle = "00")
## function that uses duckdbfs write method
parquet_path <- 'bio230121-bucket01/flare/drivers/met/gefs-v12/stage1'
gefs_to_parquet(missing_dates, path = parquet_path, sites = sites, cycle = "00", s3_endpoint = 'https://amnh1.osn.mghpcc.org')
