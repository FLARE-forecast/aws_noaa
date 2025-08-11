## setup
library(gdalcubes)
library(gefs4cast)

#install.packages("arrow", version='20.0.0')
#remotes::install_version("arrow", version = "20.0.0")
library(arrow)

print(sessioninfo::package_info())

gdalcubes::gdalcubes_options(parallel=34)
#gdalcubes::gdalcubes_options(parallel=TRUE)
#readRenviron("/home/rstudio/.Renviron")

sites <- readr::read_csv("site_list_v2.csv")

Sys.setenv("GEFS_VERSION"="v12")
dates_pseudo <- seq(as.Date("2020-10-01"), Sys.Date(), by=1)
#dates_pseudo <- seq(as.Date("2020-10-01"), as.Date("2020-10-23"), by=1)
message("GEFS v12 pseudo")

# duckdbfs::duckdb_secrets(
#   endpoint = 'amnh1.osn.mghpcc.org',
#   key = Sys.getenv("OSN_KEY"),
#   secret = Sys.getenv("OSN_SECRET"))

s3 <- gefs_s3_dir("pseudo", path = "flare/drivers/met", endpoint = "https://amnh1.osn.mghpcc.org", bucket = "bio230121-bucket01")
have_dates <- gsub("reference_datetime=", "", s3$ls())
missing_dates <- dates_pseudo[!(as.character(dates_pseudo) %in% have_dates)]

missing_dates <- "2025-07-26"

#parquet_path <- 'bio230121-bucket01/flare/drivers/met/gefs-v12/pseudo'
gefs4cast:::gefs_pseudo_measures(missing_dates,  path = s3, sites = sites)

rm()
gc()
