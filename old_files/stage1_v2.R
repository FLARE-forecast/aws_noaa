## setup
library(gdalcubes)
library(gefs4cast)

readRenviron("/home/rstudio/.Renviron")

#gdalcubes::gdalcubes_options(parallel=2*parallel::detectCores())
gdalcubes::gdalcubes_options(parallel=34)

sites <- readr::read_csv("site_list_v2.csv")

Sys.setenv("GEFS_VERSION"="v12")
dates <- seq(as.Date("2020-09-24"), Sys.Date()-1, by=1)

dates <- seq(as.Date("2024-01-01"), Sys.Date()-1, by=1)


print(Sys.getenv())

message("GEFS v12 stage1")
s3 <- gefs_s3_dir("stage1", path = "flare/drivers/met", endpoint = "https://renc.osn.xsede.org", bucket = "bio230121-bucket01")

have_dates <- gsub("reference_datetime=", "", s3$ls())
missing_dates <- dates[!(as.character(dates) %in% have_dates)]
gefs_to_parquet(missing_dates, path = s3, sites = sites, cycle = "00")
## setup
library(gdalcubes)
library(gefs4cast)

readRenviron("/home/rstudio/.Renviron")

#gdalcubes::gdalcubes_options(parallel=2*parallel::detectCores())
gdalcubes::gdalcubes_options(parallel=34)

sites <- readr::read_csv("site_list_v2.csv")

Sys.setenv("GEFS_VERSION"="v12")
dates <- seq(as.Date("2020-09-24"), Sys.Date()-1, by=1)

dates <- seq(as.Date("2024-01-01"), Sys.Date()-1, by=1)


print(Sys.getenv())

message("GEFS v12 stage1")
s3 <- gefs_s3_dir("stage1", path = "flare/drivers/met", endpoint = "https://renc.osn.xsede.org", bucket = "bio230121-bucket01")

have_dates <- gsub("reference_datetime=", "", s3$ls())
missing_dates <- dates[!(as.character(dates) %in% have_dates)]
gefs_to_parquet(missing_dates, path = s3, sites = sites, cycle = "00")
