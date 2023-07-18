#renv::restore()
source("ignore_sigpipes.R")
ignore_sigpipe()

## CRON-job to update the recent GEFS parquet files
## Will pick up from the day after the last date on record

# WARNING: needs >= GDAL 3.4.x
#remotes::install_github("eco4cast/gefs4cast")
library(gefs4cast)
library(purrr)
library(dplyr)

# be littler-compatible
readRenviron("~/.Renviron")
print(paste0("Start: ",Sys.time()))

# Set destination bucket
Sys.unsetenv("AWS_DEFAULT_REGION")
Sys.unsetenv("AWS_S3_ENDPOINT")
Sys.setenv(AWS_EC2_METADATA_DISABLED="TRUE")
s3 <- arrow::s3_bucket("drivers", endpoint_override = "s3.flare-forecast.org")

# Set desired dates and threads
# Adjust threads between 70 - 1120 depending on available RAM, CPU, + bandwidth
threads <- 16
download_all <- FALSE

#start <- as.Date("2020-09-25")

start <- as.Date("2023-03-06")

#NOTE: NEED TO REDO 2021-08-14, 2021-11-16

s3$CreateDir("noaa/gefs-v12-reprocess/stage1/0")
s3$CreateDir("noaa/gefs-v12-reprocess/stage1/18")
gefs <- s3$path("noaa/gefs-v12-reprocess/stage1/0")
gefs18 <- s3$path("noaa/gefs-v12-reprocess/stage1/18")

cycles <- c("06", "12", "18")
locations <- "site_list_v2.csv"
full_dates <-   as.Date(seq(as.Date("2023-06-28"), as.Date("2023-07-08"), by = "1 day"))

full_dates <- as.Date(c("2023-06-29"))

for(i in 1:length(full_dates)){

map(full_dates[i], noaa_gefs, cycle="00", threads=threads, s3=s3, locations = locations,
    name_pattern = "noaa/gefs-v12-reprocess/stage1/{cycle_int}/{nice_date}/{site_id}/part-0.parquet")
map(cycles, 
    function(cy) {
      map(full_dates[i], noaa_gefs, cycle=cy, max_horizon = 6,
          threads=threads, s3=s3, gdal_ops="", locations = locations,
          name_pattern = "noaa/gefs-v12-reprocess/stage1/{cycle_int}/{nice_date}/{site_id}/part-0.parquet")
    })

RCurl::url.exists("https://hc-ping.com/66e99d0a-8dbb-43df-b066-39d7f7b01af3", timeout = 5)
}

