#renv::restore()

## CRON-job to update the recent GEFS parquet files
## Will pick up from the day after the last date on record

# WARNING: needs >= GDAL 3.4.x
#remotes::install_github("eco4cast/gefs4cast")
library(gefs4cast)
library(purrr)
library(dplyr)
library(tidyverse)

# be littler-compatible
readRenviron("~/.Renviron")
print(paste0("Start: ",Sys.time()))

locations <- "site_list_v2.csv"

# Set destination bucket
Sys.unsetenv("AWS_DEFAULT_REGION")
Sys.unsetenv("AWS_S3_ENDPOINT")
Sys.setenv(AWS_EC2_METADATA_DISABLED="TRUE")
s3 <- arrow::s3_bucket("drivers", endpoint_override = "s3.flare-forecast.org")

# Set desired dates and threads
# Adjust threads between 70 - 1120 depending on available RAM, CPU, + bandwidth
threads <- 4

gefs <- s3$path("noaa/gefs-v12-reprocess/stage1/0")
have <- gefs$ls()
have_days <- as.Date(basename(have))
start <- max(have_days, na.rm=TRUE)
#start <- as.Date("2022-10-08")
#have_cycles <- basename(gefs$ls(start))

aws <- arrow::s3_bucket("noaa-gefs-pds", anonymous = TRUE)
avail <- aws$ls()
days <- as.Date(gsub("^gefs\\.(\\d{8})", "\\1", avail), "%Y%m%d")
avail_day <- max(days,na.rm=TRUE)

# ick can detect folder before it has data!
# hackish sanity check
A <- aws$ls( paste(avail[which.max(days)], "00", "atmos", "pgrb2ap5", sep="/" ))
A <- A[stringr::str_detect(A, "f384")]
if(length(A[stringr::str_detect(A, "gep")] == 60) & avail_day == Sys.Date()){
  
  full_dates <-  Sys.Date()
  
  message(paste0("Start: ",Sys.time()))
  message(paste0("Downloading: ", full_dates))
  
  map(full_dates, noaa_gefs, cycle="00", max_horizon = 384, threads=threads, s3=s3, locations = locations,
      name_pattern = "noaa/gefs-v12-reprocess/stage1/{cycle_int}/{nice_date}/{site_id}/part-0.parquet")
  
  print(paste0("End: ",Sys.time()))
}





