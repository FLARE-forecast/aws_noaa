#renv::restore()

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
download_all <- TRUE

start <- as.Date("2020-09-25")

s3$CreateDir("noaa/gefs-v12/stage1/0")
s3$CreateDir("noaa/gefs-v12/stage1/18")
gefs <- s3$path("noaa/gefs-v12/stage1/0")
gefs18 <- s3$path("noaa/gefs-v12/stage1/18")
have0 <- gefs$ls()
have18 <- gefs18$ls()
full_dates <- seq(start, Sys.Date(), by= "1 day")
include_dates <- rep(TRUE, length(full_dates))
if((length(have0) > 0 & length(have0) > 18) & !download_all){
  have_days <- as.Date(basename(have0))
  for(j in 1:length(full_dates)){
    if(full_dates[j] %in% have_days){
      include_dates[j] <- FALSE
    }
  }
  full_dates <- full_dates[include_dates]
}


cycles <- c("06", "12", "18")
cycle_dates <- list()

locations <- "site_list.csv"

for(i in 1:length(full_dates)){

map(full_dates[i], noaa_gefs, cycle="00", threads=threads, s3=s3, locations = locations)
map(cycles, 
    function(cy) {
      map(full_dates[i], noaa_gefs, cycle=cy, max_horizon = 6,
          threads=threads, s3=s3, gdal_ops="", locations = locations)
    })
}

