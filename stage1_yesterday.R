#renv::restore()

## CRON-job to update the recent GEFS parquet files
## Will pick up yesterday and any missing days in the past

# WARNING: needs >= GDAL 3.4.x
#remotes::install_github("eco4cast/gefs4cast")
library(gefs4cast)
library(purrr)
library(dplyr)
library(tidyverse)

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
threads <- 4


cycles <- c("06", "12", "18")
full_dates <- list()
cycle_dates <- list()

locations <- "site_list.csv"

s3_2 <- arrow::s3_bucket("drivers/noaa/gefs-v12/stage1/0", endpoint_override = "s3.flare-forecast.org")
d <- arrow::open_dataset(s3_2, partitioning = "reference_date") %>% filter(variable == "TMP") %>% 
  group_by(reference_date, parameter) %>% 
  summarise(max = max(horizon)) %>% 
  collect()

missing_dates <- d %>% 
  filter(parameter < 31 & max < 840 & reference_date < yesterday) %>% 
  distinct(reference_date) %>% 
  pull(reference_date)

yesterday <- Sys.Date() - lubridate::days(1)

full_dates <- c(yesterday, missing_dates)

message(paste0("Start: ",Sys.time()))
for(i in 1:length(full_dates)){
  
  message(paste0("Downloading: ", full_dates[i]))
  
  map(full_dates[i], noaa_gefs, cycle="00", threads=threads, s3=s3, locations = locations)
  map(cycles, 
      function(cy) {
        map(full_dates[i], noaa_gefs, cycle=cy, max_horizon = 6,
            threads=threads, s3=s3, gdal_ops="", locations = locations)
      })
}
print(paste0("End: ",Sys.time()))


