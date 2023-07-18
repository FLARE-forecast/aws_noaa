#renv::restore()

library(neonstore)
library(score4cast)
library(arrow)
library(dplyr)
library(ggplot2)
library(gefs4cast)
readRenviron("~/.Renviron")

message(paste0("Start: ",Sys.time()))

source(system.file("examples", "temporal_disaggregation.R", package = "gefs4cast"))

Sys.unsetenv("AWS_DEFAULT_REGION")
Sys.unsetenv("AWS_S3_ENDPOINT")
Sys.setenv(AWS_EC2_METADATA_DISABLED="TRUE")
write_s3 <- TRUE
reprocess_all <- FALSE
real_time_processing <- TRUE

s3_stage1 <- arrow::s3_bucket("drivers/noaa/gefs-v12-reprocess/stage1", 
                              endpoint_override =  "s3.flare-forecast.org",
                              anonymous=TRUE)
s3_stage2 <- arrow::s3_bucket("drivers/noaa/gefs-v12-reprocess", 
                              endpoint_override =  "s3.flare-forecast.org")
s3_stage2$CreateDir("stage2/parquet")
s3_stage2_parquet <- arrow::s3_bucket("drivers/noaa/gefs-v12-reprocess/stage2/parquet", 
                                      endpoint_override =  "s3.flare-forecast.org")

df <- arrow::open_dataset(s3_stage1, partitioning = c("cycle", "start_date", "site_id"))
#df <- arrow::open_dataset(s3_stage1, partitioning = c("cycle", "start_date"))

if(real_time_processing){
  dates <- as.character(seq(Sys.Date() - lubridate::days(6), Sys.Date(), by = "1 day"))
}else{
  dates <- as.character(seq(lubridate::as_date("2020-09-25"), lubridate::as_date("2023-03-06"), by = "1 day"))
}


cycles <- 0

available_dates <- df |> 
  dplyr::filter(start_date %in% dates,
                cycle == 0,
                parameter < 31,
                variable == "PRES") |> 
  dplyr::group_by(start_date, parameter) |> 
  dplyr::summarise(max_horizon = max(horizon)) |> 
  dplyr::summarise(max_horizon = min(max_horizon)) |> 
  #dplyr::filter(max_horizon >= 384) |> 
  dplyr::collect() |> 
  dplyr::pull(start_date)


if(length(s3_stage2_parquet$ls()) > 0){
  df2 <- arrow::open_dataset(s3_stage2_parquet, partitioning = c("cycle","start_date", "site_id"))
  #df2 <- arrow::open_dataset(s3_stage2_parquet, partitioning = c("cycle","start_date"))  
  max_horizon_date <- df2 |> 
    dplyr::filter(start_date %in% dates,
                  cycle == 0,
                  parameter < 31,
                  variable == "air_temperature") |>
    dplyr::rename(date = start_date) |> 
    group_by(date, cycle, parameter) |>
    summarize(max = max(horizon)) |> 
    group_by(date, cycle) |>
    summarize(horizon = min(max)) |> 
    dplyr::collect()
  
  forecast_start_times <- expand.grid(available_dates, cycles) |> 
    stats::setNames(c("date", "cycle")) |> 
    mutate(start_times = paste0(date, " ", stringr::str_pad(cycle, width = 2, side = "left", pad = 0), ":00:00"),
           dir_parquet = file.path(cycle, date),
           dir_netcdf = file.path(date, cycle),
           cycle = as.integer(as.character(cycle)),
           date = date) |> 
    select(date, cycle, dir_parquet, dir_netcdf) |> 
    left_join(max_horizon_date, by = c("date","cycle"))
}else{
  df2 <- arrow::open_dataset(s3_stage2_parquet) 
  forecast_start_times <- expand.grid(available_dates, cycles) |> 
    stats::setNames(c("date", "cycle")) |> 
    mutate(start_times = paste0(date, " ", stringr::str_pad(cycle, width = 2, side = "left", pad = 0), ":00:00"),
           dir_parquet = file.path(cycle, date),
           dir_netcdf = file.path(date, cycle),
           cycle = as.integer(as.character(cycle)),
           date = date) |> 
    select(date, cycle, dir_parquet, dir_netcdf) %>% 
    mutate(max_horizon_date = NA)
}


files_present <- purrr::map_int(1:nrow(forecast_start_times), function(i, forecast_start_times){
  if(forecast_start_times$cycle[i] %in% s3_stage2_parquet$ls()){
    if(forecast_start_times$dir_parquet[i] %in% s3_stage2_parquet$ls(forecast_start_times$cycle[i])){
      exiting_files <- length(s3_stage2_parquet$ls(forecast_start_times$dir_parquet[i]))
      if((forecast_start_times$horizon[i] < 840 | is.na(forecast_start_times$horizon[i])) & forecast_start_times$cycle[i] == 0){
        exiting_files <- NA
      }
    }else{
      exiting_files <- NA
    }
  }else{
    exiting_files <- NA
  }
  return(exiting_files)
}, forecast_start_times = forecast_start_times)

files_present <- tibble::tibble(files_present = files_present)
forecast_start_times <- bind_cols(forecast_start_times, files_present) 

forecast_start_times <- forecast_start_times |> 
  dplyr::filter(is.na(files_present) | files_present == 0 | reprocess_all)

#future::plan("future::multisession", workers = 4)
if(nrow(forecast_start_times) > 0){
  purrr::walk(1:nrow(forecast_start_times),
              function(i, forecast_start_times, df){
                
                s3_stage2_parquet$CreateDir(forecast_start_times$dir_parquet[i])
                message(paste0("Processing ", forecast_start_times$date[i]," ", forecast_start_times$cycle[i]))
                d1 <- df |> 
                  dplyr::filter(start_date == as.character(forecast_start_times$date[i]),
                                variable %in% c("PRES","TMP","RH","UGRD","VGRD","APCP","DSWRF","DLWRF"),
                                cycle == as.integer(forecast_start_times$cycle[i])) |> 
                  select(-c("start_date", "cycle")) |>  
                  dplyr::collect() |> 
                  disaggregate_fluxes() |> 
                  add_horizon0_time() |> 
                  convert_precip2rate() |> 
                  convert_temp2kelvin() |> 
                  convert_rh2proportion() |> 
                  disaggregate2hourly() |>
                  standardize_names_cf() |> 
                  correct_solar_geom()
                
                sites <- unique(d1$site_id)
                #path <- glue::glue(name_pattern)
                #arrow::write_parquet(d1, sink = s3_stage2_parquet$path(file.path(forecast_start_times$dir_parquet[i],"part-0.parquet")))
                
                purrr::walk(sites, function(site,d1){
                  site_id <- site
                  d1 |> filter(site_id == site) |>
                    arrow::write_parquet(sink = s3_stage2_parquet$path(file.path(forecast_start_times$dir_parquet[i],site_id,"part-0.parquet")))
                },
                d1)
                
                #RCurl::url.exists("https://hc-ping.com/66e99d0a-8dbb-43df-b066-39d7f7b01af3", timeout = 5)
                
              },
              df = df,
              forecast_start_times= forecast_start_times)
  
}else{
  message("no updates to process")
}

message(paste0("End: ",Sys.time()))
