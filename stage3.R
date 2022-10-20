readRenviron("~/.Renviron") # MUST come first
#source(".Rprofile") # littler won't read this automatically, so renv won't work
#renv::restore()


library(neonstore)
library(score4cast)
library(arrow)
library(dplyr)
library(ggplot2)
library(gefs4cast)
print(paste0("Start: ",Sys.time()))

source(system.file("examples", "temporal_disaggregation.R", package = "gefs4cast"))

base_dir <- path.expand("~/test_processing/noaa/gefs-v12/")
generate_netcdf <- TRUE

Sys.unsetenv("AWS_DEFAULT_REGION")
Sys.unsetenv("AWS_S3_ENDPOINT")
Sys.setenv(AWS_EC2_METADATA_DISABLED="TRUE")
model_name <- "NOAAGEFS_1hr_stacked"
generate_netcdf <- TRUE
write_s3 <- TRUE

message("reading stage 1")

s3_stage1 <- arrow::s3_bucket("drivers/noaa/gefs-v12/stage1", 
                              endpoint_override =  "s3.flare-forecast.org",
                              anonymous=TRUE)


message("reading stage 3")

s3_stage3 <- arrow::s3_bucket("drivers/noaa/gefs-v12/", 
                              endpoint_override =  "s3.flare-forecast.org")
s3_stage3$CreateDir("stage3/parquet")
if(generate_netcdf) s3_stage3$CreateDir("stage3/ncdf")

s3_stage3_parquet <- arrow::s3_bucket("drivers/noaa/gefs-v12/stage3/parquet", 
                                      endpoint_override =  "s3.flare-forecast.org")

if(generate_netcdf){
  s3_stage3_ncdf_local <- file.path(base_dir,"stage3", "ncdf")
  s3_stage3_ncdf <- arrow::s3_bucket("drivers/noaa/gefs-v12/stage3/ncdf", 
                                     endpoint_override =  "s3.flare-forecast.org")
  
  fs::dir_create(s3_stage3_ncdf_local)
}

message("opening stage 1")

df <- arrow::open_dataset(s3_stage1, partitioning = c("start_date", "cycle"))



sites <- df |> 
  dplyr::filter(start_date == "2020-09-25",
                variable == "PRES") |> 
  distinct(site_id) |> 
  collect() |> 
  pull(site_id)

message("collecting stage 1")

all_stage1 <- df |> 
  filter(variable %in% c("PRES","TMP","RH","UGRD","VGRD","APCP","DSWRF","DLWRF"),
         horizon %in% c(0,3)) |> 
  collect()

message("writing stage 1")
arrow::write_parquet(all_stage1, sink = "stage3tmp.parquet")

rm(all_stage1)

gc()

df <- arrow::open_dataset("stage3tmp.parquet")

purrr::walk(sites, function(site, base_dir, df){
  message(site)
  message(Sys.time())
  fname <- paste0("stage3-",site,".parquet")
  if(site %in% s3_stage3_parquet$ls()){
    d <- arrow::read_parquet(s3_stage3_parquet$path(file.path(site, fname))) %>% 
      mutate(start_date = lubridate::as_date(time))
    max_start_date <- max(d$start_date)
    d2 <- d %>% 
      filter(start_date != max_start_date) |> 
      select(-dplyr::any_of(c("start_date","horizon","forecast_valid")))
    
    date_range <- as.character(seq(max_start_date, Sys.Date(), by = "1 day"))
    do_run <- length(date_range) > 1
  }else{
    date_range <- as.character(seq(lubridate::as_date("2020-09-25"), Sys.Date(), by = "1 day"))
    d2 <- NULL
    do_run <- TRUE
    
  }
  
  if(do_run){
    
    d1 <- df |> 
      filter(start_date %in% date_range,
             site_id == site) |> 
      select(-c("start_date", "cycle")) |>
      distinct() |> 
      collect() |> 
      disaggregate_fluxes() |>  
      add_horizon0_time() |> 
      convert_precip2rate() |>
      convert_temp2kelvin() |> 
      convert_rh2proportion() |> 
      filter(horizon < 6) |> 
      mutate(start_time = min(time)) |> 
      disaggregate2hourly() |>
      standardize_names_cf() |> 
      dplyr::bind_rows(d2) |>
      mutate(start_time = min(time)) |> 
      #dplyr::select(time, start_time, site_id, longitude, latitude, ensemble, variable, height, predicted) |> 
      arrange(site_id, time, variable, ensemble)
    
    #NEED TO UPDATE TO WRITE TO S3
    
    if(generate_netcdf){
      d1 |> 
        write_noaa_gefs_netcdf(dir = file.path(s3_stage3_ncdf_local, site), model_name = model_name)
      files <- fs::dir_ls(file.path(s3_stage3_ncdf_local, site))
      
      if(write_s3){
        readRenviron("~/.Renviron")
        purrr::walk(files, function(file){
          aws.s3::put_object(file = file, 
                             object = file.path("noaa/gefs-v12/stage3/ncdf", site,basename(file)), 
                             bucket = "drivers", region = "s3", base_url = "flare-forecast.org",verbose = FALSE) 
          fs::file_delete(file)
        })
      }
    }
    
    d1 |> 
      dplyr::select(time, site_id, longitude, latitude, ensemble, variable, height, predicted) |> 
      arrow::write_parquet(sink = s3_stage3_parquet$path(file.path(site, fname)))
  }
},
base_dir = base_dir,
df = df
)

#d1 |> 
#  dplyr::filter(ensemble <= 31) %>% 
#  ggplot(aes(x = time, y = predicted, group = ensemble))  +
#  geom_line() +
#  facet_wrap(~variable, scale = "free")

#ggsave(p, filename = paste0("/home/rstudio/", sites[i],".pdf"), device = "pdf", height = 6, width = 12)

unlink("stage3tmp.parquet")
print(paste0("End: ",Sys.time()))





