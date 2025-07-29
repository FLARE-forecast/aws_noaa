library(minioclient)
source("to_hourly.R")

duckdbfs::duckdb_secrets(
    endpoint = 'https://amnh1.osn.mghpcc.org',
    key = Sys.getenv("OSN_KEY"),
    secret = Sys.getenv("OSN_SECRET"))

#install_mc()
mc_alias_set("osn", "amnh1.osn.mghpcc.org", "", "")
mc_mirror("osn/bio230121-bucket01/flare/drivers/met/gefs-v12/pseudo", "pseudo")

df <- arrow::open_dataset("pseudo") |>
  dplyr::filter(variable %in% c("PRES","TMP","RH","UGRD","VGRD","APCP","DSWRF","DLWRF"))


locations <- readr::read_csv("site_list_v2.csv")


site_list <- locations |> dplyr::pull(site_id)

s3 <- arrow::s3_bucket("bio230121-bucket01/flare/drivers/met/gefs-v12",
                       endpoint_override = "amnh1.osn.mghpcc.org",
                       access_key= Sys.getenv("OSN_KEY"),
                       secret_key= Sys.getenv("OSN_SECRET"))

s3$CreateDir("stage3")

s3 <- arrow::s3_bucket("bio230121-bucket01/flare/drivers/met/gefs-v12/stage3",
                       endpoint_override = "amnh1.osn.mghpcc.org",
                       access_key= Sys.getenv("OSN_KEY"),
                       secret_key= Sys.getenv("OSN_SECRET"))

#site_list <- site_list[1:3]

future::plan("future::multisession", workers = 8)

furrr::future_walk(site_list, function(curr_site_id, locations){
  
  df <- arrow::open_dataset("pseudo") |>
    dplyr::filter(variable %in% c("PRES","TMP","RH","UGRD","VGRD","APCP","DSWRF","DLWRF")) |>
    dplyr::filter(site_id == curr_site_id) |>
    dplyr::collect() |>
    dplyr::mutate(date = lubridate::as_date(reference_datetime),
                  new_datetime = date + lubridate::hours(as.numeric(horizon)) + lubridate::hours(as.numeric(cycle)),
                  datetime = ifelse(datetime != new_datetime, new_datetime, datetime),
                  datetime = lubridate::as_datetime(datetime)) |>
    dplyr::select(-date, -new_datetime)
  
  #s3 <- arrow::s3_bucket("bio230121-bucket01/flare/drivers/met/gefs-v12/stage3",
  #                       endpoint_override = "amnh1.osn.mghpcc.org",
  #                       access_key= Sys.getenv("OSN_KEY"),
  #                       secret_key= Sys.getenv("OSN_SECRET"))
  
  print(curr_site_id)
  df |>
    to_hourly(use_solar_geom = TRUE, psuedo = TRUE, locations = locations) |>
    dplyr::mutate(ensemble = as.numeric(stringr::str_sub(ensemble, start = 4, end = 5))) |>
    dplyr::rename(parameter = ensemble) |>
    #arrow::write_dataset(path = s3, partitioning = "site_id")
    duckdbfs::write_dataset(path = "s3://bio230121-bucket01/flare/drivers/met/gefs-v12/stage3", format = 'parquet',
                              partitioning = "site_id")
},
locations)
