source("to_hourly.R")

locations <- readr::read_csv("site_list_v2.csv")
site_list <- locations |> dplyr::pull(site_id)

#future::plan("future::multisession", workers = parallel::detectCores())

future::plan("future::sequential")

furrr::future_walk(site_list, function(curr_site_id){
  
  print(curr_site_id)
  
  duckdbfs::duckdb_secrets(
    endpoint = 'amnh1.osn.mghpcc.org',
    key = Sys.getenv("OSN_KEY"),
    secret = Sys.getenv("OSN_SECRET"))
  
  s3 <- arrow::s3_bucket("bio230121-bucket01/flare/drivers/met/gefs-v12/stage3",
                         endpoint_override = "amnh1.osn.mghpcc.org",
                         access_key= Sys.getenv("OSN_KEY"),
                         secret_key= Sys.getenv("OSN_SECRET"))
  
  message('stage3 site download')
  stage3_df <- arrow::open_dataset(s3) |>
    dplyr::filter(site_id == curr_site_id) |>
    dplyr::collect()
  
  message('pull max date')
  max_date <- stage3_df |>
    dplyr::summarise(max = as.character(lubridate::as_date(max(datetime)))) |>
    dplyr::pull(max)
  
  s3_pseudo <- arrow::s3_bucket("bio230121-bucket01/flare/drivers/met/gefs-v12/pseudo",
                                endpoint_override = "amnh1.osn.mghpcc.org",
                                access_key= Sys.getenv("OSN_KEY"),
                                secret_key= Sys.getenv("OSN_SECRET"))
  
  vars <- names(stage3_df)
  
  cut_off <- as.character(lubridate::as_date(max_date) - lubridate::days(3))
  
  message('pseudo collect...')
  
  df <- arrow::open_dataset(s3_pseudo) |>
    dplyr::filter(variable %in% c("PRES","TMP","RH","UGRD","VGRD","APCP","DSWRF","DLWRF")) |>
    dplyr::filter(site_id == curr_site_id,
                  reference_datetime >= cut_off) |>
    dplyr::collect() |>
    dplyr::mutate(date = lubridate::as_date(reference_datetime),
                  new_datetime = date + lubridate::hours(as.numeric(horizon)) + lubridate::hours(as.numeric(cycle)),
                  datetime = ifelse(datetime != new_datetime, new_datetime, datetime),
                  datetime = lubridate::as_datetime(datetime)) |>
    dplyr::select(-date, -new_datetime)
  
  if(nrow(df) > 0){
    
    df2 <- df |>
      to_hourly(use_solar_geom = TRUE, psuedo = TRUE, locations = locations) |>
      dplyr::mutate(ensemble = as.numeric(stringr::str_sub(ensemble, start = 4, end = 5))) |>
      dplyr::rename(parameter = ensemble)
    
    stage3_df_update <- stage3_df |>
      dplyr::filter(datetime < min(df2$datetime))
    
    df2 |>
      dplyr::bind_rows(stage3_df_update) |>
      dplyr::arrange(variable, datetime, parameter) |>
      #arrow::write_dataset(path = s3, partitioning = "site_id")
      duckdbfs::write_dataset(path = "s3://bio230121-bucket01/flare/drivers/met/gefs-v12/stage3", format = 'parquet',
                              partitioning = "site_id")
  }
})
