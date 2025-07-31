source("to_hourly.R")

duckdbfs::duckdb_secrets(
    endpoint = 'amnh1.osn.mghpcc.org',
    key = Sys.getenv("OSN_KEY"),
    secret = Sys.getenv("OSN_SECRET"))

locations <- readr::read_csv("site_list_v2.csv")
site_list <- locations |> dplyr::pull(site_id)

message('starting download loop...')
#future::plan("future::multisession", workers = parallel::detectCores())

#future::plan("future::sequential")

#furrr::future_walk(site_list, function(curr_site_id){

  
  curr_site_id = 'BARC'
  print(curr_site_id)
  
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
  
  message('check pseudo df...')
  
  if(nrow(df) > 0){
    
    df2 <- df |>
      to_hourly(use_solar_geom = TRUE, psuedo = TRUE, locations = locations) |>
      dplyr::mutate(ensemble = as.numeric(stringr::str_sub(ensemble, start = 4, end = 5))) |>
      dplyr::rename(parameter = ensemble)
    
    message('df2 converted to hourly...')
    stage3_df_update <- stage3_df |>
      dplyr::filter(datetime < min(df2$datetime))
    
    message('create final df...')
    df_final <- df2 |>
      dplyr::bind_rows(stage3_df_update) |>
      dplyr::arrange(variable, datetime, parameter) #|>
      #arrow::write_dataset(path = s3, partitioning = "site_id")

    print(names(df_final)
    print(nrow(df_final))
          
    message('save stage3...')
    duckdbfs::write_dataset(df_final, path = "s3://bio230121-bucket01/flare/drivers/met/gefs-v12/stage3", format = 'parquet',
                              partitioning = "site_id")
  }
#})
