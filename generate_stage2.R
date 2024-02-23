source("https://raw.githubusercontent.com/eco4cast/neon4cast/ci_upgrade/R/to_hourly.R")

site_list <- readr::read_csv("site_list_v2.csv")|> 
  dplyr::pull(site_id)

s3 <- arrow::s3_bucket("bio230121-bucket01/flare/drivers/met/gefs-v12",
                       endpoint_override = "renc.osn.xsede.org",
                       access_key= Sys.getenv("OSN_KEY"),
                       secret_key= Sys.getenv("OSN_SECRET"))

s3$CreateDir("stage2")

s3_stage2 <- arrow::s3_bucket("bio230121-bucket01/flare/drivers/met/gefs-v12/stage2",
                       endpoint_override = "renc.osn.xsede.org",
                       access_key= Sys.getenv("OSN_KEY"),
                       secret_key= Sys.getenv("OSN_SECRET"))

df <- arrow::open_dataset(s3_stage2) |>
  dplyr::distinct(reference_datetime) |>
  dplyr::collect()


curr_date <- Sys.Date()
last_week <- dplyr::tibble(reference_datetime = as.character(seq(curr_date - lubridate::days(7), curr_date - lubridate::days(1), by = "1 day")))

missing_dates <- dplyr::anti_join(last_week, df, by = "reference_datetime") |> dplyr::pull(reference_datetime)

if(length(missing_dates) > 0){
  for(i in 1:length(missing_dates)){
    
    print(missing_dates[i])
    
    bucket <- paste0("bio230121-bucket01/flare/drivers/met/gefs-v12/stage1/reference_datetime=",missing_dates[i])
    
    endpoint_override <- "https://renc.osn.xsede.org"
    s3 <- arrow::s3_bucket(paste0(bucket),
                           endpoint_override = endpoint_override,
                           anonymous = TRUE)
    
    site_df <- arrow::open_dataset(s3) |>
      dplyr::filter(variable %in% c("PRES","TMP","RH","UGRD","VGRD","APCP","DSWRF","DLWRF")) |>
      dplyr::filter(site_id %in% site_list$site_id) |>
      dplyr::collect() |>
      dplyr::mutate(reference_datetime = missing_dates[i])
    
    hourly_df <- to_hourly(site_df, use_solar_geom = TRUE, psuedo = FALSE) |>
      dplyr::mutate(ensemble = as.numeric(stringr::str_sub(ensemble, start = 4, end = 5)),
                    reference_datetime = lubridate::as_date(reference_datetime)) |>
      dplyr::rename(parameter = ensemble)
    
    arrow::write_dataset(hourly_df, path = s3_stage2, partitioning = c("reference_datetime", "site_id"))
  }
}