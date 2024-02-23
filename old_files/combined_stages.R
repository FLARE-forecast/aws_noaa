
message("Running Stage 1")
source("stage1.R")
message("Running Stage 2")
source("stage2.R")
message("Running Stage 3")
source("stage3.R")

message("Checking downloads")
readRenviron("~/.Renviron")
message(Sys.time())
Sys.unsetenv("AWS_DEFAULT_REGION")
Sys.unsetenv("AWS_S3_ENDPOINT")
Sys.setenv(AWS_EC2_METADATA_DISABLED="TRUE")

message("Check Stage 1 cycle 0")
s3_2 <- arrow::s3_bucket("bio230121-bucket01/vt_backup/drivers/noaa/gefs-v12/stage1/0", endpoint_override = "renc.osn.xsede.org")
d1 <- arrow::open_dataset(s3_2, partitioning = "reference_date") |>  
  dplyr::filter(variable == "TMP") |>  
  dplyr::group_by(reference_date, parameter) |>   
  dplyr::summarise(max = max(horizon)) |> 
  dplyr::collect()

max_date <- max(d1$reference_date)


d1 |>  
  dplyr::filter(reference_date == max_date) |>  
  dplyr::arrange(parameter) |>  
  print(n = 100)

message("Check Stage 1 cycle 6,12,18")

dates <- as.character(c(lubridate::as_date(max_date), lubridate::as_date(max_date) - lubridate::days(1)))

s3_2 <- arrow::s3_bucket("bio230121-bucket01/vt_backup/drivers/noaa/gefs-v12/stage1", endpoint_override = "renc.osn.xsede.org")
arrow::open_dataset(s3_2, partitioning = c("cycle","reference_date")) |>  
  dplyr::filter(variable == "TMP") |>  
  dplyr::filter(cycle %in% c(6,12,18), site_id == "fcre", reference_date %in% dates, parameter == 1) |> 
  dplyr::select(reference_date, cycle, horizon, reference_datetime, datetime) |>  
  dplyr::collect() |> 
  dplyr::arrange(dplyr::desc(reference_date), dplyr::desc(cycle)) |>  
  print(n = 93)

message("Check Stage 2")

s3_2 <- arrow::s3_bucket("bio230121-bucket01/vt_backup/drivers/noaa/gefs-v12/stage2/parquet/0", endpoint_override = "renc.osn.xsede.org")
arrow::open_dataset(s3_2, partitioning = "reference_date") |>  
  dplyr::filter(variable == "air_temperature") |> 
  dplyr::group_by(reference_date, parameter) |>  
  dplyr::summarise(max = max(horizon)) |>  
  dplyr::collect() |> 
  dplyr::arrange(dplyr::desc(reference_date), parameter) |>  
  print(n = 62)

message("Check Stage 3")

s3_2 <- arrow::s3_bucket("bio230121-bucket01/vt_backup/drivers/noaa/gefs-v12/stage3/parquet/fcre", endpoint_override = "renc.osn.xsede.org")
arrow::open_dataset(s3_2) |> 
  dplyr::filter(variable == "air_temperature") |> 
  dplyr::collect() |> 
  dplyr::filter(parameter == 1) |> 
  dplyr::select(datetime) |> 
  dplyr::arrange(desc(datetime)) |> 
  print(n = 48)