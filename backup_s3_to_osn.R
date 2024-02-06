minioclient::mc_alias_set("vt_s3",
                          "s3.flare-forecast.org",
                          Sys.getenv("AWS_ACCESS_KEY_ID"),
                          Sys.getenv("AWS_SECRET_ACCESS_KEY"))

minioclient::mc_alias_set("osn",
                          "renc.osn.xsede.org",
                          Sys.getenv("OSN_KEY"),
                          Sys.getenv("OSN_SECRET"))

minioclient::mc_mirror(from = "vt_s3/drivers/noaa/gefs-v12-reprocess", to = "osn/bio230121-bucket01/vt_backup/drivers/noaa/gefs-v12-reprocess")
minioclient::mc_mirror(from = "vt_s3/forecasts", to = "osn/bio230121-bucket01/vt_backup/forecasts")
minioclient::mc_mirror(from = "vt_s3/scores/parquet", to = "osn/bio230121-bucket01/vt_backup/scores/parquet")
minioclient::mc_mirror(from = "vt_s3/restart", to = "osn/bio230121-bucket01/vt_backup/restart")

RCurl::url.exists("https://hc-ping.com/07b470d1-3c51-4d05-808b-a89d3888c579", timeout = 5)
