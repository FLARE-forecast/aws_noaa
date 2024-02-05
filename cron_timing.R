#renv::restore()
#remotes::install_github("rqthomas/cronR")
#remotes::install_deps()
library(cronR)

home_dir <-  path.expand("~/aws_noaa")
log_dir <- path.expand("~/log/cron")
fs::dir_create(log_dir)

#the 840 Horizon is finished at 3:05 AM UTC on next day
#Note the Cron timing on the machine is UTC
cmd <- cronR::cron_rscript(rscript = file.path(home_dir, "stage1_yesterday.R"),
                           rscript_log = file.path(log_dir, "noaa_gefs_yesterday.log"),
                           log_append = FALSE,
                           #cmd = "/usr/local/bin/r", # use litter, more robust on CLI
                           workdir = file.path(home_dir))
                           #trailing_arg = "curl -fsS -m 10 --retry 5 -o /dev/null https://hc-ping.com/3c7408aa-070a-4985-a856-356606a297b7")
cronR::cron_add(command = cmd, frequency = '0 4 * * *', id = 'noaa_gefs_yesterday')

#3 hours is enough time for yesterday to finish downloading before downloading today

## GEFS arrow
#the 384 Horizon is finished at 6:30 AM UTC on same day
#Note the Cron timing on the machine is UTC
cmd <- cronR::cron_rscript(rscript = file.path(home_dir, "combined_stages_today.R"),
                           rscript_log = file.path(log_dir, "noaa_gefs_today.log"),
                           log_append = FALSE,
                           #cmd = "/usr/local/bin/r", # use litter, more robust on CLI
                           workdir = file.path(home_dir))
                           #trailing_arg = "curl -fsS -m 10 --retry 5 -o /dev/null https://hc-ping.com/6c3dec04-631a-4a8e-8c55-8837f2827e07")
cronR::cron_add(command = cmd, frequency = '0 7 * * *', id = 'noaa_gefs_today')

cmd <- cronR::cron_rscript(rscript = file.path(home_dir, "backup_s3_to_osn.R"),
                           rscript_log = file.path(log_dir, "backup_s3_to_osn.log"),
                           log_append = FALSE,
                           #cmd = "/usr/local/bin/r", # use litter, more robust on CLI
                           workdir = file.path(home_dir))
#trailing_arg = "curl -fsS -m 10 --retry 5 -o /dev/null https://hc-ping.com/6c3dec04-631a-4a8e-8c55-8837f2827e07")
cronR::cron_add(command = cmd, frequency = '0 0 * * *', id = 'backup_s3_to_osn')