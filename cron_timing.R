#renv::restore()
#remotes::install_github("rqthomas/cronR")
#remotes::install_deps()
library(cronR)

home_dir <-  path.expand("/home/rstudio/aws_noaa")
log_dir <- path.expand("home/rstudio/log/cron")
fs::dir_create(log_dir)

#the 840 Horizon is finished at 3:05 AM UTC on next day
#Note the Cron timing on the machine is UTC
cmd <- cronR::cron_rscript(rscript = file.path(home_dir, "stage2_combined.R"),
                           rscript_log = file.path(log_dir, "stage2_combined.log"),
                           log_append = FALSE,
                           workdir = file.path(home_dir))
                           #trailing_arg = "curl -fsS -m 10 --retry 5 -o /dev/null https://hc-ping.com/3c7408aa-070a-4985-a856-356606a297b7")
cronR::cron_add(command = cmd, frequency = '0 5 * * *', id = 'stage2_combined')

#3 hours is enough time for yesterday to finish downloading before downloading today

## GEFS arrow
#the 384 Horizon is finished at 6:30 AM UTC on same day
#Note the Cron timing on the machine is UTC
cmd <- cronR::cron_rscript(rscript = file.path(home_dir, "stage3_combined.R"),
                           rscript_log = file.path(log_dir, "stage3_combined.log"),
                           log_append = FALSE,
                           workdir = file.path(home_dir))
                           #trailing_arg = "curl -fsS -m 10 --retry 5 -o /dev/null https://hc-ping.com/6c3dec04-631a-4a8e-8c55-8837f2827e07")
cronR::cron_add(command = cmd, frequency = '0 23 * * *', id = 'stage3_combined')
