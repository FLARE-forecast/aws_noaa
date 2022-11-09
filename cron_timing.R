#renv::restore()
#remotes::install_github("rqthomas/cronR")
#remotes::install_deps()
library(cronR)

home_dir <-  path.expand("~/aws_noaa")
log_dir <- path.expand("~/log/cron")
fs::dir_create(log_dir)

## GEFS arrow 
cmd <- cronR::cron_rscript(rscript = file.path(home_dir, "stage1.R"),
                           rscript_log = file.path(log_dir, "gefs4cast-snapshot.log"),
                           log_append = FALSE,
                           cmd = "/usr/local/bin/r", # use litter, more robust on CLI
                           workdir = file.path(home_dir),
                           trailing_arg = "curl -fsS -m 10 --retry 5 -o /dev/null https://hc-ping.com/3c7408aa-070a-4985-a856-356606a297b7")
cronR::cron_add(command = cmd, frequency = '0 */4 * * *', id = 'gefs4cast')

cmd <- cronR::cron_rscript(rscript = file.path(home_dir, "stage2.R"),
                           rscript_log = file.path(log_dir, "gefs4cast_stage2.log"),
                           log_append = FALSE,
                           cmd = "/usr/local/bin/r", # use litter, more robust on CLI
                           workdir = file.path(home_dir),
                           trailing_arg = "curl -fsS -m 10 --retry 5 -o /dev/null https://hc-ping.com/c1d4a193-b174-44c6-85f9-38622f61f6d5")
cronR::cron_add(command = cmd, frequency = '30 4 * * *', id = 'gefs4cast-stage2')

cmd <- cronR::cron_rscript(rscript = file.path(home_dir, "stage3.R"),
                           rscript_log = file.path(log_dir, "gefs4cast_stage3.log"),
                           log_append = FALSE,
                           cmd = "/usr/local/bin/r", # use litter, more robust on CLI
                           workdir = file.path(home_dir),
                           trailing_arg = "curl -fsS -m 10 --retry 5 -o /dev/null https://hc-ping.com/6c3dec04-631a-4a8e-8c55-8837f2827e07")
cronR::cron_add(command = cmd, frequency = '30 8 * * *', id = 'gefs4cast-stage3')
