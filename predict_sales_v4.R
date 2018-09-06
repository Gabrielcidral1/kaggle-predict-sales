pacman::p_load(readr, forecast, tidyquant, timetk, sweep, reshape2, doParallel)

# Parallel processing

cl <- makeCluster(detectCores()-1)
registerDoParallel(cl)


train <- read_csv("C:/Users/gabri/Desktop/Ubiqum/R/Kaggle/train.csv", 
                  col_types = cols(date = col_date(format = "%Y-%m-%d")))


by_store.item <- train %>%
  group_by(store, item) %>%
  nest() # esta funcion nos permite crear tibbles dentro del dataframe


by_store.item <- by_store.item %>%
  mutate(data.ts = map(.x       = data, 
                       .f       = tk_ts, 
                       select   = -date, 
                       start    = 2013,
                       freq     = 365))

x <- sample_n(by_store.item, 1)

t <- proc.time()

by_store.item <- by_store.item %>%
              mutate(fit = map(data.ts, auto.arima))

save(by_store.item, file = "model500.csv")

x <- load("model500.csv")

(proc.time()-t)

# check error
by_store.item %>%
  mutate(glance = map(fit, sw_glance)) %>%
  unnest(glance, .drop = TRUE)

# check residuals
augment_fit_ets <- by_store.item %>%
  mutate(augment = map(fit, sw_augment, timetk_idx = TRUE, rename_index = "date")) %>%
  unnest(augment, .drop = TRUE)

# Forecasting 
by_store.item <- by_store.item %>%
  mutate(fcast.ets = map(fit, forecast, h = 90))

by_store.item_sample <- sample_n(by_store.item, 2) 

actual.pred <- by_store.item %>%
  mutate(sweep = map(fcast.ets, sw_sweep, fitted = F, timetk_idx = TRUE)) %>%
  unnest(sweep)

actual.pred.sample <- by_store.item_sample %>%
  mutate(sweep = map(fcast.ets, sw_sweep, fitted = F, timetk_idx = TRUE)) %>%
  unnest(sweep)

pred <- actual.pred %>% filter(key == "forecast") %>% select(store,item,index,value) 

# Submission

test <- read_csv("C:/Users/gabri/Desktop/Ubiqum/R/Kaggle/test.csv", 
                 col_types = cols(date = col_date(format = "%Y-%m-%d")))

pred$index <- as.Date(pred$index)
submission <- left_join(test,pred, by = c("store" = "store", "item" = "item", "date" = "index"))

# Stop do Parallel

stopCluster(cl)
rm(cl)
registerDoSEQ()

