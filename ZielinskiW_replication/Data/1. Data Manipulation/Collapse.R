### DATA MANIPULATION TO CHANGE THE TRIP DEFINITION ###

rm(list=ls())
### DECOMMENT IF USING "PACMAN" THE FIRST TIME
#install.packages("pacman")
pacman::p_load(data.table, R.matlab)
### SET WD
setwd("/Users/mac/Documents/IO/tracing_the_woes/Data")

df = readMat("./0. Original/M100_db1b.mat")
dt = as.data.table(df[["data"]])

# Define functions to apply to each column
new_col_names = c(
  "mkt_idx", "carrier_idx", "prod_share", "fare", "layovers", 
  "departures", "origin_hub", "dest_hub", "distance", "cities_non_stop_origin",
  "cities_non_stop_dest", "fl_lv_dummy", "routes_within_mkt", "connecting_at_hub",
  "slot_ctrl_airports", "carriers_serving_mkt", "low_cost_carriers", 
  "avg_population_mkt", "predicted_fares", "passengers_origin", "passengers_dest",
  "carriers_share_origin", "carriers_share_dest", "population_origin", "population_dest",
  "perc_non_stop_flights", "perc_flights_origin_hubs", "perc_flights_arrive_hubs",
  "alt_measure_departures", "quantile_25th_fitted_fares", "quantile_75th_fitted_fares",
  "num_seats", "perc_flights_by_commuters", "perc_15_min_delays",
  "perc_30_min_delays", "lc_dummy", "fitted_departures_non_stop",
  "fitted_departures_connecting", "prod_identifier"
)
setnames(dt, old = names(dt), new = new_col_names)

# Create a new variable - distance between cities
dist_city = dt[,min(distance),by=mkt_idx]
setnames(dist_city, names(dist_city), c("mkt_idx", "distance_city"))
# distance - market number combination
collapse_dist = dist_city[, max(mkt_idx), by=distance_city]
setnames(collapse_dist, names(collapse_dist), c("distance_city", "mkt_idx"))
# Create new market index based on old market distances between cities
collapse_dist$new_mkt_idx = 1:nrow(collapse_dist)
collapse_dist[,mkt_idx:=NULL]

### Get prod identifier and market index
# Sort the data table by the specified column
dt1 = merge(dt,dist_city, by = "mkt_idx", all.x = TRUE)
dt2 = merge(dt1,collapse_dist, by = "distance_city", all.x = TRUE)
dt2$mkt_idx = dt2$new_mkt_idx
dt2[,new_mkt_idx:=NULL]
dt2[,distance_city:=NULL]

functions = list(
  prod_share = mean,
  layovers = mean,
  departures = mean,
  origin_hub = min,
  dest_hub = max,
  distance = mean,
  cities_non_stop_origin = max,
  cities_non_stop_dest = min,
  fl_lv_dummy = max,
  routes_within_mkt = mean,
  connecting_at_hub = max,
  slot_ctrl_airports = max,
  carriers_serving_mkt = mean,
  low_cost_carriers = mean,
  avg_population_mkt = mean,
  predicted_fares = mean,
  passengers_origin = mean,
  passengers_dest = min,
  carriers_share_origin = mean,
  carriers_share_dest = min,
  population_origin = mean,
  population_dest = min,
  perc_non_stop_flights = mean,
  perc_flights_origin_hubs = mean,
  perc_flights_arrive_hubs = min,
  alt_measure_departures = mean,
  quantile_25th_fitted_fares = mean,
  quantile_75th_fitted_fares = mean,
  num_seats = mean,
  perc_flights_by_commuters = mean,
  perc_15_min_delays = mean,
  perc_30_min_delays = mean,
  lc_dummy = max,
  fitted_departures_non_stop = mean,
  fitted_departures_connecting = mean,
  prod_identifier = min
)

# Group by columns and apply functions
result = dt2[, lapply(names(functions), function(col) functions[[col]](get(col))), by = .(mkt_idx, carrier_idx, fare)]
result1 = result[,c(1,2,4,3,5:ncol(result)), with = FALSE]

new_col_names = c(
  "mkt_idx", "carrier_idx", "prod_share", "fare", "layovers", 
  "departures", "origin_hub", "dest_hub", "distance", "cities_non_stop_origin",
  "cities_non_stop_dest", "fl_lv_dummy", "routes_within_mkt", "connecting_at_hub",
  "slot_ctrl_airports", "carriers_serving_mkt", "low_cost_carriers", 
  "avg_population_mkt", "predicted_fares", "passengers_origin", "passengers_dest",
  "carriers_share_origin", "carriers_share_dest", "population_origin", "population_dest",
  "perc_non_stop_flights", "perc_flights_origin_hubs", "perc_flights_arrive_hubs",
  "alt_measure_departures", "quantile_25th_fitted_fares", "quantile_75th_fitted_fares",
  "num_seats", "perc_flights_by_commuters", "perc_15_min_delays",
  "perc_30_min_delays", "lc_dummy", "fitted_departures_non_stop",
  "fitted_departures_connecting", "prod_identifier"
)

setnames(result1, old = names(result1), new = new_col_names)
result1[,39] = 1:1:nrow(result1)

# Midx Index for the last product in each market
# Test
dt[, Midx := max(prod_identifier), by = mkt_idx]
Midx_test = unique(dt[,Midx])
# Works!
result1[, Midx := max(prod_identifier), by = mkt_idx]
Midx = unique(result1[,Midx])
Midx = sort(Midx)
### M130 MkDist
M130 = result1[,c(39,9)]
M130[,short_flight := as.integer(distance < 500)]
M130[,med_flight := as.integer((distance >= 500 & distance <= 1500))]
M130[,long_flight := as.integer(distance > 1500)]
writeMat("./1. Data Manipulation/M130_MkDist_new.mat", result = list(dt = as.matrix(M130)))

### Cidx
# Test
# Create a unique identifier for market-carrier combinations
dt[, UniqueCombination := paste0(mkt_idx, carrier_idx)]
# Identify unique combinations
unique_combinations = unique(dt$UniqueCombination)
result_dt = data.table(UniqueCombination = unique_combinations)
result_dt[dt, Cidx := i.prod_identifier, on = .(UniqueCombination = UniqueCombination)]
# Works! Proceed with new data

# Create a unique identifier for market-carrier combinations
result1[, UniqueCombination := paste0(mkt_idx, carrier_idx)]
# Identify unique combinations
unique_combinations = unique(result1$UniqueCombination)
Cidx_dt = data.table(UniqueCombination = unique_combinations)
Cidx_dt[result1, Cidx := i.prod_identifier, on = .(UniqueCombination = UniqueCombination)]
Cidx = sort(Cidx_dt[,Cidx])

### Market index Mid
# Test
Mid_test = dt[,1]
# Works! For new set:
Mid = result1[,1]

### nJ
nJ_test = dt[, .(NumProducts = .N), by = mkt_idx]
# Works!
nJ = result1[, .(NumProducts = .N), by = mkt_idx]

### nJC
nJC_test = dt[, .(NumProducts = .N), by = list(mkt_idx,carrier_idx)]
# Works!
nJC = result1[, .(NumProducts = .N), by = list(mkt_idx,carrier_idx)]

### nM
nM = max(result1[,mkt_idx])
### nC
nC = length(Cidx)
### nobs
nobs = nrow(result1)

### i
i = nC

### Write the results
result2 = result1[,1:39]
writeMat("./1. Data Manipulation/M100_db1_new.mat", 
         result = list(dt = as.matrix(result2)),
         Cidx = Cidx,
         Mid = Mid,
         Midx = Midx,
         i = i,
         nC = nC,
         nJ = nJ,
         nJC = nJC,
         nM = nM,
         nobs = nobs)

### 1999 ###


### DECOMMENT IF USING "PACMAN" THE FIRST TIME
rm(list=ls())
#install.packages("pacman")
pacman::p_load(data.table, R.matlab)
df = readMat("./0. Original/P100_db1b.mat")
dt = as.data.table(df[["data"]])
# Group by: carrier index, fare
# Define functions to apply to each column
new_col_names = c(
  "mkt_idx", "carrier_idx", "prod_share", "fare", "layovers", 
  "departures", "origin_hub", "dest_hub", "distance", "cities_non_stop_origin",
  "cities_non_stop_dest", "fl_lv_dummy", "routes_within_mkt", "connecting_at_hub",
  "slot_ctrl_airports", "carriers_serving_mkt", "low_cost_carriers", 
  "avg_population_mkt", "predicted_fares", "passengers_origin", "passengers_dest",
  "carriers_share_origin", "carriers_share_dest", "population_origin", "population_dest",
  "perc_non_stop_flights", "perc_flights_origin_hubs", "perc_flights_arrive_hubs",
  "alt_measure_departures", "quantile_25th_fitted_fares", "quantile_75th_fitted_fares",
  "num_seats", "perc_flights_by_commuters", "perc_15_min_delays",
  "perc_30_min_delays", "lc_dummy", "fitted_departures_non_stop",
  "fitted_departures_connecting", "prod_identifier"
)
setnames(dt, old = names(dt), new = new_col_names)

# Create a new variable - distance between cities
dist_city = dt[,min(distance),by=mkt_idx]
setnames(dist_city, names(dist_city), c("mkt_idx", "distance_city"))
# distance - market number combination
collapse_dist = dist_city[, max(mkt_idx), by=distance_city]
setnames(collapse_dist, names(collapse_dist), c("distance_city", "mkt_idx"))
# Create new market index based on old market distances between cities
collapse_dist$new_mkt_idx = 1:nrow(collapse_dist)
collapse_dist[,mkt_idx:=NULL]

### Get prod identifier and market index
# Sort the data table by the specified column
dt1 = merge(dt,dist_city, by = "mkt_idx", all.x = TRUE)
dt2 = merge(dt1,collapse_dist, by = "distance_city", all.x = TRUE)
dt2$mkt_idx = dt2$new_mkt_idx
dt2[,new_mkt_idx:=NULL]
dt2[,distance_city:=NULL]

functions = list(
  prod_share = mean,
  layovers = mean,
  departures = mean,
  origin_hub = min,
  dest_hub = max,
  distance = mean,
  cities_non_stop_origin = max,
  cities_non_stop_dest = min,
  fl_lv_dummy = max,
  routes_within_mkt = mean,
  connecting_at_hub = max,
  slot_ctrl_airports = max,
  carriers_serving_mkt = mean,
  low_cost_carriers = mean,
  avg_population_mkt = mean,
  predicted_fares = mean,
  passengers_origin = mean,
  passengers_dest = min,
  carriers_share_origin = mean,
  carriers_share_dest = min,
  population_origin = mean,
  population_dest = min,
  perc_non_stop_flights = mean,
  perc_flights_origin_hubs = mean,
  perc_flights_arrive_hubs = min,
  alt_measure_departures = mean,
  quantile_25th_fitted_fares = mean,
  quantile_75th_fitted_fares = mean,
  num_seats = mean,
  perc_flights_by_commuters = mean,
  perc_15_min_delays = mean,
  perc_30_min_delays = mean,
  lc_dummy = max,
  fitted_departures_non_stop = mean,
  fitted_departures_connecting = mean,
  prod_identifier = min
)

# Group by columns and apply functions
result = dt2[, lapply(names(functions), function(col) functions[[col]](get(col))), by = .(mkt_idx, carrier_idx, fare)]
result1 = result[,c(1,2,4,3,5:ncol(result)), with = FALSE]

new_col_names = c(
  "mkt_idx", "carrier_idx", "prod_share", "fare", "layovers", 
  "departures", "origin_hub", "dest_hub", "distance", "cities_non_stop_origin",
  "cities_non_stop_dest", "fl_lv_dummy", "routes_within_mkt", "connecting_at_hub",
  "slot_ctrl_airports", "carriers_serving_mkt", "low_cost_carriers", 
  "avg_population_mkt", "predicted_fares", "passengers_origin", "passengers_dest",
  "carriers_share_origin", "carriers_share_dest", "population_origin", "population_dest",
  "perc_non_stop_flights", "perc_flights_origin_hubs", "perc_flights_arrive_hubs",
  "alt_measure_departures", "quantile_25th_fitted_fares", "quantile_75th_fitted_fares",
  "num_seats", "perc_flights_by_commuters", "perc_15_min_delays",
  "perc_30_min_delays", "lc_dummy", "fitted_departures_non_stop",
  "fitted_departures_connecting", "prod_identifier"
)

setnames(result1, old = names(result1), new = new_col_names)
result1[,39] = 1:1:nrow(result1)

# Midx Index for the last product in each market
# Test
dt[, Midx := max(prod_identifier), by = mkt_idx]
Midx_test = unique(dt[,Midx])
# Works!
result1[, Midx := max(prod_identifier), by = mkt_idx]
Midx = unique(result1[,Midx])
Midx = sort(Midx)
### M130
M130 = result1[,c(39,9)]
M130[,short_flight := as.integer(distance < 500)]
M130[,med_flight := as.integer((distance >= 500 & distance <= 1500))]
M130[,long_flight := as.integer(distance > 1500)]
writeMat("./1. Data Manipulation/P130_MkDist_new.mat", result = list(dt = as.matrix(M130)))

### Cidx
# Test
# Create a unique identifier for market-carrier combinations
dt[, UniqueCombination := paste0(mkt_idx, carrier_idx)]
# Identify unique combinations
unique_combinations = unique(dt$UniqueCombination)
result_dt = data.table(UniqueCombination = unique_combinations)
result_dt[dt, Cidx := i.prod_identifier, on = .(UniqueCombination = UniqueCombination)]
# Works! Proceed with new data

# Create a unique identifier for market-carrier combinations
result1[, UniqueCombination := paste0(mkt_idx, carrier_idx)]
# Identify unique combinations
unique_combinations = unique(result1$UniqueCombination)
Cidx_dt = data.table(UniqueCombination = unique_combinations)
Cidx_dt[result1, Cidx := i.prod_identifier, on = .(UniqueCombination = UniqueCombination)]
Cidx = sort(Cidx_dt[,Cidx])

### Market index Mid
# Test
Mid_test = dt[,1]
# Works! For new set:
Mid = result1[,1]

### nJ
nJ_test = dt[, .(NumProducts = .N), by = mkt_idx]
# Works!
nJ = result1[, .(NumProducts = .N), by = mkt_idx]

### nJC
nJC_test = dt[, .(NumProducts = .N), by = list(mkt_idx,carrier_idx)]
# Works!
nJC = result1[, .(NumProducts = .N), by = list(mkt_idx,carrier_idx)]

### nM
nM = max(result1[,mkt_idx])
### nC
nC = length(Cidx)
### nobs
nobs = nrow(result1)

### i
i = nC

### Write the results
result2 = result1[,1:39]
writeMat("./1. Data Manipulation/P100_db1_new.mat", 
         result = list(dt = as.matrix(result2)),
         Cidx = Cidx,
         Mid = Mid,
         Midx = Midx,
         i = i,
         nC = nC,
         nJ = nJ,
         nJC = nJC,
         nM = nM,
         nobs = nobs)


