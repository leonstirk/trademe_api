
# 1. Get all daily listings from search function
# 2. Process each days listings and append to a 'master' dataset
# 3. Get the vector of new listings added that day and get the individual listing info for each of them
# 3.1 Write a lambda to get individual listings
# 3.2 Get R to hit the individual lambda? (look back at the original eventfinda code where I had the frontend hitting the lambda via a http GET request)
# 4. Clean up the raw data and append the individual listing data to the search data rows in the master dataset


##########################################################################################

## Get individual listing info
## https://api.trademe.co.nz/v1/Listings/{listingId}.{file_format}

## sample listing_ids
## ids <- sample(catalogue_df$listing_id, 1000)
## or get all ids
ids <- catalogue_df$listing_id
## Scraping loop
listing_list <- list()
for(i in 1:length(ids)) {
  url_listing <- paste0('https://api.trademe.co.nz/v1/Listings/',ids[i],'.json')
  tmp <- GET(url_listing, add_headers(Authorization = header))
  listing_list[[i]] <- tmp
  
  sl <- runif(1,0.5,2)
  Sys.sleep(sl)
}

listing_df <- bind_rows(lapply(listing_list, function(x) {
  a <- fromJSON(content(x, as='text'))
  if(!any(!is.na(a$Error))){
    at <- a$Attributes
    ot_n <- c('ListingId', 'BidderAndWatchers', 'ViewCount', 'Title', 'Body')
    ot <- a[ot_n]
    names(ot) <- ot_n
    ot <- lapply(ot, function(x) { if(is.null(x)) { NA } else { x } }) %>% unlist
    b <- c(ot, at[,'Value'])
    names(b) <- c(ot_n, at[,'Name'])
    return(b)
  }
}))
## All var names to snake case
names(listing_df) <- to_any_case(names(listing_df), 'snake')

listing_df$bedrooms <- sapply(listing_df$bedrooms, function(x) {
  if(!is.na(x) & str_detect(x, 'bedroom')) { as.numeric(str_remove_all(x, '[:alpha:]')) }
  else if (is.na(x)) { NA }
})

listing_df$bathrooms <- sapply(listing_df$bathrooms, function(x) {
  if(!is.na(x) & str_detect(x, 'bathroom')) { as.numeric(str_remove_all(x, '[:alpha:]')) }
  else if (is.na(x)) { NA }
})

listing_df$floor_area <- sapply(listing_df$floor_area, function(x) {
  if(!is.na(x) & str_detect(x, 'm²')){ as.numeric(str_remove_all(x, 'm²')) }
  else if (is.na(x)) { NA }
})

listing_df$land_area <- sapply(listing_df$land_area, function(x) {
  if(!is.na(x) & str_detect(x, 'm²')){ as.numeric(str_remove_all(x, 'm²'))/10000 }
  else if(!is.na(x) & str_detect(x, 'hectare')) { as.numeric(str_remove_all(x, '[:alpha:]')) }
  else if (is.na(x)) { NA }
})

n <- c('view_count', 'bidder_and_watchers')
listing_df[,n]  <- lapply(listing_df[,n], as.numeric)

f <- c('property_type', 'district', 'region')
listing_df[,f]  <- lapply(listing_df[,f], as.factor)

rm(n,f)

names(listing_df)[which(names(listing_df) == 'price')] <- 'price_display'
names(listing_df)[which(names(listing_df) == 'rateable_value_rv')] <- 'rateable_value'

## Save tibble of individual listing queries
## filename <- paste('datasets/listing_', today, '.rds', sep = '')
## saveRDS(listing_df, filename)

####################################################################

## Merge individual columns with catalogue df
drop_names <- names(listing_df)[which(!names(listing_df) %in% names(catalogue_df))]
merged_df <- left_join(listing_df[,c('listing_id', drop_names)], catalogue_df, by = 'listing_id')
rm(drop_names)

merged_df$ln_price <- log(merged_df$price_integer)
merged_df$ln_rv <- log(merged_df$rateable_value)

## filename <- paste('datasets/merged_', today, '.rds', sep = '')
## saveRDS(merged_df, filename)

####################################################################

master_df <- readRDS('datasets/master_df.rds')
filename <- paste('datasets/master_df_', today, '.rds', sep = '')
saveRDS(master_df, filename)
old_df <- master_df[which(!master_df$listing_id %in% merged_df$listing_id),]
## new_df <- merged_df[which(!merged_df$listing_id %in% master_df$listing_id),]
master_df <- bind_rows(old_df, merged_df)
saveRDS(master_df, 'datasets/master_df.rds')

####################################################################

## rm(list = ls())
## dat <- readRDS('datasets/master_df.rds')
## dat_sf <- dat %>% filter(access_date == Sys.Date()) %>% st_as_sf(coords = c('lon', 'lat'), crs = 4326)
## tmap_mode('view')
## tm_shape(dat_sf) + tm_dots(col = 'price_integer')
