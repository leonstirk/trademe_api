library(tidyverse)
library(aws.s3)
library(jsonlite)
library(dplyr)
library(snakecase)
library(sf)
library(tmap)

## Load data files for a single day.
## Stitch them together

getS3Files <- function(the_date) {
  ## the_date <- '2025-02-19'

# Define bucket and folder path
bucket_name <- "trademe-property-listings"
date_folder <- paste0("property-listings/",the_date,"/")  # Update with the actual date
aws_region <- "ap-southeast-2"  # Specify your correct S3 region

# List all files in the date folder (include the region)
file_list <- get_bucket(bucket = bucket_name, prefix = date_folder, region = aws_region)

# Extract file keys (paths) for JSON files
json_files <- sapply(file_list, function(x) x$Key)
json_files <- json_files[grepl("page_.*\\.json$", json_files)]  # Filter only JSON pages

# Download and combine all pages
listings_list <- lapply(json_files, function(file_key) {
  message(paste("Fetching:", file_key))
  
  # Read file from S3
  json_raw <- rawToChar(get_object(file_key, bucket = bucket_name, region = aws_region))
  json_data <- fromJSON(json_raw)
  
  # Convert to dataframe
  as.data.frame(json_data)
})

# Combine all pages into a single dataframe
df <- bind_rows(listings_list)
df$date_retrieved <- as.Date(the_date)
rm(file_list, listings_list, json_files)
names(df) <- to_snake_case(names(df))

}

################################################################
## Date variable conversion
convDate <- function(x) {
  as.Date((x %>% str_remove_all('[/Date()]') %>% as.numeric())/1000/60/60/24, origin = "1970-01-01")
}
################################################################

## Analysis
df <- raw_df %>% select(listing_id, title, start_date, end_date, price_display, geographic_location, region, suburb, address, bedrooms, bathrooms, area, land_area, parking, property_type, parking)

df$start_date <- convDate(df$start_date)
df$end_date   <- convDate(df$end_date)
#df$as_at <- convDate(df$as_at)

#df$price_display
## Price variables
df$price_integer <- df$price_display %>% str_replace_all(',','') %>% str_extract_all("(?<=\\$)\\d+") %>%
  lapply(., function(x) { paste(x, collapse = '') }) %>% unlist %>% as.integer()

## Need fixes here for bad data
#df %>% filter(!is.na(price_integer)) %>% select(price_integer, price_display) %>% arrange(price_integer) %>% head(100)
#df %>% filter(!is.na(price_integer)) %>% arrange(price_integer) %>% head(100)
#df$price_integer[!is.na(df$price_integer)] %>% quantile(probs = seq(0, 1, by = 0.1))

df$price_category <- df$price_display %>% str_remove("\\d.*$") %>% str_remove("[\\$,]") %>% str_remove("\\s*$") %>% str_remove("\\s(by|on)$") %>%
  lapply(., function(x) { paste(x, collapse = '') }) %>% unlist %>% str_remove(' $') %>% as.factor

## Pairwise merging issues
###############################################
## Bind cross sections

df_joined <- full_join(df, df_1, by = "listing_id")
unique(df_joined$listing_id)

which(duplicated(df$listing_id))
which(duplicated(df_1$listing_id))

###############################################

raw_df <- df
rm(df)

