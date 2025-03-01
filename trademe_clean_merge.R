library(tidyverse)
library(aws.s3)
library(jsonlite)
library(dplyr)
library(snakecase)
library(sf)
library(tmap)

################################################################################################################################
################################################################################################################################
## Date variable conversion
convDate <- function(x) {
  as.Date((x %>% str_remove_all('[/Date()]') %>% as.numeric())/1000/60/60/24, origin = "1970-01-01")
}

################################################################################################################################
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
  rm(file_list, listings_list, json_files)
  names(df) <- to_snake_case(names(df))
  
  return(df)
}

################################################################################################################################
## Duplicated rows in a single cross section seem to be mostly caused by the as_at variable. Possibly because of asynchronous paged fetch.
## Check for 'as_at' duplication and remove duplicates
cleanAsAtDuplicates <- function(a) {
  check_i <- a[which(duplicated(a$listing_id)), "listing_id"]
  rm_row_ids <- c()
  for(i in 1:length(check_i)) {
    tmp <- a %>% as_tibble %>% filter(listing_id == check_i[i]) %>% select(-as_at)
    if(nrow(tmp) == 2){
      if(identical(tmp[1,], tmp[2,])){
        rm_row_ids[i] <- which(a$listing_id == check_i[i])[2]
      } else { return }
    } else { return }
  }
  b <- a[-rm_row_ids,]
  if(!any(duplicated(b$listing_id))) {
    print("Successful duplicate cleaning")
    return(b)  
  } else { 
    print("Failed: there are duplicates for some other reason than the as_at var")
    return(b)  
  }
}

################################################################################################################################
################################################################################################################################



a <- getS3Files(today()-10) %>% cleanAsAtDuplicates() %>% as_tibble()
b <- getS3Files(today()-9) %>% cleanAsAtDuplicates() %>% as_tibble()


## Verify no listing_id duplicates should return FALSE
any(duplicated(a$listing))
any(duplicated(b$listing))

tmp <- inner_join(a,b, by = "listing_id")$listing_id
d <- a %>% filter(listing_id %in% tmp)
e <- b %>% filter(listing_id %in% tmp)


d <- d %>% select(where(~ class(.) %in% c("character", "logical", "integer", "numeric")))
e <- e %>% select(where(~ class(.) %in% c("character", "logical", "integer", "numeric")))


test <- rbind(d[which(d$listing_id == tmp[1]), ], e[which(e$listing_id == tmp[1]), ])
names(test)[test[1,] != test[2,]]
test <- test %>% select(-as_at)
identical(test[1,], test[2,])



common_vars <- setdiff(intersect(names(a), names(b)), "as_at")
inner_join(a, b, by = common_vars)


