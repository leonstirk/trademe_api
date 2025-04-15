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

checkVarsEquivalent <- function(dat, char, int) {
  char_var <- pull(dat[,char]) %>% as.character
  int_var <- pull(dat[,int])
  mapping <- setNames(unique(int_var), unique(char_var))
  char_as_int <- unname(mapping[char_var])
  # prove - equivalent vars return TRUE
  return(identical(char_as_int, int_var))
}

################################################################################################################################
## Convert price_display to an integer price value
getPriceInteger <- function(df) { 
  df$price_display %>% str_replace_all(',','') %>% str_extract_all("(?<=\\$)\\d+") %>%
    lapply(., function(x) { paste(x, collapse = '') }) %>% unlist %>% as.integer()
}

################################################################################################################################
## Extract the sale type (e.g. asking price, tender, etc) from price_display
getPriceCategory <- function(df) {
  df$price_display %>% str_remove("\\d.*$") %>% str_remove("[\\$,]") %>% str_remove("\\s*$") %>% str_remove("\\s(by|on)$") %>%
    lapply(., function(x) { paste(x, collapse = '') }) %>% unlist %>% str_remove(' $') %>% as.factor
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
      } else {
        
        # identify what is not identical 
        e <- names(tmp)[which(sapply(names(tmp), function(n) { !identical(tmp[1,n],tmp[2,n]) }))]
        print(paste('row', i, 'duplicated rows not identical (excl. as_at) in variable(s):', e))
        # find which row with the non-null observation in the identified 
        d <- tmp[,e] %>% is.na()
          if(any(d)) {
            # remove the row with null
            rm_row_ids[i] <- which(a$listing_id == check_i[i])[which(d)]
            return
          } else {
            # otherwise just pick something
            rm_row_ids[i] <- which(a$listing_id == check_i[i])[2]
            return
          } 
        }
    } else { 
      print('more than one duplicated row')
      # probably need to move to an append based system for rm_row_ids if this ever becomes a problem
      return }
  }
  # print(rm_row_ids)
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
### Testing individual cross sections

a <- getS3Files(today()-10) %>% cleanAsAtDuplicates() %>% as_tibble()
b <- getS3Files(today()-9) %>% cleanAsAtDuplicates() %>% as_tibble()
d <- getS3Files(today()-8) %>% cleanAsAtDuplicates() %>% as_tibble()
e <- getS3Files(today()-7) %>% cleanAsAtDuplicates() %>% as_tibble()
f <- getS3Files(today()-6) %>% cleanAsAtDuplicates() %>% as_tibble()

## Verify no listing_id duplicates should return FALSE
any(duplicated(a$listing_id))
any(duplicated(b$listing_id))

################################################################################################################################



### Exclude because useless
x <- c('start_price', 'listing_length', 'note_date', 'reserve_state', 'is_classified', 'additional_data', 'listing_group', 'listing_platform', 'property_id', 'is_new')
### Exclude because too hard for now/ needs NLP
# Investigate parking first it seems the most tractable - ask ChatGPT
# Also there is a draft open-homes scoring function in 'variable_info.R'
y <- c('amenities', 'best_contact_time', 'viewing_instructions', 'parking', 'open_homes')
## Exclude because pictures/video etc
z <- c('picture_href','photo_urls','has_embedded_video','has_3_d_tour')
##

## check names consistency
z <- lapply(list(a,b,d,e,f), function(x) { names(x) })

check_variable_consistency <- function(var_list) {
  # Convert vectors to sorted sets
  ref_set <- sort(unique(var_list[[1]]))  # Use first vector as reference
  inconsistencies <- list()
  
  for (i in seq_along(var_list)) {
    current_set <- sort(unique(var_list[[i]]))
    
    if (!identical(ref_set, current_set)) {
      missing <- setdiff(ref_set, current_set)
      extra <- setdiff(current_set, ref_set)
      
      inconsistencies[[names(var_list)[i]]] <- list(
        missing_values = missing,
        extra_values = extra
      )
    }
  }
  
  if (length(inconsistencies) == 0) {
    return("All variable name vectors contain identical elements.")
  } else {
    return(inconsistencies)
  }
}

check_variable_consistency(y)

a %>% select(-c())



### Include trademe flairs
## is there any value associated with paying for the trademe flairs?
# c('is featured', 'has_gallery', 'is_bold', 'is_highlighted')
a[,c('is_featured','has_gallery','is_bold','is_highlighted')]
## listing_extras appear to exclusively be 'premium_listing'
a[,'listing_extras'] %>% pull %>% map_chr( ~ paste(.x, collapse = ",")) %>% as.factor() %>% levels()
a[,'listing_extras'] %>% pull %>% map_chr( ~ paste(.x, collapse = ",")) %>% length == nrow(a)
a[,'listing_extras'] %>% pull %>% map_chr( ~ paste(.x, collapse = ",")) %>% table
## premium_package_code
a[,'premium_package_code'] %>% pull %>% unlist %>% as.factor() %>% levels
a[,'premium_package_code'] %>% pull %>% unlist %>% table
## is_super_featured
a[,'is_super_featured']
a[,'is_super_featured'] %>% table

### Include Locatlon information
## Flatten geographic location (redundant data?) are the NZTM coordinates equivalent to the WGS84?
a[,'geographic_location']
## address is the street address only e.g. 10 Smith Street
a[,'address']
## suburb_id and suburb are NOT equivalents
a[,c('suburb','suburb_id')]
checkVarsEquivalent(a,'suburb','suburb_id') ## NOT equivalent!!!!!!
## are district_id and district identical
a[,c('district','district_id')]
checkVarsEquivalent(a, 'district', 'district_id')
## region_id and region are equivalents
a[,c('region','region_id')]
checkVarsEquivalent(a,'region','region_id')

## Include property hedonics
### Hedonic property-level characteristics
## price_display
a[,'price_display']
## rateable_value
a[,'rateable_value']
## area is the floor area (should change this) 
a[,c('area')]
## land_area
a[,'land_area']
## bedrooms bathrooms integers
a[,c('bedrooms','bathrooms')]
## lounges - integer or NA
a[,'lounges']
a[,'lounges'] %>% pull %>% as.factor %>% levels
## total_parking
a[,'total_parking']
### Are category and property type similar?
## category_path and category are related
a[,c('category','category_path')]
checkVarsEquivalent(a,3,13)
## property_type 
a[,'property_type'] %>% pull %>% as.factor %>% levels()

#### Flatten 
### Sale properties
## sale_type and date_of_sale
a[,'property_sale_information']
## PropertySaleType   -> Sales type (auction, deadline etc)  -> c(None = 0, AskingPrice = 1, EnquiriesOver = 2, Auction = 3, Tender = 4, PriceByNegotiation = 5, PriceOnApplication = 6, DeadlinePrivateTreaty = 7, AskingPricePlusGst = 8)
## PropertyDateOfSale ->	DateTime 	                         -> Date of the auction/deadline/etc

#### Flatten
### Real estate agent info
## member_id seems to have something to do with the listing agent
a[,'member_id'] 
## This is all of the real estate agent info 
a[,'agency']$agency %>% names
lapply(a[,'agency']$agency$Agents, function(x) { x$FullName })
## agency_reference - not sure what this is supposed to be
a[,'agency_reference']


# getTrimmedTibble <- funciton(a, v) {
  ### Location variables
  
  ### Hedonic variables
  ## Convert price display
  ## Bed, bath, lounges
  ## Total parking
  ## Category/Property type
  
  ###
  
# }



################################################################################################################################
################################################################################################################################


### RCS style merge resulting in unique listing_ids taking the last observation in each merge
getRCS <- function(d, v) {
  a <- getS3Files(today()-d) %>% cleanAsAtDuplicates() %>% as_tibble() %>% select(all_of(v)) %>% mutate(across(as_at, convDate))
  print(nrow(a))
  for(i in 1:(d-1)) {
    b <- getS3Files(today()-d+i) %>% cleanAsAtDuplicates() %>% as_tibble() %>% select(all_of(v)) %>% mutate(across(as_at, convDate))
    a <- full_join(a, b, by = setdiff(names(a), "as_at"), suffix = c("_old", "_new")) %>%
      mutate(as_at = coalesce(as_at_new, as_at_old)) %>% # Keep the later observations
      select(setdiff(names(a), c("as_at_old", "as_at_new"))) %>% # Keep only relevant columns
      arrange(listing_id, desc(as_at)) %>% # Ensure latest year appears first
      distinct(listing_id, .keep_all = TRUE) # Remove duplicates, keeping latest entry
    print(nrow(a))
  }
  return(a)
}

v <- c('listing_id', 'as_at', 'land_area', 'area', 'bedrooms', 'bathrooms')

g <- getRCS(16, v)




