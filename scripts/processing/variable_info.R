
################################################################################################################################
## Convert date variables
# c('start_date', 'end_date', 'as_at')
# a$property_sale_information$PropertyDateOfSale - check how this relates to the other dates


################################################################################################################################
################################################################################################################################


################################################################################################################################
### Useless for now/don't know what it means
## it appears that start_price is not used
a[,'start_price'] %>% pull %>% is.na() %>% any
a[,'start_price'] %>% pull %>% mean
## it appears that listing_length is never used
a[,'listing_length'] %>% pull %>% is.na() %>% all
## note_date probably useless - check that all in the cross section are == "/Date(0)/"
all(a[,'note_date'] == "/Date(0)/") # should return TRUE if all == /Date(0)/
## reserve_state seems not to be used and is always set to '3'
all(pull(a[,'reserve_state']) == 3)
## is_classified seems to always be true
a[,'is_classified'] %>% all
## check if all additional_dat$BulletPoints are lists of zero length (and drop)
all(sapply(a[,'additional_data']$additional_data$BulletPoints, function(x) { is.list(x) & length(x) == 0 }))
## and the same with additional_data$Tags
all(sapply(a[,'additional_data']$additional_data$Tags, function(x) { is.list(x) & length(x) == 0 }))
## listing_group seems to have a single factor level == 'PROPERTY' might differ if searching another category e.g. rental
a[,c('listing_group')] %>% pull %>% as.factor %>% levels()
## listing platform appears to only have a single factor level '1'
a[,'listing_platform']
## property_id - mostly NA and I am unfamiliar with the reference system
a[,'property_id'] %>% na.omit()
## is_new - not sure what it means
a[,c('start_date','end_date','as_at','is_new')] %>% na.omit()
cbind(lapply(a[,c('start_date','end_date','as_at')], convDate) %>% as_tibble(), a[,'is_new']) %>% na.omit()


################################################################################################################################
### Too hard basket/NLP - could also look at this in the context of flairs or in relation to the open homes variable
## amenities - mostly NA but open text field will need NLP - should ignore this and calculate based on location
a[,'amenities'] %>% na.omit()
## best_contact_time - mostly NA - but clean factor
a[,'best_contact_time'] %>% na.omit()
a[,'best_contact_time'] %>% na.omit() %>% pull() %>% as.factor() %>% levels()
## viewing_instructions open text field
a[,'viewing_instructions'] %>% na.omit()
## parking is an open text field likely needs NLP - this seems tractable though
a[,'parking'] %>% pull


################################################################################################################################
## open_homes are either a single df or a list element, 
## if df: may be empty (0 elements) or may have elements (rows) equal to the number of booked open homes
## open homes are recorded as Start and End values
## if list elements they seem to be empty
# vector length of number of open homes per dataframe is not equal to the number of rows
sapply(pull(a[,'open_homes']), function(x) { nrow(x) }) %>% unlist %>% length == nrow(a)
# because of the list elements
tmp <- which(sapply(pull(a[,22]), function(x) { !is.data.frame(x) }))
# which are all 0
all(sapply(pull(a[tmp,22]), function(x) { length(x) }) == 0)

# Draft/ChatGPT function to compute open home score -- need to revisit and calibrate
# Note that this will probably have to make some adjustment for the 'select last observation' behavior when stitching the cross sections
compute_open_home_score <- function(df) {
  if (nrow(df) == 0) return(0)  # Handle properties with no open homes
  n_open_homes <- nrow(df)  # Count total open homes
  if (n_open_homes == 1) return(n_open_homes * 10)  # If only one, score based on count
  # Compute average frequency (gap between open homes in days)
  avg_gap_days <- mean(diff(sort(df$start)), na.rm = TRUE)
  # Scoring formula: More open homes + better frequency (shorter gaps)
  score <- (n_open_homes * 10) + (100 / (1 + avg_gap_days)) # Avoid division by zero
  return(score)
}

# Apply scoring function to each property
property_scores <- property_data %>%
  mutate(score = map_dbl(open_homes, compute_open_home_score))

################################################################################################################################
### Photo/video related
## picture_href
a[,'picture_href']
## photo_urls
# Character lists of urls. Could take the length maybe number of photos has some effect?
a[,'photo_urls']
## has_embedded-video
a[,'has_embedded_video']
## has_3_d_tour
a[,'has_3_d_tour']

################################################################################################################################
### Trademe flairs
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

################################################################################################################################
### Location information
## flatten geographic location (redundant data?) are the NZTM coordinates equivalent to the WGS84?
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

################################################################################################################################
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

################################################################################################################################
### Sale properties
## sale_type and date_of_sale
a[,'property_sale_information']
## PropertySaleType   -> Sales type (auction, deadline etc)  -> c(None = 0, AskingPrice = 1, EnquiriesOver = 2, Auction = 3, Tender = 4, PriceByNegotiation = 5, PriceOnApplication = 6, DeadlinePrivateTreaty = 7, AskingPricePlusGst = 8)
## PropertyDateOfSale ->	DateTime 	                         -> Date of the auction/deadline/etc

################################################################################################################################
### Real estate agent info
## member_id seems to have something to do with the listing agent
a[,'member_id'] 
## This is all of the real estate agent info 
a[,'agency']$agency %>% names
lapply(a[,'agency']$agency$Agents, function(x) { x$FullName })
## agency_reference - not sure what this is supposed to be
a[,'agency_reference']

################################################################################################################################
## Price variables
a %>% getPriceInteger() %>% log() %>% hist
a %>% getPriceCategory()

## Need some ideas for filtering houses for removal at the low end
## And other strange things like people putting 320 instead of 320000 (e.g. 'Offers over 320')
df <- a
df$price_integer <- df %>% getPriceInteger()
df %>% filter(!is.na(price_integer)) %>% select(price_integer, price_display, land_area, title) %>% arrange(price_integer) %>% head(100)
df %>% filter(!is.na(price_integer)) %>% arrange(price_integer) %>% head(100)
df$price_integer[!is.na(df$price_integer)] %>% quantile(probs = seq(0, 1, by = 0.1))

## Check that price category is the same as property_sale_information$PropertySaleType
df$price_category <- df %>% getPriceCategory()
df$test <- df$property_sale_information$PropertySaleType
checkVarsEquivalent(df, 'price_category', 'test')
# It is!!