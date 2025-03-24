################################################################################################################################

### Short term unbalanced 'panel' format
## Merge each new day to the previous set
d <- a %>% select(listing_id, as_at, title) %>% mutate(across(as_at, convDate))
e <- b %>% select(listing_id, as_at, title) %>% mutate(across(as_at, convDate))

f <- full_join(d, e, by = c('listing_id', 'title'))
# The listings seem to be changing day to day. To be expected I suppose
lapply(1:length(which(duplicated(f$listing_id))), function(i) { f[which(f$listing_id == f$listing_id[which(duplicated(f$listing_id))[i]]),] })


f <- full_join(d, e, by = c('listing_id'))

g <- f %>%
  pivot_longer(cols = starts_with("as_at"),  # Select columns to pivot
               names_to = "time_period",     # New column for original variable names
               values_to = "as_at")          # New column for values
