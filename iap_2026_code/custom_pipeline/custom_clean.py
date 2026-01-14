#Purpose of this file is to run modify the data to make it less messy and then run an audit to see how it
#compares to an audit of the data when we applied the first attempt at cleaning it. This is done in check.py.
# an audit of the data we get to check for any issues. 
#First load files for all instutitions and append them
import pandas as pd
import gc
import numpy as np
from math import radians, sin, cos, sqrt, atan2

def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calculate the haversine distance between two points on Earth.
    Returns distance in miles.
    """
    # Convert to radians
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    
    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    
    # Earth's radius in miles
    r = 3959
    return r * c


def remove_nearby_perfect_overlaps(df, lat_col='hospital_lat', lon_col='hospital_lon', 
                                   inst_col='institution_name',
                                   prof_col='author_id', start_col='year_start', 
                                   end_col='year_end', distance_threshold_miles=3):
    """
    For perfect overlaps (same author, same start/end dates), remove duplicates using:
    1. Drop entries with missing lat/lon first
    2. Drop entries without "university" in institution name
    3. For remaining pairs within distance threshold, randomly drop one
    
    Parameters:
    -----------
    df : DataFrame with perfect overlap flags and lat/lon columns
    lat_col, lon_col : column names for latitude and longitude
    inst_col : column name for institution/university name
    prof_col : professor/author identifier column
    start_col, end_col : date columns
    distance_threshold_miles : distance threshold for considering duplicates (default 3)
    """
    df = df.copy()
    
    # Filter for perfect overlaps only
    perfect_overlaps = df[df['perfect_overlap'] == 1].copy()
    
    if len(perfect_overlaps) == 0:
        print("No perfect overlaps found")
        return df
    
    # Track indices to drop
    indices_to_drop = set()
    
    # Group by author, start date, and end date
    grouped = perfect_overlaps.groupby([prof_col, start_col, end_col])
    
    for (author, start, end), group in grouped:
        if len(group) < 2:
            continue  # No duplicates in this group
        
        indices = group.index.tolist()
        
        # Compare all pairs within this group
        for i in range(len(indices)):
            if indices[i] in indices_to_drop:
                continue
            
            for j in range(i + 1, len(indices)):
                if indices[j] in indices_to_drop:
                    continue
                
                # Get coordinates and institution names
                lat1 = df.loc[indices[i], lat_col]
                lon1 = df.loc[indices[i], lon_col]
                lat2 = df.loc[indices[j], lat_col]
                lon2 = df.loc[indices[j], lon_col]
                inst1 = str(df.loc[indices[i], inst_col]).lower()
                inst2 = str(df.loc[indices[j], inst_col]).lower()
                
                # Step 1: Check for missing coordinates
                has_coords_i = not (pd.isna(lat1) or pd.isna(lon1))
                has_coords_j = not (pd.isna(lat2) or pd.isna(lon2))
                
                if not has_coords_i and has_coords_j:
                    # i is missing coords, drop i
                    indices_to_drop.add(indices[i])
                    break  # Move to next i
                elif has_coords_i and not has_coords_j:
                    # j is missing coords, drop j
                    indices_to_drop.add(indices[j])
                    continue  # Check next j
                elif not has_coords_i and not has_coords_j:
                    # Both missing coords, can't compare - skip
                    continue
                
                # Step 2: Check for "university" in institution name
                has_univ_i = 'university' in inst1
                has_univ_j = 'university' in inst2
                
                if has_univ_i and not has_univ_j:
                    # i has university, j doesn't - drop j
                    indices_to_drop.add(indices[j])
                    continue  # Check next j
                elif not has_univ_i and has_univ_j:
                    # j has university, i doesn't - drop i
                    indices_to_drop.add(indices[i])
                    break  # Move to next i
                
                # Step 3: Calculate distance and randomly drop if within threshold
                distance = haversine_distance(lat1, lon1, lat2, lon2)
                
                if distance < distance_threshold_miles:
                    # Randomly choose which to drop
                    to_drop = np.random.choice([indices[i], indices[j]])
                    indices_to_drop.add(to_drop)
                    
                    if to_drop == indices[i]:
                        break  # Move to next i
    
    # Remove the identified duplicates
    result = df.drop(index=list(indices_to_drop))
    
    print(f"Removed {len(indices_to_drop)} perfect overlaps using hierarchical logic")
    print(f"  - Geographic proximity ({distance_threshold_miles} miles)")
    
    return result.reset_index(drop=True)

#Define a function to handle overlapping affiliations
def clean_overlapping_affiliations(df, prof_col='author_id', start_col='year_start', 
                                   end_col='year_end', overlap_threshold_years=4):
    # Ensure dates are datetime
    df = df.copy()
    
    # Track indices to drop
    indices_to_drop = set()
    
    # Group by professor
    for prof, group in df.groupby(prof_col):
        group = group.sort_values(start_col)
        indices = group.index.tolist()
        
        # Compare all pairs of affiliations for this professor
        for i in range(len(indices)):
            if indices[i] in indices_to_drop:
                continue
                
            for j in range(i + 1, len(indices)):
                if indices[j] in indices_to_drop:
                    continue
                
                row_i = df.loc[indices[i]]
                row_j = df.loc[indices[j]]
                
                start_i, end_i = row_i[start_col], row_i[end_col]
                start_j, end_j = row_j[start_col], row_j[end_col]
                dur_i, dur_j = row_i['appoint_len'], row_j['appoint_len']
                
                # Check if there's any overlap
                overlap_start = max(start_i, start_j)
                overlap_end = min(end_i, end_j)
                
                if overlap_start >= overlap_end:
                    # No overlap
                    continue
                
                # Calculate overlap duration
                overlap_years = overlap_end - overlap_start
                
                # Rule 1: Perfect overlap (same start and end)
                if start_i == start_j and end_i == end_j:
                    # Keep both, no action
                    df.loc[indices[i], 'perfect_overlap'] = 1
                    df.loc[indices[j], 'perfect_overlap'] = 1
                    continue
                
                # Rule 2: One is a subset of the other
                i_subset_of_j = (start_i >= start_j and end_i <= end_j)
                j_subset_of_i = (start_j >= start_i and end_j <= end_i)
                
                if i_subset_of_j:
                    # i is contained in j, drop the shorter one (i)
                    indices_to_drop.add(indices[i])
                    break  # Move to next i
                elif j_subset_of_i:
                    # j is contained in i, drop the shorter one (j)
                    indices_to_drop.add(indices[j])
                    continue  # Check next j
                
                # Rule 3: Partial overlap
                # Check if overlap exceeds threshold
                if overlap_years > overlap_threshold_years:
                    # Drop the shorter affiliation
                    if dur_i < dur_j:
                        indices_to_drop.add(indices[i])
                        break  # Move to next i
                    else:
                        indices_to_drop.add(indices[j])
                        continue  # Check next j
                else:
                    #Mark overlap
                    df.loc[indices[i], 'partial_overlap_bt'] = 1
                    df.loc[indices[j], 'partial_overlap_bt'] = 1
    
    # Remove the temporary duration column and return cleaned dataframe
    result = df.drop(index=list(indices_to_drop))    
    return result.reset_index(drop=True)

#Load data
pd.set_option('display.width', 2000)
pd.set_option('display.max_columns', 50)
# Load and append files
comb_df = pd.read_csv('/home/mm4958/openalex/custom_pipeline/results/with_death_dates_custom_death_adjusted.csv')
# -------------------------
# Initial stats
# -------------------------
#Drop duplicate affiliations in terms of start and end date and author
unique_affiliation = comb_df.drop_duplicates(
    subset=['institution_id','author_id','year_start','year_end']
).copy()

# Add appointment length
unique_affiliation['appoint_len'] = (
    unique_affiliation['year_end'] - unique_affiliation['year_start']
)
# Get initial size of dataset and number of unique authors
obs0 = unique_affiliation.shape[0]
authors0 = unique_affiliation['author_id'].nunique()

print(f"\nInitial number of observations: {obs0}")
print(f"Initial number of authors: {authors0}")
print(unique_affiliation.head(50))
# -------------------------
# Affiliation 
# -------------------------
#remove authors with less than 6 years consecutive spells at some university
short_spells = unique_affiliation['appoint_len'] < 6
#Filter dataset to include only appointment lengths longer than 5 years.
unique_affiliation = unique_affiliation[~short_spells]
authors1 = unique_affiliation['author_id'].nunique()
obs1 = unique_affiliation.shape[0]
print(f"\nDropping appointments <6 years:")
print(f"  Authors dropped: {authors0-authors1} ({(authors0-authors1)/authors0:.2%})")
print(f"  Observations dropped: {obs0-obs1} ({(obs0-obs1)/obs0:.2%})")
print(unique_affiliation.head(50))

# Stage B – remove authors with >50-year spell
authors_over_50 = unique_affiliation.loc[
    unique_affiliation['appoint_len'] > 50, 'author_id'
].unique()

obs_over50 = unique_affiliation[
    unique_affiliation['author_id'].isin(authors_over_50)
].shape[0]

#Filter dataset to remove authors with appointment lengths longer than 50 years.
unique_affiliation = unique_affiliation[
    ~unique_affiliation['author_id'].isin(authors_over_50)
]
obs2 = unique_affiliation.shape[0]
authors2 = unique_affiliation['author_id'].nunique()
print(f"\nDropping authors with >50-year appointments:")
print(f"  Authors dropped: {authors1-authors2} ({(authors1-authors2)/authors1:.2%})")
print(f"  Observations dropped: {obs1-obs2} ({(obs1-obs2)/obs1:.2%})")
print(unique_affiliation.head(50))

#generate column to keep track of perfect overlap
unique_affiliation['perfect_overlap'] = 0
unique_affiliation['partial_overlap_bt'] = 0
# Now clean overlaps
df_no_overlap = clean_overlapping_affiliations(unique_affiliation)
df_no_overlap = clean_overlapping_affiliations(unique_affiliation)
obs3 = df_no_overlap.shape[0]
authors3 = df_no_overlap['author_id'].nunique()
print("Dropping appointments that overlap by > 6 years (drop the shorter one)")
print(f"  Authors dropped: {authors2-authors3} ({(authors2-authors3)/authors2:.2%})")
print(f"  Observations dropped: {obs2-obs3} ({(obs2-obs3)/obs2:.2%})")
perfect_overlap_1 = df_no_overlap['perfect_overlap'].sum()
partial_overlap_bt_n = df_no_overlap['partial_overlap_bt'].sum()
print(f"Number of perfect overlaps: {perfect_overlap_1}")
print(f"Number of partial overlaps that did not meet threshold of 4 years: {partial_overlap_bt_n}")
#download perfect overlaps and partial overlaps to a csv
remaining_overlap = df_no_overlap[
    (df_no_overlap['perfect_overlap'] == 1) | 
    (df_no_overlap['partial_overlap_bt'] == 1)
]
remaining_overlap.to_csv('/home/mm4958/openalex/perfect_overlaps.csv', index=False)
# -------------------------
# Clean perfect overlaps pairs by (1) Dropping the one with misssing coordinates 
#(2) Removing the one without word "university" in them
#(3) Randomly removing one if one is less than 3 miles from the other
# -------------------------
df_affil_clean = remove_nearby_perfect_overlaps(df_no_overlap)
obs4 = df_affil_clean.shape[0]
authors4 = df_affil_clean['author_id'].nunique()
perfect_overlap_2 = df_affil_clean['perfect_overlap'].sum()
print("Dropping perfect overlaps according to logic above")
print(f"  Authors dropped: {authors3-authors4} ({(authors3-authors4)/authors3:.2%})")
print(f"  Observations dropped: {obs3-obs4} ({(obs3-obs4)/obs3:.2%})")
print(f"Number of perfect overlaps remaining: {perfect_overlap_2} so removed {perfect_overlap_1}-{perfect_overlap_2}, so ({(perfect_overlap_1-perfect_overlap_2)/perfect_overlap_1:.2%})")
print(df_affil_clean.head(50))
# -------------------------
# Now work with death/birth dates
# -------------------------
#Convert variables to date-time objects in years
df_affil_clean['date_of_birth'] = pd.to_datetime(df_affil_clean['date_of_birth'], errors='coerce').dt.year
df_affil_clean['date_of_death'] = pd.to_datetime(df_affil_clean['date_of_death'], errors='coerce').dt.year
# Keep year_start and year_end as-is if they're already integers
df_affil_clean['year_start'] = pd.to_numeric(df_affil_clean['year_start'], errors='coerce')
df_affil_clean['year_end'] = pd.to_numeric(df_affil_clean['year_end'], errors='coerce')
#Create following new vars, lifespan, time between birth and first pub, time between death andlast pub
df_affil_clean['lifespan'] = df_affil_clean['date_of_death'] - df_affil_clean['date_of_birth']
earliest_start = df_affil_clean.groupby('author_id')['year_start'].min().reset_index()
earliest_start = earliest_start.rename(columns={'year_start':'earliest_start'})
latest_end = df_affil_clean.groupby('author_id')['year_end'].max().reset_index()
latest_end = latest_end.rename(columns={'year_end':'latest_end'})
df_merged = pd.merge(df_affil_clean, earliest_start, on='author_id')
df_merged = pd.merge(df_merged, latest_end, on='author_id')
#Create variables measuring time from birth to first pub and time from last pub to death
#Then adjust the latter to discard cases posthomous publications
df_merged['time_to_first_pub'] = (
    df_merged['earliest_start'] - df_merged['date_of_birth']
)
df_merged['latest_end_adj'] = df_merged[['latest_end', 'date_of_death']].min(axis=1)
# Recompute time_from_last_pub using adjusted latest_end
df_merged['time_from_last_pub'] = df_merged['date_of_death'] - df_merged['latest_end_adj']

# -------------------------
# Do some final filtering based on birth and death dates
# -------------------------
#First get some stats for later
lifespan_0 = (df_merged['lifespan']<=15).sum()
pub_b4_birth = (df_merged['time_to_first_pub']<0).sum()
author_b4_birth = df_merged[df_merged['time_to_first_pub'] < 0]['author_id'].nunique()
pub_50_birth = (df_merged['time_to_first_pub']>50).sum()
author_50_birth = df_merged[df_merged['time_to_first_pub'] > 50]['author_id'].nunique()
# Identify authors who published less than 15 years after birth
authors_to_drop_early = df_merged.loc[df_merged['time_to_first_pub'] <= 15, 'author_id'].unique()
# Identify authors who published > 50 years after birth
authors_to_drop_late = df_merged.loc[df_merged['time_to_first_pub'] > 50, 'author_id'].unique()
# Combine them into one list
all_authors_to_drop = np.union1d(authors_to_drop_early, authors_to_drop_late)
#Drop them
df_merged = df_merged[~df_merged['author_id'].isin(all_authors_to_drop)]
#Remove the case of the 1 author with a lifespan of 0
df_merged = df_merged[(df_merged['lifespan'] > 0) | (df_merged['lifespan'].isna())]
obs_final = df_merged.shape[0]
authors_final = df_merged['author_id'].nunique()
print("\nAfter filtering authors with first pub less than 15 years after birth or more than 50 years after birth and non-zero lifespans:")
print(f" Observations dropped: {obs4-obs_final}  (from {obs4}) ({(obs4-obs_final)/obs4:.2%})")
print(f" Authors dropped: {authors4-authors_final}  (from {authors4}) ({(authors4-authors_final)/authors4:.2%})")
print(f" Observations remaining: {obs_final}  (from {obs0}) ({(obs_final)/obs0:.2%})")
print(f" Authors remaining: {authors_final}  (from {authors0}) ({(authors_final)/authors0:.2%})")
print(df_merged.head(50))
# -------------------------
# Print summary stats
# -------------------------
count_death_date = df_merged[df_merged['date_of_death'].notna() & (df_merged['date_of_death'] != '')]['author_id'].nunique()
count_birth_date = df_merged[df_merged['date_of_birth'].notna() & (df_merged['date_of_birth'] != '')]['author_id'].nunique()
count_birth_death_date = df_merged[
    (df_merged['date_of_death'].notna()) & (df_merged['date_of_death'] != '') & 
    (df_merged['date_of_birth'].notna()) & (df_merged['date_of_birth'] != '')
]['author_id'].nunique()
summary_lifespan = df_merged[['time_to_first_pub', 'time_from_last_pub', 'lifespan', 'date_of_birth', 'date_of_death','earliest_start', 'latest_end', 'appoint_len', "total_works", "total_citations"]].describe(percentiles=[0.25, 0.5, 0.75]).round(2)
print('Summary stats for lifespan variables below:\n', summary_lifespan)
print(f"Non-missing death dates (1 / author): {count_death_date} ({count_death_date/authors_final:.2%})")
print(f"Non-missing birth dates (1 / author): {count_birth_date} ({count_birth_date/authors_final:.2%})")
print(f"Non-missing birth & death dates (1 / author): {count_birth_death_date} ({count_birth_death_date/authors_final:.2%})")
#print(f"Number of observations 15 or less years before earliest start: {pub_b4_birth} ({pub_b4_birth/obs_final:.2%})")
#print(f"Number of authors born 15 or less years before earliest start: {author_b4_birth} ({author_b4_birth/authors_final:.2%})")
#print(f"Number of observations publishing more than 50 years after earliest start: {pub_50_birth} ({pub_50_birth/obs_final:.2%})")
#print(f"Number of authors publishing more than 50 years after earliest start: {author_50_birth} ({author_50_birth/authors_final:.2%})")
#print(f"Dropped this many observations when filtering to only positive lifespans:{lifespan_0}")
# -------------------------
# Additional summary stats
# -------------------------
appointments_per_author = (
    df_merged.groupby('author_id')
    .size()
    .reset_index(name='appointments_per_author')
)

summary_appointments_by_author = appointments_per_author.describe(
    percentiles=[0.25, 0.5, 0.75]
).round(2)

print('\nSummary stats for number of appointments per author below:\n', summary_appointments_by_author)
df_merged.to_csv('/home/mm4958/openalex/custom_pipeline/results/df_cleaned_final_custom.csv', index=False)
