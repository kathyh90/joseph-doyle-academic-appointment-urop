import pandas as pd

# INPUT FILEPATHS
original_csv = "/home/kathyh90/joe-doyle-urop-2025/iap_2026_code/results/cornell_only_affiliations.csv"
comparison_csv = "/home/kathyh90/joe-doyle-urop-2025/iap_2026_code/results/cornell_author_profiles_extended_f.csv"
# error_csv = "/home/kathyh90/joe-doyle-urop-2025/CORNELL_errors.csv"
output_csv = "/home/kathyh90/joe-doyle-urop-2025/iap_2026_code/results/cornell_author_remaining.csv"

# all_authors = pd.read_csv(original_csv)
num_all_authors = len(all_authors['name'])

queried_authors = pd.read_csv(comparison_csv)
num_queried_authors = len(queried_authors['name'])

# error_authors = pd.read_csv(error_csv)
# num_error_authors = len(error_authors['name'])

all_authors['oid'] = all_authors['author_id'].apply(lambda x: str(x).rsplit("/", 1)[-1])
queried_authors['oid'] = queried_authors['author_id'].apply(lambda x: str(x).rsplit("/", 1)[-1])
# error_authors['oid'] = error_authors['author_id'].apply(lambda x: str(x).rsplit("/", 1)[-1])

# ~ is the NOT operator
missing_authors = all_authors[~all_authors['oid'].isin(queried_authors['oid'])]

# missing_authors = missing_authors[~missing_authors['oid'].isin(error_authors['oid'])]

missing_authors = missing_authors.drop(columns=['oid'])

missing_authors.to_csv(output_csv, index=False)

# –– COMPARING DIFFERENCES BY NAME –––––––––––––––––––––––––––––––––––––––––––––––
# duplicate profiles made less authors appear
# missing_authors = all_authors[~all_authors['name'].isin(queried_authors['name'])]

# missing_authors.to_csv(output_csv, index=False)

# –– COMPARING DIFFERENCES BY FORMATTED NAME –––––––––––––––––––––––––––––––––––––––––––––––
# authors missing after formatting but not using because due to different OpenAlex profiles
# all_authors['name_clean'] = all_authors['name'].str.strip().str.lower()
# queried_authors['name_clean'] = queried_authors['name'].str.strip().str.lower()

# missing_authors_clean = all_authors[~all_authors['name_clean'].isin(queried_authors['name_clean'])]
# print(f"Number of resulting clean authors missing: {len(missing_authors_clean)}")

# print(f"Number of needed authors missing: {num_all_authors-num_queried_authors} and total errors from before: {num_error_authors}")
# print(f"Number of resulting authors missing: {len(missing_authors)}")

# –– COMBINE DATAFRAMES TOGETHER –––––––––––––––––––––––––––––––––––––––––––––––
import pandas as pd

input1 = "/home/kathyh90/joe-doyle-urop-2025/iap_2026_code/results/cornell_author_profiles_extended_f.csv"
input2 = "/home/kathyh90/joe-doyle-urop-2025/iap_2026_code/results/add_cornell_author_profiles_extended_f.csv"
output_csv = "/home/kathyh90/joe-doyle-urop-2025/iap_2026_code/results/cornell_author_profiles_extended_f.csv"

df1 = pd.read_csv(input1)
df2 = pd.read_csv(input2)

df1 = pd.concat([df1, df2], ignore_index=True)

df1.to_csv(output_csv, index=False)
