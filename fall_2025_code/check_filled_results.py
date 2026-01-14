import pandas as pd

# ── CONFIG ────────────────────────────────────────────────────────────────
INPUT_CSV         = "/home/kathyh90/joseph-doyle-academic-appointment-urop/results/extended_results.csv"
OUTPUT_CSV        = "/home/kathyh90/joseph-doyle-academic-appointment-urop/results/filled_death_dates_results.csv"
NAME_COL          = "name"       # name column
ORCID_COL         = "orcid"      # ORCID ID column
BIRTH_DATE_COL    = "date_of_birth"
DEATH_DATE_COL    = "date_of_death"
AFFILIATION_COL   = "affiliation"
Q_ID_COL          = "q_id"

df = pd.read_csv(INPUT_CSV, dtype=str)
df.columns = df.columns.str.strip()

print(df.head(10))

# count for death_date specifically
death_date_counts = df[DEATH_DATE_COL].count()
print(death_date_counts)

# create dataframe with only filled death dates academics
df_death_date = df[df[DEATH_DATE_COL].notna()]

df_death_date.to_csv(OUTPUT_CSV, index=False)
print(f"\n✓ Done! Wrote {len(df_death_date)} rows to '{OUTPUT_CSV}'.")

# general count for result columns
result_columns = [ORCID_COL, BIRTH_DATE_COL, DEATH_DATE_COL, AFFILIATION_COL, Q_ID_COL]

counts = {col: 0 for col in result_columns}

for col in counts.keys():
    counts[col] = int(df[col].count())

print(counts)

# create dataframe with multiple filled results columns: death date and affiliation
specified_columns = [DEATH_DATE_COL, AFFILIATION_COL]
death_affiliation_results = df[df[specified_columns].notna().all(axis=1)]

print(death_affiliation_results.head(10))

death_affiliation_output_csv = "/home/kathyh90/joseph-doyle-academic-appointment-urop/results/death_and_affiliation_results.csv"
death_affiliation_results.to_csv(death_affiliation_output_csv, index=False)
print(f"\n✓ Done! Wrote {len(death_affiliation_results)} rows to '{death_affiliation_output_csv}'.")
