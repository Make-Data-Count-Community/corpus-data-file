import os
import pandas as pd

# Step 1: Load the file
downloads_path = os.path.join(os.path.expanduser("~"), "Downloads")
file_path = os.path.join(downloads_path, "ror_id_candidate_matches_merged_local.csv")

df = pd.read_csv(file_path)

# Helper functions
def is_true(val):
    return str(val).strip().upper() == 'TRUE'

def is_false(val):
    return str(val).strip().upper() == 'FALSE'

# Step 2: Create masks
all_true_mask = df.apply(lambda row: all(is_true(row.get(f'Match {i} Chosen', '')) for i in range(1, 4)), axis=1)
all_false_mask = df.apply(lambda row: all(is_false(row.get(f'Match {i} Chosen', '')) for i in range(1, 4)), axis=1)

# Step 3: Split into categories
all_true = df[all_true_mask]
all_false = df[all_false_mask]
not_all_true = df[~all_true_mask]  # Anything that's not all true (includes all_false + partial_true)

# Step 4: Export
all_true.to_csv('all_true_affiliations.csv', index=False)
all_false.to_csv('all_false_affiliations.csv', index=False)
not_all_true.to_csv('not_all_true_affiliations.csv', index=False)

# Step 5: Summary
print(f"📄 Total rows: {len(df)}")
print(f"✅ All TRUE rows: {len(all_true)}")
print(f"❌ All FALSE rows: {len(all_false)}")
print(f"🤷 Not ALL TRUE rows: {len(not_all_true)}")
