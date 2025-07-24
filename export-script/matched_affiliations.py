import os
import pandas as pd

# --- Step 1: Load the file ---
downloads_path = os.path.join(os.path.expanduser("~"), "Downloads")
file_path = os.path.join(downloads_path, "ror_id_candidate_matches_merged_local.csv")

df = pd.read_csv(
    file_path,
    sep=",",
    quotechar='"',
    skipinitialspace=True,
    dtype=str,
    keep_default_na=False,
    na_values=[""],
    index_col=False  # ⛔ Ensures 'Dataset' doesn't become the index
)
df.columns = df.columns.str.strip()

# --- Step 2: Helper functions ---
def count_true_matches(row):
    return sum(
        str(row.get(f"Match {i} Chosen", "")).strip().upper() == "TRUE"
        for i in range(1, 4)
    )

def is_all_false(row):
    return all(
        str(row.get(f"Match {i} Chosen", "")).strip().upper() == "FALSE"
        for i in range(1, 4)
    )

# --- Step 3: Apply ---
df["num_true"] = df.apply(count_true_matches, axis=1)
df["all_false"] = df.apply(is_all_false, axis=1)

# --- Step 4: Filter ---
exactly_one_true = df[df["num_true"] == 1]
all_false = df[df["all_false"] == True]
more_than_one_true = df[df["num_true"] > 1]

# --- Step 5: Export ---
exactly_one_true.to_csv("exactly_one_true_affiliations.csv", index=False)
all_false.to_csv("all_false_affiliations.csv", index=False)
more_than_one_true.to_csv("more_than_one_true_affiliations.csv", index=False)

# --- Step 6: Summary ---
print(f"📄 Total rows: {len(df)}")
print(f"✅ Exactly ONE TRUE rows: {len(exactly_one_true)}")
print(f"❌ All FALSE rows: {len(all_false)}")
print(f"⚠️ More than ONE TRUE rows: {len(more_than_one_true)}")
