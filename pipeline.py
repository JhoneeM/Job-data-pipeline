# ============================================================
# Job Application Data Pipeline
# ============================================================
# EXTRACT  → Read raw job application data from jobs.csv
# TRANSFORM → Parse dates, calculate derived columns,
#             summarise by status, work type, salary range
# LOAD     → Save processed results to processed_jobs.csv
#
# Tech Stack: Python, Pandas
# How to run: python pipeline.py
# ============================================================

import pandas as pd
from datetime import datetime


# ── STEP 1: EXTRACT ─────────────────────────────────────────
# Read the CSV into a Pandas DataFrame.
# salary_min and salary_max are already split in the source file.
# work_type and city are already split in the source file.

df = pd.read_csv("jobs.csv")

print("=== Raw Data Loaded ===")
print(df.to_string(index=False))
print()


# ── STEP 2: TRANSFORM ───────────────────────────────────────

# -- Date parsing --
# Convert all date columns from text strings to real date objects
# so Python can perform date arithmetic on them.
df['date_applied']    = pd.to_datetime(df['date_applied'])
df['job_posted_date'] = pd.to_datetime(df['job_posted_date'])
df['date_responded']  = pd.to_datetime(df['date_responded'], errors='coerce')
# errors='coerce' turns blank/missing dates into NaT (Not a Time)
# instead of crashing — important for rows still waiting on a response.

today = datetime.today()

# -- Derived column: days_since_applied --
# How many days have passed since the application was submitted?
df['days_since_applied'] = (today - df['date_applied']).dt.days

# -- Derived column: application_speed_days --
# How quickly did the applicant apply after the job was posted?
# A lower number means the applicant was faster off the mark.
df['application_speed_days'] = (
    df['date_applied'] - df['job_posted_date']
).dt.days

# -- Derived column: response_time_days --
# How many days between submitting the application and receiving
# a response (interview invite or rejection)?
# This is only calculated where a date_responded value exists.
# Rows still awaiting a response will show NaN (not a number).
df['response_time_days'] = (
    df['date_responded'] - df['date_applied']
).dt.days

# -- Derived column: salary_mid --
# Average of salary_min and salary_max, useful for comparing
# compensation across roles without needing a full range.
df['salary_min']= pd.to_numeric(df['salary_min'], errors='coerce')
df['salary_max']= pd.to_numeric(df['salary_max'], errors='coerce')

df['salary_mid'] = (df['salary_min'] + df['salary_max']) / 2

# -- Clean status --
# Strip leading/trailing whitespace, then convert to title case
# so values like "applied", " Applied ", "APPLIED" all become "Applied".
# This MUST happen before value_counts() — if you count first and clean
# after, dirty values get counted separately and the breakdown is wrong.
df['status'] = df['status'].str.strip().str.title()

# -- Status breakdown --
# Count how many applications fall into each status category.
# value_counts() groups rows by unique values and counts each group.
# Because we cleaned status above, each category is counted correctly.
status_counts = df['status'].value_counts()

# Calculate response rate: percentage of apps that got any reply
total_apps     = len(df)
responded      = df['date_responded'].notna().sum()
response_rate  = (responded / total_apps) * 100

# -- Work type breakdown --
# How many jobs are Remote vs Hybrid vs On-site?
work_type_counts = df['work_type'].value_counts()

# -- City breakdown (only for non-remote roles) --
onsite_df    = df[df['city'].notna() & (df['city'] != '')]
city_counts  = onsite_df['city'].value_counts()

# -- Salary stats --
avg_salary_min = df['salary_min'].mean()
avg_salary_max = df['salary_max'].mean()
avg_salary_mid = df['salary_mid'].mean()

# -- Response time stats (Interview / Rejected rows only) --
responded_df  = df[df['response_time_days'].notna()]
avg_response  = responded_df['response_time_days'].mean()
avg_speed     = df['application_speed_days'].mean()


# ── Print Summary ────────────────────────────────────────────
print("=== Pipeline Summary ===")
print(f"Total Applications         : {total_apps}")
print(f"Total Responses Received   : {int(responded)} ({response_rate:.0f}% response rate)")
print(f"Avg Days to Apply          : {avg_speed:.1f} days after job posted")
print(f"Avg Response Time          : {avg_response:.1f} days (Interview/Rejected only)")
print(f"Avg Salary Min             : ${avg_salary_min:,.0f}")
print(f"Avg Salary Max             : ${avg_salary_max:,.0f}")
print(f"Avg Salary Midpoint        : ${avg_salary_mid:,.0f}")
print()

print("--- Applications by Status ---")
print(status_counts.to_string())
print()

print("--- Applications by Work Type ---")
print(work_type_counts.to_string())
print()

if not city_counts.empty:
    print("--- On-site / Hybrid Cities ---")
    print(city_counts.to_string())
    print()

print("=== Full Processed Data ===")
print(df[[
    'job_id', 'company', 'role', 'status',
    'days_since_applied', 'application_speed_days', 'response_time_days',
    'salary_min', 'salary_max', 'salary_mid',
    'work_type', 'city', 'notes'
]].to_string(index=False))
print()


# ── STEP 3: LOAD ────────────────────────────────────────────
# Write the enriched DataFrame to a new CSV file.
# index=False prevents Pandas from adding an extra row-number column.

df.to_csv("processed_jobs.csv", index=False)

print("✓ Processed data saved to processed_jobs.csv")
