# Fynd COD payout: a Databricks Asset Bundle you can run on Free Edition

Every morning this job pays brands for COD bags that were delivered:

```
pick delivered COD bags  →  work out each brand's payout  →  write payout Excel + ledger  →  refresh bag journey table
   (skips bags already paid)    (holds bags that don't add up)     (ledger = "this bag is paid")      (feeds the dashboard)
```

Everything about **when** it runs, **in what order**, and **who hears if it fails** lives in
`databricks.yml` and `resources/`. The actual maths lives in the notebooks under `src/`.
All data is synthetic, so it is safe on Free Edition (which is for non-commercial use).

## What's in the repo

| File | What it does |
| --- | --- |
| `databricks.yml` | The bundle: its name, variables, and the `dev` / `prod` targets |
| `resources/storage.yml` | Creates the schema and the `payout_files` volume |
| `resources/cod_brand_payout.job.yml` | The job: 07:00 IST schedule, 4 tasks in order, failure email |
| `resources/setup.job.yml` | One-off job that loads 240 fake bags (and resets the ledger) |
| `src/setup/00_load_sample_data.py` | Builds the fake `bags` table (same columns as `avis_base_view`) |
| `src/cod_payout/01…04_*.py` | The four steps of the payout |
| `.github/workflows/deploy.yml` | PR → deploy to dev; merge to main → deploy to prod |

### How this maps to the video

The video uses the 2023 preview syntax. Same ideas, new names:

| In the video | Here | Why |
| --- | --- | --- |
| `bundle.yml` | `databricks.yml` | File was renamed |
| `environments:` | `targets:` | Renamed; `development` / `production` are now `dev` / `prod` with `mode:` |
| `${bundle.environment}` | `${bundle.target}` | Renamed |
| `new_cluster:` block | *(none)* | Free Edition is serverless only, so no cluster is defined |
| `schedule:` only in production | `schedule:` on the job, `mode: development` pauses it in dev | Same effect, less copy-paste |

## Run it (about 15 minutes)

### 1. Install the CLI and log in

```bash
# macOS
brew tap databricks/tap && brew install databricks
# Windows / Linux: https://docs.databricks.com/dev-tools/cli/install

databricks auth login --host https://<your-workspace>.cloud.databricks.com
```

Your workspace URL is in the browser's address bar when you are logged in to Free Edition.

### 2. Deploy to dev

```bash
databricks bundle validate     # checks the YAML
databricks bundle deploy       # dev is the default target
```

In the workspace you'll now see, under **Jobs & Pipelines**, two jobs prefixed with `[dev your_name]`,
and under **Catalog › workspace** a schema named `dev_<your_name>_finops_dev`.
The schedule is paused because this is dev.

### 3. Load the fake bags, then run the payout

```bash
databricks bundle run setup_sample_data
databricks bundle run cod_brand_payout
```

Or press **Run now** on each job in the UI. Watch the four tasks go green one after another.

### 4. Look at what it produced

Open a SQL editor or notebook on the dev schema:

```sql
SELECT qa_flag, COUNT(*) FROM brand_payable_bag GROUP BY qa_flag;   -- what got paid vs held
SELECT * FROM brand_payable_summary ORDER BY brand_id;              -- one row per brand
SELECT payout_stage, COUNT(*) FROM bag_journey GROUP BY 1;           -- the dashboard table
```

The fake data has four bags planted on purpose: three where the cash collected doesn't match
(`CUSTOMER_MATH_MISMATCH`) and one with no GST rate (`MISSING_GST_RATE`). They show up as
**On hold**, not paid. The payout Excel is in **Catalog › your schema › Volumes › payout_files**.

### 5. Prove it never pays twice

Run `databricks bundle run cod_brand_payout` again. Step 1 finds only the held bags, because
every paid bag is already in `settlement_ledger`. Nothing is paid a second time.

### 6. Put it on GitHub and let Actions deploy

```bash
git init && git add . && git commit -m "COD payout bundle"
git branch -M main
git remote add origin https://github.com/<you>/fynd-cod-payout.git
git push -u origin main
```

Then in GitHub: **Settings › Secrets and variables › Actions** › add

* `DATABRICKS_HOST`: your workspace URL
* `DATABRICKS_TOKEN`: from Databricks **User settings › Developer › Access tokens**

From now on: open a pull request → Actions deploys to **dev**. Merge it → Actions deploys to
**prod**, where the 07:00 schedule is live. Run the setup job once in prod to give it data.

## The dashboard

`bag_journey` is the table the bag journey dashboard reads: one row per bag with its
`payout_stage` (SETTLED, ON_HOLD, AWAITING_SETTLEMENT, …).

* **On Free Edition:** build a Databricks dashboard on `bag_journey` and `brand_payable_summary`.
* **At Fynd:** add a final task that writes these two tables to BigQuery. Looker Studio connects
  to BigQuery natively, so the existing Looker Studio dashboard just points at them.

## Moving this to Fynd's GCP workspace

1. Add a target (say `fynd_prod`) with `workspace: host:` set to the GCP workspace.
2. In step 1, read `bq_fynd.FP_analytics_dwh.avis_base_view` (via Lakehouse Federation)
   instead of the fake `bags` table.
3. Drop the setup job.
4. Confirm the business rules marked `CONFIRM` in `02_compute_brand_payable.py`
   (delivered status value, which coupons the brand funds, current TCS/TDS rates).
