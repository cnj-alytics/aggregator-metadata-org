# aggregator-metadata-org

Workers that run on the **cnj-alytics** organisation's GitHub machines (Team plan, 60 at once),
separate from the 40 machines of the personal `cnj-analytics/aggregator_metadata` repo, which keeps
the hourly ranking scrapes.

Supabase (project `aggregator_metadata`) is the controller: it starts machines here through the
GitHub API, hands each one its work, and saves every result as soon as it arrives.

## Layout

| Path | What goes there |
|---|---|
| `.github/workflows/<platform>-<country>-<job>.yml` | One workflow per job |
| `<platform>/<country>/<job>.js` | The script that workflow runs |
| `cross-platform/<country>/` | Jobs that touch all four apps (Talabat, Careem, Deliveroo, Noon) |
| `archive/` | Retired jobs |

App folders (`talabat/`, `careem/`, `deliveroo/`, `noon/`) are added only when a job for that app moves here.

## Jobs

| Job | Workflow | Script | Started by |
|---|---|---|---|
| Menu check | `cross-platform-uae-menu-check.yml` | `cross-platform/uae/menu-check.js` | `select menu_check_start();` in Supabase |

## Setup

- Repo secrets: `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`
- Supabase Vault secret `github_pat_org`: fine-grained token, resource owner `cnj-alytics`,
  access to this repo only, permission **Actions: Read and write**
