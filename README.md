# sectors_corporate_actions

Scraper pipeline for all corporate actions in IDX using data from [this data sources](https://new.sahamidx.com).

However, there are several unavailable corporate action data in that source that we need to manually scrape it. The list of manually scraped data right now are
1. Right Issue (idx_right_issue table)
2. Reverse Stock Split (idx_stock_split table)
3. Buybacks (idx_buybacks)

For those three data, we need to manually add it using [this streamlit app](https://sectors-corporateaction.streamlit.app) that has been made to make it easier to update the data

## AGM (idx_agm)

Everything that fills `idx_agm` is one script, [`agm/agm_pipeline.py`](agm/agm_pipeline.py),
run daily at 18:00 WIB by [Daily AGM Pipeline](.github/workflows/daily_agm_pipeline.yaml).
Shareholder meetings come from KSEI; sahamidx is only used for public exposes,
which KSEI does not publish.

```bash
python -m agm.agm_pipeline                        # daily run, all four steps
python -m agm.agm_pipeline --dry-run              # fetch, but only show the write plan
python -m agm.agm_pipeline --month 9 --year 2026  # backfill one month
python -m agm.agm_pipeline --only minutes upsert  # run some steps only
```

Its four steps are:

1. `fetch_rups_schedule()` — KSEI meeting schedule (JSON API) and announcement
   PDFs. A cancellation marks the meeting `Dibatalkan` / `Cancelled`; a revision
   moves the existing row to the new date rather than adding a second one, and
   removes a leftover row from an earlier reschedule when the meeting never
   happened on that date.
2. `fetch_public_expose()` — public exposes from sahamidx.
3. `fetch_minutes()` — KSEI minutes of meeting (risalah): download, extract text
   (OCR for scans) and summarize into `summary` + `tags`.
4. `upsert_idx_agm()` — writes the result to `idx_agm`.

Every step is incremental, so a daily run does no repeated work. The only thing
stored is **`agm/state.json`** (a few kB): the KSEI file ids already handled and
what each one said, so no PDF is downloaded, OCR'd or summarized twice. It is
committed, so a fresh checkout (a GitHub runner) continues where the last run
left off. Everything else is checked against the database itself: before
writing, each row is compared with what `idx_agm` already holds and only real
differences are written, so a normal day touches a handful of rows. Empty values
never overwrite what is in the table, and PDFs are read from a temporary file
and deleted again.

Summaries keep the output format of
[summarize-agm-result](https://github.com/supertypeai/summarize-agm-result)
(`Agenda #1: ...` plus taxonomy tags); its summarizer is vendored unchanged in
[`agm/meeting_summarizer/`](agm/meeting_summarizer). The LLM step needs
`OPENROUTER_API_KEY` (or `OPENAI_API_KEY` / `GEMINI_API_KEY` with `--api`), and
text extraction needs `poppler-utils` plus `tesseract-ocr` for scanned PDFs.
