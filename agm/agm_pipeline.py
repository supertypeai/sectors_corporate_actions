"""
AGM pipeline: everything that fills the Supabase `idx_agm` table.

Four steps, run daily:

1. fetch_rups_schedule()  KSEI meeting schedule (JSON API) plus the announcement
                          PDFs, which carry the meeting date, time and recording
                          date. A cancellation marks its meeting cancelled; a
                          revision moves the meeting, so the row idx_agm already
                          has changes date instead of a second row appearing.
2. fetch_public_expose()  Public exposes from sahamidx, the only source for them
                          (KSEI does not publish public exposes).
3. fetch_minutes()        KSEI minutes of meeting (risalah): download, extract
                          text (OCR for scans) and summarize into summary + tags,
                          keeping the output format of
                          supertypeai/summarize-agm-result.
4. upsert_idx_agm()       Writes the result to idx_agm.

Nothing is stored except `agm/state.json`, a few kB listing the KSEI file ids
already handled, so a PDF is never downloaded, OCR'd or summarized twice. The
database itself is the rest of the memory: before writing, every row is compared
with what is already in idx_agm and only real differences are written, so a
daily run normally touches a handful of rows. PDFs are read from a temporary
file and deleted again.

Sources:
    https://www.ksei.co.id/api/corporate_actions            (schedule, JSON)
    https://web.ksei.co.id/publications/corporate-action-schedules/meeting-announcement
    https://web.ksei.co.id/publications/corporate-action-schedules/minutes-of-meeting
    https://www.new.sahamidx.com/?/rups/page/<n>            (public expose)

Usage (from the repository root):
    python -m agm.agm_pipeline                         # daily run
    python -m agm.agm_pipeline --dry-run               # fetch, show the write plan only
    python -m agm.agm_pipeline --month 9 --year 2026   # backfill a month
    python -m agm.agm_pipeline --skip-summary          # no LLM calls
    python -m agm.agm_pipeline --force                 # rewrite rows even if unchanged

Environment: SUPABASE_URL, SUPABASE_KEY, and OPENROUTER_API_KEY for the summary
step (or OPENAI_API_KEY / GEMINI_API_KEY with --api). Text extraction needs
poppler-utils, and tesseract-ocr for scanned PDFs.
"""

from bs4 import BeautifulSoup
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

from dotenv import load_dotenv
from pypdf import PdfReader
from supabase import create_client

from agm.meeting_summarizer.config import load_settings
from agm.meeting_summarizer.summarizer import (
    MeetingSummarizer,
    _parse_iso_date,
    extract_meeting_date,
    extract_symbol,
)
from agm.meeting_summarizer import pdf as pdf_extractor
from agm.rups_place_helper import (
    clean_agm_place,
    detect_agm_place_desc,
    resolve_place_desc,
)

from contextlib import contextmanager

import pandas as pd
import argparse
import calendar
import json
import logging
import os
import re
import requests
import tempfile
import threading
import time


LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

file_handler = logging.FileHandler(Path(__file__).parent / "agm_pipeline.log")
file_handler.setFormatter(
    logging.Formatter("%(asctime)s [%(levelname)s] - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
)

LOGGER.addHandler(file_handler)
LOGGER.addHandler(logging.StreamHandler())


STATE_PATH = Path(__file__).parent / "state.json"

KSEI_API_URL = "https://www.ksei.co.id/api/corporate_actions"
KSEI_LEGACY_BASE_URL = "https://web.ksei.co.id"
KSEI_MEETING_URL = (
    f"{KSEI_LEGACY_BASE_URL}/publications/corporate-action-schedules/meeting-announcement"
)
KSEI_MINUTES_URL = (
    f"{KSEI_LEGACY_BASE_URL}/publications/corporate-action-schedules/minutes-of-meeting"
)
SAHAMIDX_RUPS_URL = "https://www.new.sahamidx.com/?/rups/page"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/128.0 Safari/537.36"
)

# The columns this pipeline owns in idx_agm (updated_on is set on every write)
SCHEDULE_COLUMNS = ["recording_date", "agm_time", "agm_place", "agm_place_desc"]
SUMMARY_COLUMNS = ["summary", "tags", "source_link", "source_file"]

# Rolling API window for the daily run: past days catch late corrections,
# future days catch newly announced meetings
DEFAULT_LOOKBACK_DAYS = 3
DEFAULT_LOOKAHEAD_DAYS = 45

# Equity tickers are 4 letters; bonds/sukuk (RUPO) use longer codes, e.g. PTPP03BCN1
EQUITY_CODE_PATTERN = re.compile(r"^[A-Z]{4}$")

MEETING_TYPE_PATTERN = re.compile(r"\((RUPS[A-Z]*|RUPO|RUPSU|RUPTS?)\)")

# Title prefix -> announcement type. KSEI filenames follow the same scheme:
# Peng_ (new), PengRev_ (revision), PengBatal_ (cancellation)
ANNOUNCEMENT_TYPES = {
    "Revisi Pemberitahuan": "REVISION",
    "Pembatalan Pemberitahuan": "CANCELLATION",
    "Pemberitahuan": "NEW",
}

# Shareholder meetings only: bond-holder (RUPO) minutes carry the issuer's name
# and would otherwise overwrite the equity AGM row
SHAREHOLDER_MEETING_TYPES = {"RUPS", "RUPST", "RUPSLB", "RUPSTLB"}

PDF_SYMBOL_PATTERN = re.compile(r"^Peng[A-Za-z]*_[A-Z]+_([A-Z]{4})")

# "... akan diadakan pada tanggal 12.10.2026, pukul 11:00."
# "... akan diadakan pada hari tanggal 29.01.2026 11:00 dibatalkan ."
PDF_AGM_PATTERN = re.compile(
    r"diadakan pada (?:hari )?tanggal (\d{2})\.(\d{2})\.(\d{4}),?\s*(?:pukul\s*)?(\d{1,2})[:.](\d{2})"
)
PDF_RECORDING_PATTERN = re.compile(
    r"tercatat dalam Daftar Pemegang Saham.{0,120}?tanggal (\d{2})\.(\d{2})\.(\d{4})"
)

# Meeting date phrasings in KSEI minutes, most specific first
_DAY_DATE = r"[A-Za-z']+\s*,?\s*(?:tanggal\s+|date\s+)?(\d{1,2}\s+[A-Za-z]+\s+\d{4})"
MINUTES_DATE_PATTERNS = [
    # "Hari/Tanggal : Selasa, 15 September 2026" / "Day / Date : Tuesday, 15 September 2026"
    re.compile(r"(?:Hari\s*/\s*Tanggal|Day\s*/\s*Date)\s*:?\s*" + _DAY_DATE, re.IGNORECASE),
    # "Rapat diselenggarakan pada hari Selasa, tanggal 15 September 2026"
    re.compile(
        r"(?:Rapat|RUPS\w*)\s+(?:telah\s+)?(?:diselenggarakan|dilaksanakan|diadakan)\s+"
        r"pada\s+(?:hari\s+)?" + _DAY_DATE,
        re.IGNORECASE,
    ),
    # "pada hari ini, Kamis, tanggal 10 September 2026" (notarial deed wording)
    re.compile(r"pada\s+hari\s+ini\s*,?\s*" + _DAY_DATE, re.IGNORECASE),
    # "The Meeting was held on Tuesday, 15 September 2026"
    re.compile(r"(?:held|convened)\s+on\s+" + _DAY_DATE, re.IGNORECASE),
]

ID_MONTHS = {
    "januari": 1,
    "februari": 2,
    "maret": 3,
    "april": 4,
    "mei": 5,
    "juni": 6,
    "juli": 7,
    "agustus": 8,
    "september": 9,
    "oktober": 10,
    "november": 11,
    "desember": 12,
}

# Indonesian month abbreviations used by sahamidx
ID_TO_EN_MONTHS = {"Mei": "May", "Ags": "Aug", "Agu": "Aug", "Okt": "Oct", "Nop": "Nov", "Des": "Dec"}
DATE_FORMATS = ["%d-%b-%Y", "%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"]

# Scanned PDFs return (almost) no text from pdftotext
MIN_TEXT_CHARS = 200

# Hard limit per meeting summary (see summarize_with_timeout)
SUMMARY_TIMEOUT_SEC = 300

# A meeting is held about 23 days after its recording date (99% within 31).
# Anything far outside that is a KSEI typo, e.g. ASMI recorded 2026-07-02 for a
# meeting dated 2027-07-27, and must not become a row of its own.
MAX_RECORDING_GAP_DAYS = 90

# Minutes are published after the meeting, at most this long after it
MAX_MINUTES_DELAY_DAYS = 90

# How close a date from a document has to be to an idx_agm row to mean the same
# meeting (the window summarize-agm-result uses)
DATE_MATCH_WINDOW_DAYS = 7

# A KSEI file that keeps failing is retried this many times, then left alone
MAX_ATTEMPTS = 5


# --------------------------------------------------------------------------
# State: the KSEI file ids already handled
# --------------------------------------------------------------------------
def load_state() -> dict:
    if not STATE_PATH.exists():
        return {"announcements": {}, "minutes": {}}

    state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    state.setdefault("announcements", {})
    state.setdefault("minutes", {})
    return state


def save_state(state: dict):
    state["updated_on"] = datetime.now().isoformat(timespec="seconds")
    STATE_PATH.write_text(json.dumps(state, indent=1, sort_keys=True), encoding="utf-8")


def pending_ids(entries: list[dict], state_section: dict) -> list[dict]:
    """The listed files not handled yet, and the failed ones still worth a retry."""
    pending = []

    for entry in entries:
        seen = state_section.get(entry["file_id"])
        if seen is None:
            pending.append(entry)
        elif not seen.get("done") and seen.get("attempts", 0) < MAX_ATTEMPTS:
            pending.append(entry)

    return pending


def record_attempt(
    state_section: dict, file_id: str, done: bool, note: str = None, meeting: dict = None
):
    entry = state_section.setdefault(file_id, {"attempts": 0})
    entry["attempts"] = entry.get("attempts", 0) + 1
    entry["done"] = done

    # What the file said, so its meeting can be replayed into later runs without
    # downloading it again
    if meeting:
        entry["meeting"] = meeting

    if note:
        entry["note"] = note[:200]
    elif "note" in entry:
        del entry["note"]


def prune_state(state: dict, keep_days: int = 400):
    """Forget files whose meeting is long past, to keep state.json small."""
    cutoff = (date.today() - timedelta(days=keep_days)).isoformat()

    for section in ["announcements", "minutes"]:
        for file_id, entry in list(state[section].items()):
            agm_date = (entry.get("meeting") or {}).get("agm_date")
            if agm_date and agm_date < cutoff:
                del state[section][file_id]


# --------------------------------------------------------------------------
# HTTP
# --------------------------------------------------------------------------
def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    return session


@contextmanager
def downloaded_pdf(session: requests.Session, url: str, filename: str, **kwargs):
    """
    Download `url` as a PDF into a temp file, yield its local path, delete it
    once the block exits. Raises RuntimeError if the response is not a PDF.
    """
    kwargs.setdefault("timeout", 120)
    response = request_with_retry(session, url, **kwargs)

    if response is None or not response.content.startswith(b"%PDF"):
        raise RuntimeError(f"Failed to download a valid PDF from {url}")

    with tempfile.TemporaryDirectory(prefix="agm_") as temp_dir:
        pdf_path = os.path.join(temp_dir, filename)
        with open(pdf_path, "wb") as pdf_file:
            pdf_file.write(response.content)
        yield pdf_path


def request_with_retry(
    session: requests.Session, url: str, retries: int = 3, backoff: float = 5.0, **kwargs
) -> requests.Response | None:
    kwargs.setdefault("timeout", 60)

    for attempt in range(1, retries + 1):
        try:
            response = session.get(url, **kwargs)
            response.raise_for_status()
            return response

        except requests.exceptions.RequestException as error:
            LOGGER.warning(f"Attempt {attempt}/{retries} failed for {url}: {error}")
            if attempt < retries:
                time.sleep(backoff * attempt)

    LOGGER.error(f"Giving up on {url} after {retries} attempts")
    return None


def month_days(month: int, year: int) -> list[date]:
    last_day = calendar.monthrange(year, month)[1]
    return [date(year, month, day) for day in range(1, last_day + 1)]


def months_to_check(month: int = None, year: int = None) -> list[tuple[int, int]]:
    """One month for a backfill, else this month plus last month in its first week."""
    today = date.today()

    if month:
        return [(month, year or today.year)]

    months = [(today.month, today.year)]
    if today.day <= 7:
        last_month = today.replace(day=1) - timedelta(days=1)
        months.append((last_month.month, last_month.year))
    return months


# --------------------------------------------------------------------------
# KSEI schedule (JSON API)
# --------------------------------------------------------------------------
def parse_meeting_description(description: str) -> tuple[str | None, str | None]:
    """
    Parse the KSEI proxy voting description into (agm_time, agm_place).

    Example description:
        "Waktu : 14:00 WIB - selesai \\nTempat : Plaza Asia Lt. 28, Jl. Jend.
         Sudirman\\nKav. 59 \\n\\nBagi Pemegang Rekening yang akan hadir ..."
    """
    if not description:
        return None, None

    text = description.replace("\r\n", "\n")

    agm_time = None
    time_match = re.search(r"Waktu\s*:\s*(\d{1,2})[:.](\d{2})", text, re.IGNORECASE)
    if time_match:
        agm_time = f"{int(time_match.group(1)):02d}:{time_match.group(2)}:00"

    agm_place = None
    place_match = re.search(
        r"Tempat\s*:\s*(.*?)(?:\n\s*\n|Bagi Pemegang Rekening|$)",
        text,
        re.IGNORECASE | re.DOTALL,
    )
    if place_match:
        agm_place = re.sub(r"\s+", " ", place_match.group(1)).strip() or None

    return agm_time, agm_place


def fetch_meetings_api(days: list[date], session: requests.Session) -> pd.DataFrame:
    """
    Shareholder meetings (type_of_ca = PROXY VOTING) from the KSEI API.

    The API takes one day per request and returns every meeting whose cum,
    record or meeting date falls on it, so a range is asked for day by day and
    deduplicated. Only equities are kept: bond-holder (RUPO) meetings carry a
    security code like PTPP03BCN1 and do not belong in idx_agm.
    """
    meetings = {}

    LOGGER.info(f"[SCHEDULE] Fetching KSEI meetings from {days[0]} to {days[-1]}")

    for day in days:
        response = request_with_retry(
            session,
            KSEI_API_URL,
            params={
                "locale": "id",
                "filter[ca_date]": day.isoformat(),
                "type_of_ca": "PROXY VOTING",
            },
            headers={"Accept": "application/json"},
        )

        if response is None:
            continue

        for row in response.json().get("data", []):
            security_code = (row.get("security_code") or "").strip()
            if not EQUITY_CODE_PATTERN.match(security_code):
                continue

            agm_time, raw_place = parse_meeting_description(row.get("description"))
            agm_place = clean_agm_place(raw_place) if raw_place else None
            agm_date = (row.get("effective_date") or "")[:10] or None

            meetings[(f"{security_code}.JK", agm_date)] = {
                "symbol": f"{security_code}.JK",
                "agm_date": agm_date,
                "recording_date": (row.get("record_date") or "")[:10] or None,
                "agm_time": agm_time,
                "agm_place": agm_place,
                "agm_place_desc": detect_agm_place_desc(agm_place) if agm_place else None,
            }

        time.sleep(0.5)

    LOGGER.info(f"[SCHEDULE] {len(meetings)} meetings from the API")
    return pd.DataFrame(meetings.values())


# --------------------------------------------------------------------------
# KSEI publication listings (announcements and minutes share one layout)
# --------------------------------------------------------------------------
def parse_indonesian_date(date_str: str) -> str | None:
    """Parse '07 Maret 2026' into '2026-03-07'."""
    parts = (date_str or "").strip().split()
    if len(parts) != 3:
        return None

    day, month_name, year = parts
    month = ID_MONTHS.get(month_name.lower())

    if month is None or not day.isdigit() or not year.isdigit():
        return None

    return date(int(year), month, int(day)).isoformat()


def parse_announcement_title(title: str) -> tuple[str | None, str | None]:
    """
    Extract (meeting_type, company_name) from a title such as
    'Pemberitahuan Rapat Umum Pemegang Saham Tahunan (RUPST) BANK MEGA Tbk, PT'.
    """
    type_match = MEETING_TYPE_PATTERN.search(title)
    if not type_match:
        return None, None

    return type_match.group(1), title[type_match.end():].strip() or None


def parse_announcement_type(title: str) -> str | None:
    for prefix, announcement_type in ANNOUNCEMENT_TYPES.items():
        if title.startswith(prefix):
            return announcement_type

    return None


def parse_listing(html: str, page_url: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    entries = []

    table = soup.find("table", class_="table")
    if table is None or table.tbody is None:
        return entries

    for row in table.tbody.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 3:
            continue

        link = cells[0].find("a", href=True)
        if link is None:
            continue

        # The live page double-escapes "&" in its links (Month=09&amp;amp;Year=...),
        # so rebuild the URL from the file id instead of trusting the href
        file_id = re.search(r"file=(\w+)", link["href"])
        title = cells[1].get_text(" ", strip=True)
        meeting_type, company_name = parse_announcement_title(title)

        entries.append(
            {
                "file_id": file_id.group(1) if file_id else None,
                "title": title,
                "announcement_type": parse_announcement_type(title),
                "meeting_type": meeting_type,
                "company_name": company_name,
                "announcement_date": parse_indonesian_date(cells[2].get_text(strip=True)),
                "download_url": (
                    f"{page_url}?file={file_id.group(1)}"
                    if file_id
                    else urljoin(page_url, link["href"])
                ),
            }
        )

    return entries


def fetch_listing(
    page_url: str, months: list[tuple[int, int]], session: requests.Session
) -> list[dict]:
    """Read a KSEI publication list for the given months."""
    entries = []

    for month, year in months:
        LOGGER.info(f"[KSEI] Listing {page_url.rsplit('/', 1)[-1]} for {year}-{month:02d}")
        response = request_with_retry(
            session,
            page_url,
            params={"Month": f"{month:02d}", "Year": str(year)},
            headers={"Accept": "text/html", "Accept-Language": "id-ID"},
            timeout=90,
        )

        if response is None:
            LOGGER.error("[KSEI] Could not load the list; the legacy site may be down")
            continue

        entries += parse_listing(response.text, page_url)

    seen = set()
    unique = []
    for entry in entries:
        if entry["file_id"] and entry["file_id"] not in seen:
            seen.add(entry["file_id"])
            unique.append(entry)

    LOGGER.info(f"[KSEI] {len(unique)} files listed")
    return unique


# --------------------------------------------------------------------------
# Announcement PDFs
# --------------------------------------------------------------------------
def parse_announcement_pdf(pdf_path: str) -> dict:
    """
    Extract the meeting date/time and recording date from an announcement PDF.
    Cancellation PDFs only carry the (cancelled) meeting date and time.
    """
    result = {"agm_date": None, "agm_time": None, "recording_date": None}

    try:
        reader = PdfReader(pdf_path)
        text = " ".join(page.extract_text() or "" for page in reader.pages)
    except Exception as error:
        LOGGER.error(f"[ANNOUNCEMENT] Could not read {pdf_path}: {error}")
        return result

    text = re.sub(r"\s+", " ", text)

    agm_match = PDF_AGM_PATTERN.search(text)
    if agm_match:
        day, month, year, hour, minute = agm_match.groups()
        result["agm_date"] = f"{year}-{month}-{day}"
        result["agm_time"] = f"{int(hour):02d}:{minute}:00"

    recording_match = PDF_RECORDING_PATTERN.search(text)
    if recording_match:
        day, month, year = recording_match.groups()
        result["recording_date"] = f"{year}-{month}-{day}"

    return result


def read_announcement_pdf(entry: dict, session: requests.Session) -> dict | None:
    """
    Read one announcement PDF and return the meeting it describes: symbol (from
    the KSEI filename, e.g. Peng_RUPSLB_PEVE1_07032026_203529.pdf -> PEVE), date,
    time and recording date. The PDF itself is not kept.
    """
    # Resolve the ?file=<id> redirect first to learn the real filename
    redirect = request_with_retry(
        session, entry["download_url"], allow_redirects=False, retries=2
    )
    if redirect is None:
        return None

    location = redirect.headers.get("Location")
    pdf_url = urljoin(KSEI_LEGACY_BASE_URL, location) if location else entry["download_url"]
    filename = os.path.basename(urlparse(pdf_url).path)
    symbol_match = PDF_SYMBOL_PATTERN.match(filename)

    try:
        with downloaded_pdf(session, pdf_url, filename) as pdf_path:
            parsed = parse_announcement_pdf(pdf_path)
    except RuntimeError as error:
        LOGGER.error(f"[ANNOUNCEMENT] {error}")
        return None

    if not symbol_match or not parsed["agm_date"]:
        LOGGER.warning(f"[ANNOUNCEMENT] No symbol or date in {filename}")
        return None

    return {
        "symbol": f"{symbol_match.group(1)}.JK",
        "announcement_type": entry["announcement_type"],
        "meeting_type": entry["meeting_type"],
        "announcement_date": entry["announcement_date"],
        "title": entry["title"],
        **parsed,
    }


def announced_on(file_id: str, meeting: dict) -> str:
    """When an announcement was published; KSEI file ids end with <date><time>."""
    if meeting.get("announcement_date"):
        return meeting["announcement_date"]

    match = re.match(r"^\d+?(\d{8})\d{6}$", file_id)
    return (
        f"{match.group(1)[:4]}-{match.group(1)[4:6]}-{match.group(1)[6:]}" if match else ""
    )


def meeting_type_of(meeting: dict) -> str | None:
    if meeting.get("meeting_type"):
        return meeting["meeting_type"]

    meeting_type, _ = parse_announcement_title(meeting.get("title") or "")
    return meeting_type


# --------------------------------------------------------------------------
# Minutes of meeting
# --------------------------------------------------------------------------
def extract_transcript(pdf_path: str) -> str:
    """pdftotext first; OCR when the PDF is a scan (little or no text layer)."""
    text = pdf_extractor._extract_text_with_pdftotext(Path(pdf_path))

    if len(re.sub(r"\s", "", text)) >= MIN_TEXT_CHARS:
        return text

    LOGGER.info("[MINUTES] Little text in the PDF, running OCR")
    ocr_text = pdf_extractor._extract_text_with_ocr(Path(pdf_path))

    if len(re.sub(r"\s", "", ocr_text)) > len(re.sub(r"\s", "", text)):
        return ocr_text

    if not text.strip():
        raise RuntimeError("No text extracted from PDF (pdftotext and OCR)")

    return text


def download_minutes_text(entry: dict, session: requests.Session) -> str:
    """Download one minutes PDF, extract its text and drop the PDF again."""
    with downloaded_pdf(session, entry["download_url"], f"{entry['file_id']}.pdf", retries=2) as pdf_path:
        return extract_transcript(pdf_path)


def normalize_company_name(name: str) -> str:
    name = (name or "").upper().replace("&", " DAN ")
    name = re.sub(r"\(PERSERO\)|\bPERSERO\b|\bTBK\b\.?|\bPT\b\.?", "", name)
    return re.sub(r"[^A-Z0-9]", "", name)


def resolve_symbol(company_name: str, transcript: str, lookup: dict[str, str]) -> str | None:
    """
    KSEI serves the minutes PDF without a filename, so the ticker comes from the
    company name in the title. KSEI's own names (from the schedule and the
    announcements of this run) take priority over idx_company_profile, because
    they are written the same way and follow renames.
    """
    symbol = lookup.get(normalize_company_name(company_name))
    if symbol:
        return symbol

    # Fall back to "Kode Emiten : XXXX" in the document, like summarize-agm-result
    from_text = extract_symbol(transcript)
    return None if from_text == "NOT_STATED.JK" else from_text


def resolve_agm_date(
    transcript: str, minutes_date: str | None, scheduled_dates: list[str]
) -> str | None:
    """
    Meeting date for the minutes.

    A date stated in the minutes wins, because meetings are often postponed by a
    few days (FORU: scheduled 16 July, held 22 July). Dates outside the window
    between the meeting and its publication are ignored, since the first date in
    a minutes PDF is often the cover letter date or an older meeting. With no
    usable date in the text, the latest meeting already known for that company
    before the publication date is used.
    """

    def is_plausible(agm_date: str | None) -> bool:
        if not agm_date or not minutes_date:
            return bool(agm_date)
        earliest = date.fromisoformat(minutes_date) - timedelta(days=MAX_MINUTES_DELAY_DAYS)
        return earliest.isoformat() <= agm_date <= minutes_date

    preview = transcript[:16000]
    candidates = [
        _parse_iso_date(match.group(1))
        for pattern in MINUTES_DATE_PATTERNS
        for match in pattern.finditer(preview)
    ]
    candidates.append(extract_meeting_date(transcript, doc_type="agms"))

    parsed = next((candidate for candidate in candidates if is_plausible(candidate)), None)
    if parsed:
        return parsed

    plausible_scheduled = sorted(
        agm_date for agm_date in scheduled_dates if is_plausible(agm_date)
    )
    return plausible_scheduled[-1] if plausible_scheduled else None


def summarize_with_timeout(summarizer: MeetingSummarizer, transcript: str) -> dict:
    """
    Run the summarizer with a hard time limit. The vendored HTTP timeouts only
    cover gaps between received bytes, so a slowly streamed response can hang a
    run indefinitely; an abandoned daemon thread does not block exiting.
    """
    result = {}

    def target():
        try:
            result["payload"] = json.loads(summarizer.summarize(transcript, doc_type="agms"))
        except Exception as error:
            result["error"] = error

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(SUMMARY_TIMEOUT_SEC)

    if thread.is_alive():
        raise TimeoutError(f"Summarizer did not finish within {SUMMARY_TIMEOUT_SEC}s")
    if "error" in result:
        raise result["error"]

    return result["payload"]


# --------------------------------------------------------------------------
# Public expose (sahamidx)
# --------------------------------------------------------------------------
def parse_date_safe(date_str: str) -> str | None:
    if not date_str:
        return None

    date_str = date_str.strip()
    if date_str in {"", "-", "N/A"}:
        return None

    for id_month, en_month in ID_TO_EN_MONTHS.items():
        date_str = date_str.replace(id_month, en_month)

    for date_format in DATE_FORMATS:
        try:
            return datetime.strptime(date_str, date_format).strftime("%Y-%m-%d")
        except ValueError:
            continue

    LOGGER.warning(f"[PUBEX] Could not parse date: {date_str}")
    return None


def pubex_scraper(
    session: requests.Session, listed_symbols: set[str], end_date: str = None
) -> pd.DataFrame:
    """
    Scrape the sahamidx RUPS schedule and keep only the public exposes with
    agm_date on or after `end_date` (default today).

    A meeting can appear as several rows; they are grouped by (symbol, agm_date)
    and resolve_place_desc picks the description, which is why a public expose
    whose agm_place reads e.g. "Zoom Meeting" is still recognised as one.
    """
    end_date = (
        datetime.now() if end_date is None else datetime.strptime(end_date, "%Y-%m-%d")
    ).strftime("%Y-%m-%d")

    LOGGER.info(f"[PUBEX] Scraping sahamidx from cutoff {end_date}")

    page = 1
    rows = []
    keep_scraping = True

    while keep_scraping:
        response = request_with_retry(session, f"{SAHAMIDX_RUPS_URL}/{page}")
        if response is None:
            break

        table_rows = BeautifulSoup(response.text, "lxml").find_all("tr")

        for row in table_rows:
            try:
                cells = {
                    field: row.find("td", {"data-header": header})
                    for field, header in [
                        ("symbol", "Kode Emiten"),
                        ("recording_date", "Tanggal Rekording"),
                        ("agm_date", "Tanggal Rups"),
                        ("agm_place", "Tempat"),
                        ("agm_time", "Jam"),
                    ]
                }

                if not all(cells.values()):
                    continue

                symbol = f"{cells['symbol'].text.strip()}.JK"
                if symbol not in listed_symbols:
                    continue

                recording_date = parse_date_safe(cells["recording_date"].text.strip())
                agm_date = parse_date_safe(cells["agm_date"].text.strip())

                if not recording_date or not agm_date or recording_date > agm_date:
                    continue

                if agm_date < end_date:
                    LOGGER.info(f"[PUBEX] Reached agm_date {agm_date} before {end_date}, stopping")
                    keep_scraping = False
                    break

                agm_place = clean_agm_place(cells["agm_place"].text.strip())
                rows.append(
                    {
                        "symbol": symbol,
                        "recording_date": recording_date,
                        "agm_date": agm_date,
                        "agm_place": agm_place,
                        "agm_time": cells["agm_time"].text.strip(),
                        "agm_place_desc": detect_agm_place_desc(agm_place),
                    }
                )

            except Exception as error:
                LOGGER.error(f"[PUBEX] Skipping row due to error: {error}")

        if not keep_scraping:
            break

        page += 1
        time.sleep(1.2)

    scraped = pd.DataFrame(rows)
    if scraped.empty:
        return scraped

    scraped = scraped.sort_values("recording_date", ascending=False)
    merged = []

    for _, group in scraped.groupby(["symbol", "agm_date"], sort=False, dropna=False):
        base = group.iloc[0].copy()
        descriptions = list(
            dict.fromkeys(value for value in group["agm_place_desc"] if str(value).strip())
        )
        places = list(dict.fromkeys(value for value in group["agm_place"] if str(value).strip()))

        base["agm_place_desc"] = resolve_place_desc(descriptions)
        base["agm_place"] = places[0] if places else None
        merged.append(base.to_dict())

    public_exposes = pd.DataFrame(merged)
    public_exposes = public_exposes[public_exposes["agm_place_desc"] == "Public expose"]

    LOGGER.info(f"[PUBEX] {len(public_exposes)} public exposes out of {len(merged)} meetings")
    return public_exposes.reset_index(drop=True)


# --------------------------------------------------------------------------
# Database
# --------------------------------------------------------------------------
def supabase_client():
    return create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))


def fetch_listed_symbols(client) -> dict[str, str]:
    """{symbol: company_name} of every company in idx_company_profile."""
    rows = client.from_("idx_company_profile").select("symbol,company_name").execute().data
    return {row["symbol"]: row.get("company_name") or "" for row in rows}


EXISTING_ROW_COLUMNS = [
    "symbol",
    "recording_date",
    "agm_date",
    "agm_place",
    "agm_place_desc",
    "agm_time",
    "summary",
    "tags",
    "source_link",
    "source_file",
]


def fetch_existing_rows(client, symbols: list[str]) -> pd.DataFrame:
    """Every idx_agm row of the symbols this run touches."""
    rows = []
    for start in range(0, len(symbols), 100):
        rows += (
            client.table("idx_agm")
            .select(",".join(EXISTING_ROW_COLUMNS))
            .in_("symbol", symbols[start : start + 100])
            .execute()
            .data
        )

    return pd.DataFrame(rows, columns=EXISTING_ROW_COLUMNS)


def same_value(new_value, old_value) -> bool:
    if isinstance(new_value, list) or isinstance(old_value, list):
        return list(new_value or []) == list(old_value or [])

    old_text = "" if old_value is None else str(old_value)
    new_text = "" if new_value is None else str(new_value)

    # Postgres returns a time column as HH:MM:SS
    if re.fullmatch(r"\d{2}:\d{2}", new_text):
        new_text += ":00"

    return new_text == old_text


def plan_writes(rows: list[dict], existing: pd.DataFrame, listed: set[str], force: bool) -> list[dict]:
    """
    Decide what to do with every row, given what idx_agm already holds.

    idx_agm has PRIMARY KEY (symbol, recording_date) and UNIQUE (symbol,
    agm_date), and symbol must exist in idx_company_profile, so a plain upsert
    can violate a key. Rows are written as explicit updates and inserts:

    - the meeting is already there -> update the fields that actually differ;
      its recording_date is kept when the new one belongs to another meeting
    - it is not there -> insert, unless (symbol, recording_date) is taken
    - a summary whose meeting is not there -> attach it to the same company's
      meeting within +-7 days (that row then moves to the minutes' date)
    """
    by_agm_date = {
        (row.symbol, row.agm_date): row._asdict() for row in existing.itertuples(index=False)
    }
    by_recording_date = {
        (row.symbol, row.recording_date): row.agm_date for row in existing.itertuples(index=False)
    }

    plans = []

    for row in rows:
        symbol, agm_date = row["symbol"], row["agm_date"]
        values = {
            column: value
            for column, value in row.items()
            # pandas fills a missing column with NaN, which is not valid JSON
            if value is not None and not (isinstance(value, float) and pd.isna(value))
        }
        plan = {"symbol": symbol, "agm_date": agm_date, "match_agm_date": agm_date, "reason": ""}

        if symbol not in listed:
            plans.append({**plan, "action": "skip", "reason": "symbol not in idx_company_profile"})
            continue

        previous = values.pop("previous_agm_date", None)
        current = by_agm_date.get((symbol, agm_date))

        # A revision: move the row the meeting already has, unless the meeting on
        # the old date already happened (it has a summary) or the new date exists
        if previous:
            prior = by_agm_date.get((symbol, previous))

            if prior is not None and prior.get("summary"):
                # The meeting on the old date went ahead, so it is not a leftover
                pass

            elif prior is not None and current is None:
                current = prior
                plan["match_agm_date"] = previous
                plan["reason"] = f"rescheduled from {previous}"
                values["agm_date"] = agm_date
                by_agm_date.pop((symbol, previous), None)

            elif prior is not None:
                # Both dates are in idx_agm: the meeting never happened on the
                # old one, so that row is a leftover of an earlier reschedule
                by_agm_date.pop((symbol, previous), None)
                by_recording_date.pop((symbol, prior.get("recording_date")), None)
                plans.append(
                    {
                        "symbol": symbol,
                        "agm_date": previous,
                        "match_agm_date": previous,
                        "action": "delete",
                        "reason": f"superseded by {agm_date}",
                    }
                )

        # A summary for a meeting idx_agm does not have under that date
        if current is None and "recording_date" not in values:
            candidates = [
                (existing_symbol, existing_date)
                for (existing_symbol, existing_date) in by_agm_date
                if existing_symbol == symbol
                and abs((date.fromisoformat(existing_date) - date.fromisoformat(agm_date)).days)
                <= DATE_MATCH_WINDOW_DAYS
            ]
            if not candidates:
                plans.append({**plan, "action": "skip", "reason": "no idx_agm row within +-7 days"})
                continue

            match = min(
                candidates,
                key=lambda key: (
                    abs((date.fromisoformat(key[1]) - date.fromisoformat(agm_date)).days),
                    key[1],
                ),
            )
            current = by_agm_date[match]
            plan["match_agm_date"] = match[1]
            values["agm_date"] = agm_date
            plan["reason"] = f"matched agm_date {match[1]}, moved to {agm_date}"

        if current is None:
            recording_date = values.get("recording_date")
            owner = by_recording_date.get((symbol, recording_date))

            if owner is not None:
                plans.append(
                    {
                        **plan,
                        "action": "skip",
                        "reason": f"recording_date {recording_date} already used by agm_date {owner}",
                    }
                )
                continue

            by_agm_date[(symbol, agm_date)] = {**values}
            by_recording_date[(symbol, recording_date)] = agm_date
            plans.append({**plan, "action": "insert", "values": values})
            continue

        # Keep the stored recording_date when the new one is another meeting's key
        recording_date = values.get("recording_date")
        if recording_date and recording_date != current.get("recording_date"):
            owner = by_recording_date.get((symbol, recording_date))
            if owner is not None and owner != plan["match_agm_date"]:
                values.pop("recording_date")
                plan["reason"] = (
                    f"kept recording_date {current.get('recording_date')}: "
                    f"{recording_date} belongs to agm_date {owner}"
                )

        changed = {
            column: value
            for column, value in values.items()
            if force or not same_value(value, current.get(column))
        }

        if not changed:
            plans.append({**plan, "action": "unchanged"})
            continue

        by_agm_date[(symbol, agm_date)] = {**current, **changed}
        plans.append({**plan, "action": "update", "values": changed})

    return plans


def execute_plan(plan: dict, query, empty_result: str):
    """Run one query, recording its outcome on the plan and logging a failure."""
    try:
        response = query.execute()
        plan["result"] = "ok" if response.data else empty_result
    except Exception as error:
        plan["result"] = f"error: {error}"

    if plan["result"] != "ok":
        LOGGER.error(f"[UPSERT] {plan['action']} {plan['symbol']} {plan['agm_date']}: {plan['result']}")


def apply_plans(client, plans: list[dict], dry_run: bool) -> list[dict]:
    """Run the planned writes, setting updated_on on everything written."""
    counts = pd.Series([plan["action"] for plan in plans]).value_counts().to_dict()
    LOGGER.info(f"[UPSERT] {counts}{' (dry run)' if dry_run else ''}")

    for plan in plans:
        if plan["action"] in {"insert", "skip", "delete"} or plan["reason"]:
            LOGGER.info(
                f"[UPSERT] {plan['action']:9} {plan['symbol']} {plan['agm_date']} "
                f"{plan['reason']} {','.join(plan.get('values', {}))}".rstrip()
            )

    if dry_run:
        return plans

    updated_on = datetime.now(timezone.utc).isoformat(sep=" ", timespec="microseconds")

    for plan in plans:
        if plan["action"] in {"skip", "unchanged"}:
            continue

        if plan["action"] == "delete":
            query = (
                client.table("idx_agm")
                .delete()
                .eq("symbol", plan["symbol"])
                .eq("agm_date", plan["agm_date"])
                .is_("summary", "null")
            )
            execute_plan(plan, query, "no row deleted")
            continue

        values = {**plan["values"], "updated_on": updated_on}

        if plan["action"] == "insert":
            query = client.table("idx_agm").insert(values)
        else:
            values.pop("symbol", None)
            query = (
                client.table("idx_agm")
                .update(values)
                .eq("symbol", plan["symbol"])
                .eq("agm_date", plan["match_agm_date"])
            )

        execute_plan(plan, query, "no row changed")

    written = sum(1 for plan in plans if plan.get("result") == "ok" and plan["action"] != "delete")
    deleted = sum(1 for plan in plans if plan.get("result") == "ok" and plan["action"] == "delete")
    LOGGER.info(f"[UPSERT] {written} rows written to idx_agm, {deleted} deleted")
    return plans


# --------------------------------------------------------------------------
# 1. KSEI RUPS schedule
# --------------------------------------------------------------------------
def previous_agm_date(revision: dict, earlier: list[tuple[str, str, dict]]) -> str | None:
    """
    The meeting date a revision supersedes: the date of the last announcement
    published earlier for the same company and the same kind of meeting (a RUPST
    revision does not move the RUPSLB).
    """
    meeting_type = meeting_type_of(revision)

    for _, _, announcement in reversed(earlier):
        if (
            announcement["symbol"] == revision["symbol"]
            and meeting_type_of(announcement) == meeting_type
            and announcement["agm_date"] != revision["agm_date"]
        ):
            return announcement["agm_date"]

    return None


def fetch_rups_schedule(
    session: requests.Session,
    state: dict,
    months: list[tuple[int, int]],
    days: list[date],
    skip_pdf: bool = False,
) -> tuple[pd.DataFrame, dict[str, str]]:
    """
    KSEI meeting schedule: the API for place, time and recording date, and the
    announcement PDFs for what changed since.

    Each announcement PDF is read once ever (the file ids live in state.json).
    A cancellation marks its meeting cancelled; a new or revised announcement
    carries the meeting's own dates, which take priority over the API.

    Returns the meetings and a {normalized company name: symbol} lookup for the
    minutes step.
    """
    meetings = fetch_meetings_api(days, session)
    rows = {
        (row["symbol"], row["agm_date"]): row for row in meetings.to_dict("records")
    } if not meetings.empty else {}

    lookup = {}
    entries = fetch_listing(KSEI_MEETING_URL, months, session)
    for entry in entries:
        if entry["company_name"]:
            lookup.setdefault(normalize_company_name(entry["company_name"]), None)

    if skip_pdf:
        return pd.DataFrame(rows.values()), {k: v for k, v in lookup.items() if v}

    pending = pending_ids(entries, state["announcements"])
    LOGGER.info(f"[SCHEDULE] {len(pending)} of {len(entries)} announcements to read")

    for position, entry in enumerate(pending, start=1):
        LOGGER.info(f"[SCHEDULE] {position}/{len(pending)} {entry['title']}")
        announcement = None

        try:
            announcement = read_announcement_pdf(entry, session)
        except Exception as error:
            LOGGER.error(f"[SCHEDULE] {entry['file_id']}: {error}")

        record_attempt(
            state["announcements"],
            entry["file_id"],
            done=announcement is not None,
            note=None if announcement else "could not read the PDF",
            meeting=announcement,
        )

        if announcement and entry["company_name"]:
            lookup[normalize_company_name(entry["company_name"])] = announcement["symbol"]

        time.sleep(1)

    # Replay every announcement ever read: their meetings are not always in the
    # API window, and a row that failed to reach idx_agm must be retried
    announcements = sorted(
        (
            (announced_on(file_id, stored["meeting"]), file_id, stored["meeting"])
            for file_id, stored in state["announcements"].items()
            if stored.get("meeting")
        )
    )

    # A revision supersedes the date its meeting had before: that date must not
    # be written again, and the row already on it is the one to move
    superseded = {}
    for position, (_, _, announcement) in enumerate(announcements):
        if announcement["announcement_type"] != "REVISION":
            continue

        previous = previous_agm_date(announcement, announcements[:position])
        if previous and previous != announcement["agm_date"]:
            superseded[(announcement["symbol"], previous)] = announcement["agm_date"]

    # The API keeps returning a meeting on its old date long after a revision
    # moved it, so drop those rows too
    for key in superseded:
        rows.pop(key, None)

    for _, _, announcement in announcements:
        key = (announcement["symbol"], announcement["agm_date"])

        if key in superseded:
            continue
        row = rows.get(key, {"symbol": key[0], "agm_date": key[1]})

        # The announcement is the company's own statement, so it wins
        for column in ["recording_date", "agm_time"]:
            if announcement.get(column):
                row[column] = announcement[column]

        if announcement["announcement_type"] == "CANCELLATION":
            row["agm_place"] = "Dibatalkan"
            row["agm_place_desc"] = "Cancelled"

        # So the existing idx_agm row is moved instead of a second row appearing
        for (symbol, previous), moved_to in superseded.items():
            if symbol == key[0] and moved_to == key[1]:
                row["previous_agm_date"] = previous

        rows[key] = row

    schedule = pd.DataFrame(rows.values())
    LOGGER.info(f"[SCHEDULE] {len(schedule)} meetings to check against idx_agm")
    return schedule, {name: symbol for name, symbol in lookup.items() if symbol}


# --------------------------------------------------------------------------
# 2. Public expose (sahamidx)
# --------------------------------------------------------------------------
def fetch_public_expose(session: requests.Session, listed_symbols: set[str]) -> pd.DataFrame:
    """Public exposes from sahamidx; KSEI does not publish them."""
    return pubex_scraper(session, listed_symbols)


# --------------------------------------------------------------------------
# 3. KSEI minutes of meeting
# --------------------------------------------------------------------------
def fetch_minutes(
    session: requests.Session,
    state: dict,
    months: list[tuple[int, int]],
    schedule: pd.DataFrame,
    existing: pd.DataFrame,
    lookup: dict[str, str],
    company_names: dict[str, str],
    api: str = "openrouter",
    skip_summary: bool = False,
) -> pd.DataFrame:
    """
    Download the new minutes, extract their text and summarize them.

    Each file is handled once ever. Minutes published in the same run for the
    same meeting (a RUPST and RUPSLB held on one day share one idx_agm row) are
    summarized together.
    """
    entries = [
        entry
        for entry in fetch_listing(KSEI_MINUTES_URL, months, session)
        if entry["meeting_type"] in SHAREHOLDER_MEETING_TYPES
    ]
    pending = pending_ids(entries, state["minutes"])
    LOGGER.info(f"[MINUTES] {len(pending)} of {len(entries)} minutes to read")

    if not pending:
        return pd.DataFrame(), {}

    # Company name -> ticker: KSEI's own names first, then idx_company_profile
    name_lookup = dict(lookup)
    for symbol, company_name in company_names.items():
        name_lookup.setdefault(normalize_company_name(company_name), symbol)

    documents = {}

    for position, entry in enumerate(pending, start=1):
        LOGGER.info(f"[MINUTES] {position}/{len(pending)} {entry['title']}")

        try:
            transcript = download_minutes_text(entry, session)

            symbol = resolve_symbol(entry["company_name"], transcript, name_lookup)
            if symbol is None:
                raise RuntimeError(f"Could not resolve ticker for {entry['company_name']}")

            scheduled_dates = []
            if not schedule.empty:
                scheduled_dates += schedule[schedule["symbol"] == symbol]["agm_date"].tolist()
            if not existing.empty:
                scheduled_dates += existing[existing["symbol"] == symbol]["agm_date"].tolist()

            agm_date = resolve_agm_date(transcript, entry["announcement_date"], scheduled_dates)
            if agm_date is None:
                raise RuntimeError("Could not resolve the meeting date")

            documents.setdefault((symbol, agm_date), []).append(
                {**entry, "symbol": symbol, "agm_date": agm_date, "transcript": transcript}
            )

        except Exception as error:
            LOGGER.error(f"[MINUTES] {entry['file_id']}: {error}")
            record_attempt(state["minutes"], entry["file_id"], done=False, note=str(error))

        time.sleep(1)

    if skip_summary or not documents:
        return pd.DataFrame(), {}

    summarizer = MeetingSummarizer(load_settings(api))
    summaries = []
    written_by = {}

    LOGGER.info(f"[MINUTES] Summarizing {len(documents)} meetings with {api}")

    for position, ((symbol, agm_date), group) in enumerate(documents.items(), start=1):
        # RUPST first so its link is the meeting's source_link
        group = sorted(group, key=lambda document: (document["meeting_type"] != "RUPST", document["file_id"]))
        LOGGER.info(f"[MINUTES] {position}/{len(documents)} Summarizing {symbol} {agm_date}")

        try:
            transcript = "\n\n".join(
                (
                    f"===== {document['title']} =====\n\n{document['transcript']}"
                    if len(group) > 1
                    else document["transcript"]
                )
                for document in group
            )
            payload = summarize_with_timeout(summarizer, transcript)

            summaries.append(
                {
                    "symbol": symbol,
                    "agm_date": agm_date,
                    "summary": payload["summary"],
                    "tags": payload.get("tags", []),
                    "source_link": group[0]["download_url"],
                    "source_file": "; ".join(
                        f"{symbol.replace('.JK', '')} - {document['title']}" for document in group
                    ),
                }
            )

            written_by[(symbol, agm_date)] = [document["file_id"] for document in group]

        except Exception as error:
            LOGGER.error(f"[MINUTES] Summarizing {symbol} {agm_date} failed: {error}")
            for document in group:
                record_attempt(state["minutes"], document["file_id"], done=False, note=str(error))

    return pd.DataFrame(summaries), written_by


# --------------------------------------------------------------------------
# 4. Upsert
# --------------------------------------------------------------------------
def drop_implausible(schedule: pd.DataFrame) -> pd.DataFrame:
    """
    A meeting is held about 23 days after its recording date (99% within 31), so
    a much larger gap is a KSEI typo, e.g. ASMI recorded 2026-07-02 for a meeting
    dated 2027-07-27, and must not become a row of its own.
    """
    if schedule.empty or "recording_date" not in schedule.columns:
        return schedule

    gap = (
        pd.to_datetime(schedule["agm_date"], errors="coerce")
        - pd.to_datetime(schedule["recording_date"], errors="coerce")
    ).dt.days
    implausible = gap.notna() & (gap.abs() > MAX_RECORDING_GAP_DAYS)

    for index, row in schedule[implausible].iterrows():
        LOGGER.warning(
            f"[SCHEDULE] Dropping {row['symbol']} {row['agm_date']}: "
            f"{int(gap[index])} days after recording_date {row['recording_date']}"
        )

    return schedule[~implausible]


def upsert_idx_agm(
    client,
    schedule: pd.DataFrame,
    public_exposes: pd.DataFrame,
    summaries: pd.DataFrame,
    existing: pd.DataFrame,
    listed: set[str],
    dry_run: bool = False,
    force: bool = False,
) -> list[dict]:
    """Write the schedule, public exposes and summaries to idx_agm."""
    rows = []
    for frame in [drop_implausible(schedule), public_exposes, summaries]:
        if not frame.empty:
            rows += frame.to_dict("records")

    if not rows:
        LOGGER.info("[UPSERT] Nothing to write")
        return []

    plans = plan_writes(rows, existing, listed, force)
    return apply_plans(client, plans, dry_run)


# --------------------------------------------------------------------------
# Pipeline
# --------------------------------------------------------------------------
def run(
    month: int = None,
    year: int = None,
    api: str = "openrouter",
    dry_run: bool = False,
    force: bool = False,
    skip_pdf: bool = False,
    skip_summary: bool = False,
    lookback: int = DEFAULT_LOOKBACK_DAYS,
    lookahead: int = DEFAULT_LOOKAHEAD_DAYS,
):
    load_dotenv(".env", override=True)

    session = build_session()
    client = supabase_client()
    state = load_state()

    months = months_to_check(month, year)
    today = date.today()
    days = (
        month_days(month, year or today.year)
        if month
        else [today + timedelta(days=offset) for offset in range(-lookback, lookahead + 1)]
    )

    LOGGER.info(f"[PIPELINE] Running for {months}")

    company_names = fetch_listed_symbols(client)
    listed = set(company_names)

    try:
        schedule, lookup = fetch_rups_schedule(session, state, months, days, skip_pdf=skip_pdf)
        public_exposes = fetch_public_expose(session, listed)

        symbols = sorted(
            {*(schedule["symbol"] if not schedule.empty else []),
             *(public_exposes["symbol"] if not public_exposes.empty else [])}
        )
        existing = fetch_existing_rows(client, symbols) if symbols else pd.DataFrame()

        summaries, written_by = fetch_minutes(
            session,
            state,
            months,
            schedule,
            existing,
            lookup,
            company_names,
            api=api,
            skip_summary=skip_summary,
        )

        # A summary can belong to a company the schedule did not mention
        if not summaries.empty:
            extra = sorted(set(summaries["symbol"]) - set(symbols))
            if extra:
                existing = pd.concat(
                    [existing, fetch_existing_rows(client, extra)], ignore_index=True
                )

        plans = upsert_idx_agm(
            client, schedule, public_exposes, summaries, existing, listed, dry_run, force
        )

        # A summary counts as handled once its row is in idx_agm; until then the
        # minutes file stays pending and is summarized again next run
        if not dry_run:
            for plan in plans:
                file_ids = written_by.get((plan["symbol"], plan["agm_date"]), [])
                for file_id in file_ids:
                    record_attempt(
                        state["minutes"],
                        file_id,
                        done=plan.get("result") == "ok",
                        note=None if plan.get("result") == "ok" else str(plan.get("result")),
                        meeting={"symbol": plan["symbol"], "agm_date": plan["agm_date"]},
                    )

    finally:
        if not dry_run:
            prune_state(state)
            save_state(state)

    LOGGER.info("[PIPELINE] Done")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "AGM pipeline: KSEI schedule, public exposes, KSEI minutes and the "
            "idx_agm upsert. Without --month it runs in daily mode and only reads "
            "and writes what is new."
        )
    )
    parser.add_argument("--month", "-m", type=int, help="Backfill a whole month")
    parser.add_argument("--year", "-y", type=int, help="Year for --month")
    parser.add_argument("--api", choices=["openrouter", "openai", "gemini"], default="openrouter")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show the write plan only. State is not saved, so anything summarized in a dry run is summarized again on the next real run",
    )
    parser.add_argument("--force", action="store_true", help="Write rows even if unchanged")
    parser.add_argument("--skip-pdf", action="store_true", help="Skip the announcement PDFs")
    parser.add_argument("--skip-summary", action="store_true", help="Skip the LLM summaries")
    parser.add_argument("--lookback", type=int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--lookahead", type=int, default=DEFAULT_LOOKAHEAD_DAYS)

    args = parser.parse_args()

    run(
        month=args.month,
        year=args.year,
        api=args.api,
        dry_run=args.dry_run,
        force=args.force,
        skip_pdf=args.skip_pdf,
        skip_summary=args.skip_summary,
        lookback=args.lookback,
        lookahead=args.lookahead,
    )
