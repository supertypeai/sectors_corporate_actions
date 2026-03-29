import re 


# ***** Regex patterns to clean agm_place ***** 
_PLATFORM_LABELS = {
    'zoom':  'Zoom Meeting',
    'teams': 'Teams Meeting',
    'webex': 'Webex Meeting',
    'ksei':  'E-RUPS KSEI',
}

# Virtual-only filler words — if the entire result consists only of these,
# the entry is blanked out.
_VIRTUAL_FILLER_WORDS = re.compile(
    r"""\b(?:
        online | daring | virtual | zoom | teams | webex | webinar |
        melalui | secara | dilakukan | sarana | media | elektronik |
        registration | link | pendaftaran | by | via | dengan |
        fasilitas | sistem | system | meeting | rapat | umum | pemegang | saham |
        tautan | ksei | egms | e\-?rups? | aplikasi | mengakses | electronic |
        general | easy | webinar\-?id | pass | passcode | password
    )\b""",
    re.VERBOSE | re.IGNORECASE
)

_ONLY_PUNCT = re.compile(r'^[\s,.:;/\-\(\)%@\d]+$')


# ***** Regex patterns to detect agm_place_desc *****
PUBLIC_EXPOSE_PATTERN = re.compile(
    r'public\s*expose|publik\s*ekspose|pubex',
    re.IGNORECASE
)


ONLINE_EVENT_URL_PATTERN  = re.compile(
    r'akses\.ksei|easy\.ksei|zoom\.us|teams\.microsoft|teams\.live|'
    r'meet\.google|webex|pubexlive\.idx|idxchannel\.com',
    re.IGNORECASE
)

ONLINE_KSEI_PHRASE_PATTERN = re.compile(
    r'(melalui|mengakses)\s+fasilitas\s+(electronic\s+general\s+meeting\s+system\s+ksei|easy\.ksei)',
    re.IGNORECASE
)

ONLINE_KEYWORD_PATTERN = re.compile(
    r'e-rups|erups|secara\s+elektronik|electronic\s+general\s+meeting|'
    r'secara\s+virtual|live\s+event|live\s+conference|'
    r'google\s*meet|microsoft\s*teams|ms\.?\s*teams|'
    r'virtual|webinar|daring|online|zoom',
    re.IGNORECASE
)

SUPPLEMENTARY_URL_PATTERN = re.compile(
    r'maps\.app\.goo\.gl|maps\.google\.com|'
    r'forms\.gle|forms\.office\.com|docs\.google\.com/forms|'
    r'tinyurl\.com',
    re.IGNORECASE
)

ONLINE_URL_FALLBACK_PATTERN = re.compile(r'https?\s*://', re.IGNORECASE)

ONLINE_OVERRIDE_PATTERN = re.compile(
    r'secara\s+(online|elektronik|virtual)\s+(bertempat\s+di|di\s+ruang|-)',
    re.IGNORECASE
)

ONSITE_KEYWORD_PATTERN = re.compile(
    r'jl\.|jalan|hotel|gedung|menara|plaza|tower|center|centre|'
    r'lantai|lt\.|ballroom|hall|auditorium|'
    r'rt\.|rw\.|kec\.|no\.|lokasi\s*:|'
    r'secara\s+fisik|fisik\s*:|'
    r'peserta\s+yang\s+(wajib|hadir|akan\s+hadir)|\bhadir\b|'
    r'kehadiran\s+terbatas|\bkantor\b|\bbuilding\b|\boffline\b|'
    r'dengan\s+tautan\s+registrasi',
    re.IGNORECASE
)

MAPS_URL_PATTERN = re.compile(
    r'maps\.app\.goo\.gl|maps\.google\.com',
    re.IGNORECASE
)

# Registration URLs (ambiguous alone, treated as Online when no physical context) 
REGISTRATION_URL_PATTERN = re.compile(
    r'forms\.gle|forms\.office\.com|docs\.google\.com/forms|tinyurl\.com',
    re.IGNORECASE
)

HYBRID_PHRASE_PATTERN = re.compile(
    r'dan/atau\s+melalui|atau\s+mengakses(\s+fasilitas)?|atau\s+melalui|elektronik\s*:|hybrid',
    re.IGNORECASE
)


def _is_virtual_remnant(s: str) -> bool:
    """Return True if s contains nothing meaningful after removing virtual filler tokens."""
    cleaned = _VIRTUAL_FILLER_WORDS.sub('', s)
    cleaned = cleaned.strip(' ,.:;/-()%@')
    return cleaned == '' or bool(_ONLY_PUNCT.match(cleaned))


def _detect_platform(s: str) -> str | None:
    """Return a platform label if the string contains a known meeting platform."""
    if re.search(r'(?i)\bteams\b|teams\.microsoft\.com', s):
        return _PLATFORM_LABELS['teams']
    if re.search(r'(?i)\bwebex\b|webex\.com', s):
        return _PLATFORM_LABELS['webex']
    if re.search(r'(?i)\bzoom\b|zoom\.us|zoom\s*webinar', s):
        return _PLATFORM_LABELS['zoom']
    if re.search(r'(?i)e-?rups?|ksei|easy\.ksei|mengakses\s+fasilitas\s+electronic', s):
        return _PLATFORM_LABELS['ksei']
    return None


def clean_agm_place(text: str) -> str:
    """
    Clean and normalize AGM (Annual General Meeting) place descriptions.

    Steps:
    1. Detect platform-only entries early → return label
    2. Detect Public Expose → return fixed label
    3. Strip markdown decoration, URLs, credentials, hex fragments
    4. Collapse whitespace
    5. Virtual remnant check → blank if nothing real remains
    6. Fix concatenated CamelCase / fused place words
    7. Fix ALL CAPS → Title Case
    8. Normalize punctuation & orphan symbols
    9. Remove trailing city/province/country suffix
    10. Fix spacing around punctuation
    11. Final cleanup + virtual remnant check
    """
    if not text or not text.strip():
        return ""

    s = text.strip()

    # --- 1. Public Expose: return fixed label immediately ---
    if re.search(r'(?i)(public\s+expose|expose\s+publik|paparan\s+publik)', s):
        return 'IDX Public Expose'

    # --- 2. E-RUPS KSEI: return fixed label immediately ---
    if re.search(r'(?i)(mengakses\s+fasilitas\s+electronic\s+general\s+meeting|e-?rups)', s):
        return 'E-RUPS KSEI'

    # --- 3. Strip markdown decoration, URLs, credentials ---

    # Replace known platform URLs with labels BEFORE any other URL stripping.
    # Use [^\s_]+ instead of \S+ so __ markdown terminators don't sneak into the match.
    # Strip markdown decoration first so __Https://...__ is normalised to " Https://... "
    # Collapse __Url__ markdown so the full URL is one unbroken token before matching
    # Greedily consume everything between __ markers so path fragments aren't left behind
    s = re.sub(r'_{1,2}(https?://.*?)_{1,2}', r'\1', s, flags=re.IGNORECASE | re.DOTALL)
    # Strip remaining loose underscores/asterisks
    s = re.sub(r'_{1,2}|\*{1,2}', ' ', s)

    s = re.sub(
        r'(?i)https?://\S+?(?=\s|$)',   # greedy-safe: stop at whitespace
        lambda m: (
            'Teams Meeting'  if re.search(r'teams\.microsoft\.com', m.group(), re.I) else
            'Webex Meeting'  if re.search(r'webex\.com',            m.group(), re.I) else
            'Zoom Meeting'   if re.search(r'zoom\.us',              m.group(), re.I) else
            'E-RUPS KSEI'    if re.search(r'ksei',                  m.group(), re.I) else
            ''
        ),
        s
    )
    # Strip any other remaining URLs
    s = re.sub(r'https?://\S+', '', s, flags=re.IGNORECASE)

    # Strip "Tautan [Pendaftaran]: ...", "Registration Link: ...", "Dalam tautan ..."
    s = re.sub(r'(?i)tautan\s*(?:pendaftaran)?\s*[:\-]?\s*\S*', '', s)
    s = re.sub(r'(?i)dalam\s*tautan\s*\S*', '', s)
    s = re.sub(r'(?i)registration\s*link\s*[:\-]?\s*\S*', '', s)

    # Strip Zoom/Webinar ID and Password (do BEFORE URL strip already done above,
    # but also catches patterns in plain text like "Webinar-ID : 857 5202 6256")
    s = re.sub(r'(?i)(webinar[-\s]?id|meeting\s+id|id\s+meeting)\s*[:\-]?\s*[\d\s]{6,25}', '', s)
    s = re.sub(r'(?i)(password|passcode|pass|kata\s+sandi)\s*[:\-]?\s*\S+', '', s)

    # Deduplicate consecutive platform labels e.g. "Zoom Meeting Zoom Meeting"
    for label in ('Zoom Meeting', 'Teams Meeting', 'Webex Meeting', 'E-RUPS KSEI'):
        s = re.sub(rf'({re.escape(label)})\s+\1', r'\1', s, flags=re.IGNORECASE)

    # Strip stray parenthetical platform tags like (?Easy.Ksei?) or (?Ksei?)
    s = re.sub(r'\([\?!\s]*[A-Za-z0-9\.\?!\s]{1,30}[\?!\s]*\)', '', s)

    # Strip orphan hex/encoded fragments left after URL removal
    # e.g. "Mgu1Zje0..." or "%7B%22Tid%22" or "05A84485E2Ca92"
    s = re.sub(r'\b[0-9A-Fa-f]{8,}\b', '', s)          # hex blobs
    s = re.sub(r'(?:%[0-9A-Fa-f]{2})+\S*', '', s)       # percent-encoded sequences
    s = re.sub(r'\b\w*%\w+\b', '', s)                   # any token containing %

    # --- 4. Collapse whitespace & newlines ---
    s = re.sub(r'[\r\n\t]+', ' ', s)
    s = re.sub(r'  +', ' ', s).strip()

    # --- 5. Early virtual remnant check ---
    if _is_virtual_remnant(s):
        # But if a platform label survived, return just that
        platform = _detect_platform(s)
        return platform if platform else ""

    # --- 6. Fix concatenated words ---
    s = re.sub(r'([a-z])([A-Z])', r'\1 \2', s)
    _PLACE_SPLITS = [
        (r'(?i)(sanur)(denpasar)',          lambda m: m.group(1) + ', ' + m.group(2).title()),
        (r'(?i)(kuta)(badung|denpasar)',    lambda m: m.group(1) + ', ' + m.group(2).title()),
        (r'(?i)(seminyak)(kuta|badung)',    lambda m: m.group(1) + ', ' + m.group(2).title()),
        (r'(?i)(\w{4,})(kel\.)',            r'\1, \2'),
        (r'(?i)(\w{4,})(kec\.)',            r'\1, \2'),
        (r'(?i)(\w{4,})(daerah)',           r'\1 \2'),
        (r'(?i)(\w{4,})(kota)',             r'\1, \2'),
    ]
    for pattern, repl in _PLACE_SPLITS:
        s = re.sub(pattern, repl, s)

    # --- 7. Fix ALL CAPS tokens → Title Case ---
    def fix_token_case(token):
        known_upper = {'DKI', 'RT', 'RW', 'JL', 'KAV', 'NO', 'GN', 'KEL', 'KEC',
                       'ADM', 'II', 'III', 'IV', 'VI', 'VII', 'VIII', 'IX', 'XI', 'XII'}
        if token.upper() in known_upper:
            return token.upper()
        if not token.isupper():
            return token
        if len(token) > 3:
            return token.title()
        return token

    s = ' '.join(fix_token_case(t) for t in s.split())

    # --- 8. Normalize punctuation & orphan symbols ---
    s = re.sub(r'\([\?!\s]*[A-Za-z0-9\.\?!\s]{1,30}[\?!\s]*\)', '', s)
    s = re.sub(r'\?([A-Za-z][A-Za-z0-9\s]{1,40}?)\?', r'\1', s)
    s = re.sub(r',\s*,', ',', s)
    s = re.sub(r'\.\s*\.', '.', s)
    s = re.sub(r'[\s:;,\-]+$', '', s)
    s = re.sub(r'^[\s:;,\-]+', '', s)
    s = re.sub(r'  +', ' ', s)

    # --- 9. Remove trailing redundant city/province/country suffix ---
    s = re.sub(
        r',?\s*Kota\s+Adm\.?\s+Jakarta\s+\w+\s+DKI\.?\s+Jakarta\s+Indonesia\s*$',
        '', s, flags=re.IGNORECASE
    ).strip()
    s = re.sub(
        r',?\s*[-–]?\s*(DKI\.?\s+Jakarta\s+)?Indonesia\s*$',
        '', s, flags=re.IGNORECASE
    ).strip()
    s = re.sub(
        r',?\s*Kota\s+\w+\s+\w[\w\s]{0,30}$',
        '', s, flags=re.IGNORECASE
    ).strip()
    s = re.sub(r'\s*[-–]\s*Indonesia\s*$', '', s, flags=re.IGNORECASE).strip()

    # --- 10. Fix spacing around punctuation ---
    s = re.sub(r'\s+,', ',', s)
    s = re.sub(r',([^\s])', r', \1', s)
    s = re.sub(r'\s+\.', '.', s)
    s = re.sub(r'\.([A-Za-z])', r'. \1', s)

    # --- 11. Final cleanup + virtual remnant check ---
    s = re.sub(r'  +', ' ', s).strip(' ,.')
    if _is_virtual_remnant(s):
        platform = _detect_platform(s)
        return platform if platform else ""

    return s


def is_public_expose(agm_place: str) -> bool:
    if not agm_place:
        return False
   
    return bool(PUBLIC_EXPOSE_PATTERN.search(agm_place))


def is_online(agm_place: str) -> bool:
    if not agm_place:
        return False

    if ONLINE_EVENT_URL_PATTERN.search(agm_place):
        return True

    if ONLINE_KSEI_PHRASE_PATTERN.search(agm_place):
        return True

    if ONLINE_KEYWORD_PATTERN.search(agm_place):
        return True

    has_any_url = bool(ONLINE_URL_FALLBACK_PATTERN.search(agm_place))
    has_maps_url = bool(MAPS_URL_PATTERN.search(agm_place))
    has_registration_url = bool(REGISTRATION_URL_PATTERN.search(agm_place))
    has_physical_context = bool(ONSITE_KEYWORD_PATTERN.search(agm_place) or has_maps_url)

    # registration URL alone with no physical context → Online
    if has_any_url and has_registration_url and not has_physical_context:
        return True

    # any other URL that is not a maps or registration URL → Online
    if has_any_url and not has_registration_url and not has_maps_url:
        return True

    return False


def is_onsite(agm_place: str) -> bool:
    if not agm_place:
        return False
    
    return bool(
        ONSITE_KEYWORD_PATTERN.search(agm_place) or 
        MAPS_URL_PATTERN.search(agm_place)
    )


def is_hybrid(agm_place: str) -> bool:
    if not agm_place:
        return False
    
    explicit_hybrid = bool(HYBRID_PHRASE_PATTERN.search(agm_place))
    implicit_hybrid = is_onsite(agm_place) and is_online(agm_place)
    
    return explicit_hybrid or implicit_hybrid


def detect_agm_place_desc(agm_place: str) -> str: 
    if is_public_expose(agm_place):
        return 'Public expose'

    elif 'dibatalkan' in (agm_place or '').lower():
        return 'Dibatalkan'

    elif ONLINE_OVERRIDE_PATTERN.search(agm_place or ''):
        return 'Online'

    elif is_hybrid(agm_place):
        return 'Hybrid'

    elif is_online(agm_place):
        return 'Online'

    elif is_onsite(agm_place):
        return 'Onsite'

    elif len(agm_place) > 5:
        return 'Onsite'

    else:
        return None
