import hmac, hashlib, os, re, json, sys, random
from difflib import SequenceMatcher

import psycopg
import requests
from flask import Flask, request, abort

app = Flask(__name__)

VERIFY_TOKEN = os.environ["VERIFY_TOKEN"]
APP_SECRET = os.environ["APP_SECRET"]
IG_TOKEN = os.environ.get("IG_TOKEN", "")
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
GOOGLE_KEY = os.environ.get("GOOGLE_MAPS_KEY", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")

IG_API = "https://graph.instagram.com/v23.0"  # use the version shown in your dashboard
SHARE_TYPES = {"ig_reel", "ig_post"}

# Google Maps URL that opens the exact Place ID.
MAPS_LINK = (
    "https://www.google.com/maps/search/?api=1&query=Google&query_place_id="
)

# Total tries for a reel whose processing errored (API outage, timeout, ...).
# Reels that were processed successfully (place found OR no place) are never re-run.
MAX_ATTEMPTS = 2

# Spend and storage controls
MAX_SHARES_PER_USER_PER_DAY = 50
NEGATIVE_RETENTION_DAYS = 30
MESSAGE_RETENTION_HOURS = 48
PLACE_CACHE_RETENTION_DAYS = 180
PRUNE_PROBABILITY = 0.01


def log(*args):
    print(*args, file=sys.stderr, flush=True)


# ---------- database ----------

def db():
    # prepare_threshold=None avoids prepared-statement errors behind a pooler
    return psycopg.connect(
        DATABASE_URL,
        connect_timeout=5,
        prepare_threshold=None,
    )


def claim_message(mid):
    """True the first time a message id is seen (guards against Meta retries).

    Stores a 128-bit MD5 hash of the id for deduplication only.
    """
    h = hashlib.md5(mid.encode()).hexdigest()
    with db() as conn:
        cur = conn.execute(
            """
            INSERT INTO processed_messages (mid_hash)
            VALUES (%s::uuid)
            ON CONFLICT DO NOTHING
            """,
            (h,),
        )
        return cur.rowcount == 1


def reel_key(payload):
    """Stable id shared by everyone who sends the same reel."""
    m = re.search(
        r"instagram\.com/(?:reels?|p)/([A-Za-z0-9_-]+)",
        payload.get("url") or "",
    )
    return m.group(1) if m else payload.get("reel_video_id")


def claim_reel(key):
    """Atomically claim a reel for processing.

    True means THIS request should do the work.

    False means it was already processed, is being processed right now,
    or has used up its retries. Failed reels can be retried after one hour;
    abandoned processing claims can be reclaimed after five minutes.
    """
    with db() as conn:
        row = conn.execute(
            """
            INSERT INTO reels (reel_key, status)
            VALUES (%s, 'processing')
            ON CONFLICT (reel_key) DO UPDATE
               SET status = 'processing',
                   attempts = reels.attempts + 1,
                   updated_at = now()
             WHERE reels.attempts < %s
               AND (
                    (reels.status = 'failed'
                     AND reels.updated_at < now() - interval '1 hour')
                 OR (reels.status = 'processing'
                     AND reels.updated_at < now() - interval '5 minutes')
               )
            RETURNING reel_key
            """,
            (key, MAX_ATTEMPTS),
        ).fetchone()

        return row is not None


def upsert_place(place):
    """Store/update the canonical Google Place record."""
    place_id = place["id"]
    name = place["displayName"]["text"]
    address = place.get("formattedAddress")

    # Google Maps URLs using the Place ID are stable and avoid ambiguous searches.
    maps_url = f"{MAPS_LINK}{place_id}"

    # Keep the extracted city separately because Google may not always provide
    # a clean city field in formattedAddress.
    city = None

    with db() as conn:
        conn.execute(
            """
            INSERT INTO places (
                place_id, name, city, formatted_address, maps_url
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (place_id) DO UPDATE SET
                name = EXCLUDED.name,
                city = COALESCE(EXCLUDED.city, places.city),
                formatted_address = EXCLUDED.formatted_address,
                maps_url = EXCLUDED.maps_url,
                updated_at = now()
            """,
            (place_id, name, city, address, maps_url),
        )


def finish_reel(key, saved, unverified):
    """Persist the reel's resolved places and mark processing complete."""
    with db() as conn:
        for p, m in saved:
            place_id = m["id"]

            maps_url = f"{MAPS_LINK}{place_id}"

            conn.execute(
                """
                INSERT INTO places (
                    place_id, name, city, formatted_address, maps_url
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (place_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    city = COALESCE(EXCLUDED.city, places.city),
                    formatted_address = EXCLUDED.formatted_address,
                    maps_url = EXCLUDED.maps_url,
                    updated_at = now()
                """,
                (
                    place_id,
                    m["displayName"]["text"],
                    p.get("city"),
                    m.get("formattedAddress"),
                    maps_url,
                ),
            )

            conn.execute(
                """
                INSERT INTO reel_places (
                    reel_key, place_id, extracted_name, extracted_city, match_status
                )
                VALUES (%s, %s, %s, %s, 'matched')
                ON CONFLICT (reel_key, place_id) DO NOTHING
                """,
                (key, place_id, p["name"], p.get("city")),
            )

        for p in unverified:
            conn.execute(
                """
                INSERT INTO reel_places (
                    reel_key, place_id, extracted_name, extracted_city, match_status
                )
                VALUES (%s, NULL, %s, %s, 'unverified')
                """,
                (key, p["name"], p.get("city")),
            )

        status = "done" if saved else "no_place"

        conn.execute(
            """
            UPDATE reels
            SET status = %s, updated_at = now()
            WHERE reel_key = %s
            """,
            (status, key),
        )


def fail_reel(key):
    with db() as conn:
        conn.execute(
            """
            UPDATE reels
            SET status = 'failed', updated_at = now()
            WHERE reel_key = %s
            """,
            (key,),
        )


def link_user(igsid, key):
    """Associate a user with a reel and all matched places from that reel.

    If this reel was already processed for another user, this does NOT run
    Anthropic or Google Places again. It simply reuses the existing place IDs.
    """
    with db() as conn:
        conn.execute(
            """
            INSERT INTO users (igsid)
            VALUES (%s)
            ON CONFLICT DO NOTHING
            """,
            (igsid,),
        )

        conn.execute(
            """
            INSERT INTO user_reels (igsid, reel_key)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
            """,
            (igsid, key),
        )

        # Populate this user's saved-place collection from the already-resolved
        # places attached to the reel.
        conn.execute(
            """
            INSERT INTO user_places (igsid, place_id)
            SELECT %s, rp.place_id
            FROM reel_places rp
            WHERE rp.reel_key = %s
              AND rp.match_status = 'matched'
              AND rp.place_id IS NOT NULL
            ON CONFLICT DO NOTHING
            """,
            (igsid, key),
        )


def reel_state(key):
    with db() as conn:
        row = conn.execute(
            """
            SELECT status
            FROM reels
            WHERE reel_key = %s
            """,
            (key,),
        ).fetchone()

        if not row:
            return None, []

        status = row[0]

        rows = conn.execute(
            """
            SELECT
                rp.place_id,
                rp.extracted_name,
                rp.extracted_city,
                rp.match_status
            FROM reel_places rp
            WHERE rp.reel_key = %s
            ORDER BY rp.id
            """,
            (key,),
        ).fetchall()

    return status, rows


def user_places(igsid):
    """Return the user's deduplicated saved-place collection."""
    with db() as conn:
        return conn.execute(
            """
            SELECT
                p.place_id,
                p.name,
                p.city,
                p.formatted_address,
                p.maps_url,
                up.created_at
            FROM user_places up
            JOIN places p ON p.place_id = up.place_id
            WHERE up.igsid = %s
            ORDER BY up.created_at DESC
            """,
            (igsid,),
        ).fetchall()


def over_daily_limit(igsid):
    with db() as conn:
        n = conn.execute(
            """
            SELECT count(*)
            FROM user_reels
            WHERE igsid = %s
              AND created_at > now() - interval '1 day'
            """,
            (igsid,),
        ).fetchone()[0]

    return n >= MAX_SHARES_PER_USER_PER_DAY


def prune():
    """Delete expired rows.

    Positive reel results remain permanently because they are what save
    Anthropic and Google Places API spend. Negative/failed reel results
    expire after NEGATIVE_RETENTION_DAYS.

    User-place records are independent of reel retention, so a place that was
    successfully saved remains in the user's collection even after an old
    non-positive reel is pruned.
    """
    with db() as conn:
        conn.execute(
            """
            DELETE FROM processed_messages
            WHERE created_at < now() - make_interval(hours => %s)
            """,
            (MESSAGE_RETENTION_HOURS,),
        )

        conn.execute(
            """
            DELETE FROM reels
            WHERE status <> 'done'
              AND updated_at < now() - make_interval(days => %s)
            """,
            (NEGATIVE_RETENTION_DAYS,),
        )

        # place_cache is only a lookup optimization. The canonical place data
        # in `places` is retained because it may be referenced by user_places.
        conn.execute(
            """
            DELETE FROM place_cache
            WHERE updated_at < now() - make_interval(days => %s)
            """,
            (PLACE_CACHE_RETENTION_DAYS,),
        )


def maybe_prune():
    """Cheap probabilistic cleanup so no cron job is needed."""
    if random.random() < PRUNE_PROBABILITY:
        try:
            prune()
        except Exception as e:
            log("prune failed:", repr(e))


# ---------- routes ----------

@app.get("/")
def health():
    return "ok", 200


@app.get("/webhook")
def verify():
    if (
        request.args.get("hub.mode") == "subscribe"
        and request.args.get("hub.verify_token") == VERIFY_TOKEN.strip()
    ):
        return request.args["hub.challenge"], 200

    abort(403)


@app.get("/privacy")
def privacy():
    return """<h1>Privacy Policy</h1>
    <p>This app receives Instagram messages you send to our account, extracts place names,
    and saves them to your list. We store your Instagram-scoped ID and saved places.
    To delete your data, message us or email you@example.com.</p>""", 200


# ---------- place extraction ----------

def lookup_handle(handle):
    """Public Business/Creator profile info for an @mentioned account."""
    try:
        r = requests.get(
            f"{IG_API}/me",
            params={
                "fields": (
                    f"business_discovery.username({handle})"
                    "{{name,biography,website}}"
                ),
                "access_token": IG_TOKEN,
            },
            timeout=8,
        )
        return r.json().get("business_discovery")
    except Exception as e:
        log("lookup_handle failed:", handle, repr(e))
        return None


GATE_SYSTEM = (
    "Answer only YES or NO. Does this Instagram caption name or point to a specific "
    "physical place (restaurant, cafe, bar, shop, attraction, hotel)? Products, recipes, "
    "general tips, memes and ads are NO."
)

EXTRACT_SYSTEM = """You identify real-world places featured in an Instagram reel from its caption.
You get the caption, hashtags, and profile info for accounts it @mentions.
Use web search if needed to identify a venue from a handle or partial name. Do not invent places.
List EVERY distinct physical venue (restaurants, cafes, bars, shops, attractions).
Reply ONLY with a JSON array:
[{"name": str, "city": str|null, "neighborhood": str|null,
  "confidence": 0-1, "evidence": "mention"|"caption"|"hashtag"|"search"}]
Return [] if the reel does not feature a specific place (for example a product or general tip)."""


def worth_extracting(caption):
    """Cheap pre-check so non-place captions never reach the expensive extraction call."""
    if "📍" in caption:
        return True

    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 5,
                "system": GATE_SYSTEM,
                "messages": [{"role": "user", "content": caption}],
            },
            timeout=10,
        )

        if r.status_code != 200:
            log("gate error:", r.status_code, r.text[:200])
            return True  # fail open so real places aren't dropped

        text = "".join(
            b.get("text", "")
            for b in r.json().get("content", [])
        )

        return text.strip().upper().startswith("Y")

    except Exception as e:
        log("gate failed:", repr(e))
        return True


def extract_places(caption):
    """Return extracted places.

    [] means no place. API/parse errors raise so the reel is marked failed
    rather than being cached as a successful no-place result.
    """
    caption = caption.strip()[:2000]

    if not caption or not worth_extracting(caption):
        return []

    handles = list(
        dict.fromkeys(
            re.findall(r"@([A-Za-z0-9._]{1,30})", caption)
        )
    )[:8]

    hashtags = re.findall(r"#(\w+)", caption)
    profiles = {h: lookup_handle(h) for h in handles}

    prompt = (
        f"Caption:\n{caption}\n\n"
        f"Hashtags: {hashtags}\n\n"
        f"Mentioned profiles: {json.dumps(profiles)}"
    )

    body = {
        "model": "claude-sonnet-5",
        "max_tokens": 1500,
        "system": EXTRACT_SYSTEM,
        "messages": [{"role": "user", "content": prompt}],
    }

    if any(v is None for v in profiles.values()):
        body["tools"] = [
            {
                "type": "web_search_20250305",
                "name": "web_search",
                "max_uses": 3,
            }
        ]

    r = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": ANTHROPIC_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json=body,
        timeout=45,
    )

    if r.status_code != 200:
        raise RuntimeError(
            f"anthropic {r.status_code}: {r.text[:300]}"
        )

    text = "".join(
        b.get("text", "")
        for b in r.json().get("content", [])
        if b.get("type") == "text"
    )

    start, end = text.find("["), text.rfind("]")

    if start == -1 or end == -1:
        raise ValueError(
            f"no JSON array in model output: {text[:300]}"
        )

    return json.loads(text[start:end + 1])


def place_query_key(p):
    parts = [
        p.get("name"),
        p.get("neighborhood"),
        p.get("city"),
    ]

    text = re.sub(
        r"[^a-z0-9 ]+",
        "",
        " ".join(x for x in parts if x).lower(),
    )

    return " ".join(text.split())


def resolve(p):
    """Resolve an extracted place to a canonical Google Place.

    First checks the local query cache. Only if there is no cached query
    does it call Google Places.
    """
    name = (p.get("name") or "").strip()

    if not name:
        return None

    qkey = place_query_key(p)

    # Fast path: same extracted place/query has already been resolved.
    with db() as conn:
        row = conn.execute(
            """
            SELECT
                pc.place_id,
                pl.name,
                pl.city,
                pl.formatted_address,
                pl.maps_url
            FROM place_cache pc
            JOIN places pl ON pl.place_id = pc.place_id
            WHERE pc.query_key = %s
            """,
            (qkey,),
        ).fetchone()

    if row:
        place_id, cached_name, city, address, maps_url = row

        return {
            "id": place_id,
            "displayName": {"text": cached_name},
            "formattedAddress": address,
            "maps_url": maps_url,
        }

    # No cached query: spend a Google Places lookup.
    query = " ".join(
        x for x in [
            name,
            p.get("neighborhood"),
            p.get("city"),
        ]
        if x
    )

    r = requests.post(
        "https://places.googleapis.com/v1/places:searchText",
        headers={
            "X-Goog-Api-Key": GOOGLE_KEY,
            "X-Goog-FieldMask": (
                "places.id,places.displayName,places.formattedAddress"
            ),
        },
        json={
            "textQuery": query,
            "maxResultCount": 3,
        },
        timeout=8,
    )

    if r.status_code != 200:
        raise RuntimeError(
            f"places {r.status_code}: {r.text[:300]}"
        )

    for pl in r.json().get("places", []):
        sim = SequenceMatcher(
            None,
            name.lower(),
            pl["displayName"]["text"].lower(),
        ).ratio()

        if sim >= 0.6:
            place_id = pl["id"]
            maps_url = f"{MAPS_LINK}{place_id}"

            # Store the canonical place globally.
            with db() as conn:
                conn.execute(
                    """
                    INSERT INTO places (
                        place_id, name, city, formatted_address, maps_url
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (place_id) DO UPDATE SET
                        name = EXCLUDED.name,
                        formatted_address = EXCLUDED.formatted_address,
                        maps_url = EXCLUDED.maps_url,
                        updated_at = now()
                    """,
                    (
                        place_id,
                        pl["displayName"]["text"],
                        p.get("city"),
                        pl.get("formattedAddress"),
                        maps_url,
                    ),
                )

                # Store the extraction query -> Place ID mapping.
                conn.execute(
                    """
                    INSERT INTO place_cache (query_key, place_id)
                    VALUES (%s, %s)
                    ON CONFLICT (query_key)
                    DO UPDATE SET
                        place_id = EXCLUDED.place_id,
                        updated_at = now()
                    """,
                    (qkey, place_id),
                )

            return pl

    return None


# ---------- messaging ----------

def send_dm(igsid, text):
    r = requests.post(
        f"{IG_API}/me/messages",
        headers={"Authorization": f"Bearer {IG_TOKEN}"},
        json={
            "recipient": {"id": igsid},
            "message": {"text": text},
        },
        timeout=8,
    )

    if r.status_code != 200:
        log("send_dm failed:", r.status_code, r.text[:300])


def build_reply(status, rows):
    if status == "processing":
        return (
            "That reel is already being processed. "
            "It will appear in your list shortly."
        )

    if status == "failed":
        return (
            "I couldn't process that reel right now. "
            "The link is kept in your list."
        )

    matched = [r for r in rows if r[3] == "matched"]
    unverified = [r for r in rows if r[3] == "unverified"]

    if matched:
        lines = [
            (
                f"{i}. {name}{', ' + city if city else ''}\n"
                f"{MAPS_LINK}{pid}"
            )
            for i, (pid, name, city, _) in enumerate(matched, 1)
        ]

        msg = (
            f"Saved {len(matched)} place(s) from this reel:\n"
            + "\n".join(lines)
        )
    else:
        msg = (
            "I didn't find a specific place in that reel. "
            "I kept the link in your unresolved list."
        )

    if unverified:
        msg += (
            "\nAlso spotted, but not matched on Maps: "
            + ", ".join(r[1] for r in unverified)
        )

    return msg


def process_share(sender, att):
    payload = att.get("payload") or {}
    key = reel_key(payload)

    if not key:
        log("share without a usable reel key")
        return

    if over_daily_limit(sender):
        send_dm(
            sender,
            "You've reached today's limit for saved reels. "
            "Try again tomorrow.",
        )
        return

    # If this returns True, this request is responsible for doing the
    # expensive extraction/Google resolution.
    if claim_reel(key):
        try:
            found = extract_places(payload.get("title") or "")

            saved = []
            unverified = []
            seen = set()

            for p in found[:12]:
                if not (p.get("name") or "").strip():
                    continue

                m = resolve(p)

                if m and m["id"] not in seen:
                    seen.add(m["id"])
                    saved.append((p, m))

                elif (
                    not m
                    and p.get("confidence", 0) >= 0.5
                ):
                    unverified.append(p)

            finish_reel(key, saved, unverified)

        except Exception as e:
            log("processing failed:", repr(e))
            fail_reel(key)

    # IMPORTANT:
    # This happens whether this request processed the reel or another
    # request processed it previously. For an existing reel, it simply
    # attaches the existing matched places to this user.
    link_user(sender, key)

    status, rows = reel_state(key)
    send_dm(sender, build_reply(status, rows))


def handle_event(ev):
    msg = ev.get("message")

    if not msg or msg.get("is_echo"):
        return

    mid = msg.get("mid")

    if mid:
        try:
            if not claim_message(mid):
                return
        except Exception as e:
            log("dedup check failed:", repr(e))  # fail open

    sender = ev["sender"]["id"]

    for att in msg.get("attachments", []):
        if att.get("type") in SHARE_TYPES:
            process_share(sender, att)


# ---------- webhook ----------

@app.post("/webhook")
def receive():
    raw = request.get_data()

    sig = request.headers.get("X-Hub-Signature-256", "")

    expected = (
        "sha256="
        + hmac.new(
            APP_SECRET.strip().encode(),
            raw,
            hashlib.sha256,
        ).hexdigest()
    )

    valid = hmac.compare_digest(sig, expected)

    log(
        json.dumps(
            {
                "valid_sig": valid,
                "sig_present": bool(sig),
                "bytes": len(raw),
            }
        )
    )

    if not valid:
        abort(403)

    body = request.get_json(silent=True) or {}

    for entry in body.get("entry", []):
        for ev in entry.get("messaging", []):
            try:
                handle_event(ev)
            except Exception as e:
                log("handler error:", repr(e))

    maybe_prune()

    return "ok", 200
