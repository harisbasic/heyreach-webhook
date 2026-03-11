"""
HeyReach → Attio Webhook Bridge

Deployed on Railway. Receives HeyReach webhook events and updates
Attio CRM pipeline stages accordingly.

Background conversation sync runs every 10 minutes, polling HeyReach
conversations API to catch replies and status changes that webhooks miss
(manual Unibox messages, leads not in Attio when webhook fired, downtime).

Endpoints:
  POST /webhook/reply         — HeyReach MESSAGE_REPLY_RECEIVED
  POST /webhook/connection    — HeyReach CONNECTION_REQUEST_ACCEPTED
  POST /webhook/message-sent  — HeyReach MESSAGE_SENT
  POST /sync                  — Pull all HeyReach conversations and sync to Attio
  GET  /health                — Health check (includes last sync status)
"""

import os
import json
import logging
import threading
import time
import re
import unicodedata
import socket
from datetime import datetime, timezone
from flask import Flask, request, jsonify
import requests
import dns.resolver

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Config from environment ---
ATTIO_API_KEY = os.environ["ATTIO_API_KEY"]
HEYREACH_API_KEY = os.environ["HEYREACH_API_KEY"]
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")

# Attio IDs
LIST_ID = "2f66cbc4-d297-4b89-a057-82f3cf84d2bb"  # ATLAS Pipeline

STAGES = {
    "new_lead": "686a4b91-78bb-4716-bcfb-9bf447d7f70d",
    "enriched": "f82f10af-3aa9-40ab-b41f-1cf49c3838c1",
    "scored_tier1": "fefae55c-2e42-4713-8a2c-d884d0bf4eeb",
    "scored_tier2": "01954b8a-4b7d-436a-ab9b-29fa4fed47a0",
    "in_sequence": "8aa24f57-9409-4a46-8d3b-d1f2cfcd044e",
    "replied": "3138f360-8c5f-4079-8039-32d454e2b107",
    "meeting_booked": "258126a0-9de9-4322-8f53-027418a4ba7a",
    "disqualified": "f19b31ac-9efb-450a-9f42-2975ac3cedfc",
}

ATTIO_HEADERS = {
    "Authorization": f"Bearer {ATTIO_API_KEY}",
    "Content-Type": "application/json",
}

HEYREACH_HEADERS = {
    "X-API-KEY": HEYREACH_API_KEY,
    "Content-Type": "application/json",
}

HANNA_ACCOUNT_ID = 140072
SYNC_INTERVAL = int(os.environ.get("SYNC_INTERVAL", "600"))  # seconds, default 10 min
EMAIL_ENRICHMENT_INTERVAL = int(os.environ.get("EMAIL_ENRICHMENT_INTERVAL", "1800"))  # default 30 min
ZEROBOUNCE_API_KEY = os.environ.get("ZEROBOUNCE_API_KEY", "")

# Track sync state for health endpoint
last_sync = {"time": None, "status": None, "updated": 0, "total": 0}
last_email_enrichment = {"time": None, "status": None, "found": 0, "processed": 0}

# Researched company → domain mapping (verified correct domains)
DOMAIN_MAP = {
    "Göteborg Energi": "goteborgenergi.se",
    "GÖTEBORG ENERGI NÄT AB": "goteborgenergi.se",
    "Göteborg Energi Nät AB": "goteborgenergi.se",
    "Göteborg Energi AB": "goteborgenergi.se",
    "Göteborg Energi Elnät": "goteborgenergi.se",
    "Göteborgenergi AB": "goteborgenergi.se",
    "Goteborg Energi": "goteborgenergi.se",
    "Jämtkraft AB": "jamtkraft.se",
    "Jönköping Energi": "jonkopingenergi.se",
    "Jönköping Energi AB": "jonkopingenergi.se",
    "Jönköping Energi Nät AB": "jonkopingenergi.se",
    "Gävle Energi AB": "gavleenergi.se",
    "Mälarenergi": "malarenergi.se",
    "Mälarenergi AB": "malarenergi.se",
    "Mälarenergi Elnät AB": "malarenergi.se",
    "Mälarenergi Vatten AB": "malarenergi.se",
    "MÄLARENERGI ELNÄT AB": "malarenergi.se",
    "Karlstads Energi AB": "karlstadsenergi.se",
    "Affärsverken Karlskrona AB": "affarsverken.se",
    "Affärsverken AB": "affarsverken.se",
    "Eskilstuna Strängnäs Energi och Miljö AB": "esem.se",
    "Skellefteå Kraft": "skekraft.se",
    "Skellefteå Kraft AB": "skekraft.se",
    "SKELLEFTEÅ KRAFT ELNÄT AB": "skekraft.se",
    "Borås Energi och Miljö AB": "borasem.se",
    "Uddevalla Energi AB": "uddevallaenergi.se",
    "Alingsås Energi": "alingsasenergi.se",
    "Dala Energi AB": "dalaenergi.se",
    "Halmstads Energi och Miljö AB": "hem.se",
    "HALMSTADS ENERGI OCH MILJÖ NÄT AB": "hem.se",
    "Luleå Energi": "luleaenergi.se",
    "Umeå Energi AB": "umeaenergi.se",
    "Umeå Energi Elnät AB": "umeaenergi.se",
    "Energi & Driftteknik i Sundsvall AB": "ed-teknik.se",
    "ENERGI & DRIFTTEKNIK I SUNDSVALL AB": "ed-teknik.se",
    "Tekniska verken i Linköping AB": "tekniskaverken.se",
    "Tekniska verken i Linkoping AB": "tekniskaverken.se",
    "Kraftringen": "kraftringen.se",
    "Kraftringen Elförsäljning AB": "kraftringen.se",
    "Sundsvall Energi": "sundsvallenergi.se",
    "Trollhättan Energi AB": "trollhattanenergi.se",
    "Region Kalmar län": "regionkalmar.se",
    "Region Jönköpings län": "rjl.se",
    "Region Jämtland Härjedalen": "regionjh.se",
    "Östersunds kommun": "ostersund.se",
    "Arvika Fjärrvärme AB, Arvika Kraft AB": "teknikivast.se",
    "ONE Nordic ES AB": "one-nordic.se",
    "KALMAR ENERGI VÄRME AB": "kalmarenergi.se",
    "Skånska Energi": "skanska-energi.se",
    "Eksjö Energi Elit AB": "eksjoenergi.se",
    "Varberg Energi": "varbergenergi.se",
    "Härnösand Energi & Miljö AB, HEMAB": "hemab.se",
}

EMAIL_PATTERNS = [
    "{first}.{last}@{domain}",
    "{f}{last}@{domain}",
    "{first}{last}@{domain}",
    "{f}.{last}@{domain}",
]

# --- Attio helpers ---

def attio_find_person(first_name, last_name):
    """Find a person in Attio by name. Returns record_id or None."""
    resp = requests.post(
        "https://api.attio.com/v2/objects/people/records/query",
        headers=ATTIO_HEADERS,
        json={
            "filter": {"name": {"first_name": first_name, "last_name": last_name}},
            "limit": 5,
        },
    )
    resp.raise_for_status()
    people = resp.json().get("data", [])

    for p in people:
        name_vals = p.get("values", {}).get("name", [])
        if name_vals:
            fn = name_vals[0].get("first_name", "").lower()
            ln = name_vals[0].get("last_name", "").lower()
            if fn == first_name.lower() and ln == last_name.lower():
                return p["id"]["record_id"]

    # Fallback: take first result
    if people:
        return people[0]["id"]["record_id"]
    return None


def attio_find_person_by_linkedin(linkedin_url):
    """Find a person in Attio by LinkedIn URL. Returns record_id or None."""
    resp = requests.post(
        "https://api.attio.com/v2/objects/people/records/query",
        headers=ATTIO_HEADERS,
        json={
            "filter": {"linkedin": {"contains": linkedin_url.rstrip("/").split("/in/")[-1]}},
            "limit": 5,
        },
    )
    resp.raise_for_status()
    people = resp.json().get("data", [])
    if people:
        return people[0]["id"]["record_id"]
    return None


def attio_get_pipeline_entry(record_id):
    """Find a person's ATLAS Pipeline entry. Returns (entry_id, current_stage_status_id) or (None, None)."""
    offset = 0
    while True:
        resp = requests.post(
            f"https://api.attio.com/v2/lists/{LIST_ID}/entries/query",
            headers=ATTIO_HEADERS,
            json={"limit": 50, "offset": offset},
        )
        resp.raise_for_status()
        entries = resp.json().get("data", [])
        if not entries:
            break

        for entry in entries:
            if entry.get("parent_record_id") == record_id:
                entry_id = entry["id"]["entry_id"]
                stage_vals = entry.get("entry_values", {}).get("stage", [])
                current_stage = None
                if stage_vals:
                    status_obj = stage_vals[0].get("status", {})
                    current_stage = status_obj.get("id", {}).get("status_id")
                return entry_id, current_stage

        if len(entries) < 50:
            break
        offset += 50

    return None, None


def attio_update_stage(entry_id, stage_id):
    """Update a pipeline entry's stage."""
    resp = requests.put(
        f"https://api.attio.com/v2/lists/{LIST_ID}/entries/{entry_id}",
        headers=ATTIO_HEADERS,
        json={"data": {"entry_values": {"stage": [{"status": stage_id}]}}},
    )
    resp.raise_for_status()
    return resp.json()


def attio_create_note(record_id, title, body):
    """Create a note on a person's record in Attio."""
    try:
        resp = requests.post(
            "https://api.attio.com/v2/notes",
            headers=ATTIO_HEADERS,
            json={
                "data": {
                    "title": title,
                    "format": "plaintext",
                    "content_plaintext": body,
                    "parent_object": "people",
                    "parent_record_id": record_id,
                }
            },
        )
        resp.raise_for_status()
        logger.info(f"Note created on {record_id}: {title}")
        return resp.json()
    except Exception as e:
        logger.error(f"Failed to create note on {record_id}: {e}")
        return None


# Stage progression order — only move forward, never backward
STAGE_ORDER = [
    STAGES["new_lead"],
    STAGES["enriched"],
    STAGES["scored_tier1"],
    STAGES["scored_tier2"],
    STAGES["in_sequence"],
    STAGES["replied"],
    STAGES["meeting_booked"],
]


def should_advance(current_stage_id, target_stage_id):
    """Only advance pipeline stage forward, never backward."""
    if current_stage_id == STAGES["disqualified"]:
        return False
    try:
        current_idx = STAGE_ORDER.index(current_stage_id)
        target_idx = STAGE_ORDER.index(target_stage_id)
        return target_idx > current_idx
    except ValueError:
        # Unknown stage — allow the update
        return True


def process_lead(first_name, last_name, linkedin_url, target_stage, event_type, message_text=None):
    """Core logic: find lead in Attio, update pipeline stage, and log activity."""
    # Try LinkedIn URL first, then name
    record_id = None
    if linkedin_url:
        record_id = attio_find_person_by_linkedin(linkedin_url)

    if not record_id and first_name and last_name:
        record_id = attio_find_person(first_name, last_name)

    if not record_id:
        logger.warning(f"[{event_type}] Lead not found in Attio: {first_name} {last_name} ({linkedin_url})")
        return {"status": "not_found", "name": f"{first_name} {last_name}"}

    # Always log activity as a note (even if stage doesn't advance)
    name = f"{first_name} {last_name}".strip()
    if event_type in ("REPLY", "SYNC") and message_text:
        attio_create_note(record_id, f"LinkedIn reply from {name}", message_text)
    elif event_type == "CONNECTION":
        attio_create_note(record_id, f"LinkedIn connection accepted", f"{name} accepted connection request on LinkedIn.")
    elif event_type == "MESSAGE_SENT":
        attio_create_note(record_id, f"LinkedIn message sent to {name}", message_text or "Outreach message sent via HeyReach.")

    entry_id, current_stage = attio_get_pipeline_entry(record_id)

    if not entry_id:
        logger.warning(f"[{event_type}] {first_name} {last_name} found in People but not in pipeline")
        return {"status": "not_in_pipeline", "name": f"{first_name} {last_name}"}

    if not should_advance(current_stage, target_stage):
        logger.info(f"[{event_type}] {first_name} {last_name} already at stage {current_stage}, skipping stage update")
        return {"status": "already_at_or_past_stage", "name": f"{first_name} {last_name}", "note_logged": True}

    attio_update_stage(entry_id, target_stage)
    logger.info(f"[{event_type}] {first_name} {last_name} → stage updated")
    return {"status": "updated", "name": f"{first_name} {last_name}", "note_logged": True}


# --- Webhook endpoints ---

# /health endpoint is defined below, after sync logic


@app.route("/webhook/reply", methods=["POST"])
def webhook_reply():
    """HeyReach MESSAGE_REPLY_RECEIVED webhook."""
    data = request.json or {}
    logger.info(f"Reply webhook received: {json.dumps(data)[:500]}")

    first_name = data.get("firstName", "")
    last_name = data.get("lastName", "")
    linkedin_url = data.get("profileUrl", "")
    message_text = data.get("messageText", data.get("message", ""))

    result = process_lead(first_name, last_name, linkedin_url, STAGES["replied"], "REPLY", message_text=message_text)
    return jsonify(result)


@app.route("/webhook/connection", methods=["POST"])
def webhook_connection():
    """HeyReach CONNECTION_REQUEST_ACCEPTED webhook."""
    data = request.json or {}
    logger.info(f"Connection webhook received: {json.dumps(data)[:500]}")

    first_name = data.get("firstName", "")
    last_name = data.get("lastName", "")
    linkedin_url = data.get("profileUrl", "")

    # Connection accepted → move to In Sequence (if not already past it)
    result = process_lead(first_name, last_name, linkedin_url, STAGES["in_sequence"], "CONNECTION")
    return jsonify(result)


@app.route("/webhook/message-sent", methods=["POST"])
def webhook_message_sent():
    """HeyReach MESSAGE_SENT webhook."""
    data = request.json or {}
    logger.info(f"Message-sent webhook received: {json.dumps(data)[:500]}")

    first_name = data.get("firstName", "")
    last_name = data.get("lastName", "")
    linkedin_url = data.get("profileUrl", "")

    message_text = data.get("messageText", data.get("message", ""))
    result = process_lead(first_name, last_name, linkedin_url, STAGES["in_sequence"], "MESSAGE_SENT", message_text=message_text)
    return jsonify(result)


# --- Manual sync endpoint ---

@app.route("/sync", methods=["POST"])
def sync_conversations():
    """Manual sync: accepts a list of leads with replies and updates Attio.

    POST body:
    {
        "leads": [
            {"firstName": "Magnus", "lastName": "Olsson", "profileUrl": "https://...", "stage": "replied"},
            ...
        ]
    }

    Stage values: "replied", "in_sequence", "meeting_booked"
    """
    data = request.json or {}
    leads = data.get("leads", [])

    if not leads:
        return jsonify({"error": "No leads provided. Send {\"leads\": [{\"firstName\": ..., \"lastName\": ..., \"profileUrl\": ..., \"stage\": \"replied\"}]}"}), 400

    results = {"total": len(leads), "updated": 0, "skipped": 0, "not_found": 0, "details": []}

    for lead in leads:
        first_name = lead.get("firstName", "")
        last_name = lead.get("lastName", "")
        linkedin_url = lead.get("profileUrl", "")
        stage_name = lead.get("stage", "replied")
        target_stage = STAGES.get(stage_name, STAGES["replied"])
        message_text = lead.get("messageText", "")

        try:
            result = process_lead(first_name, last_name, linkedin_url, target_stage, "SYNC", message_text=message_text)
        except Exception as e:
            logger.error(f"Error processing {first_name} {last_name}: {e}")
            result = {"status": "error", "name": f"{first_name} {last_name}", "error": str(e)}
        results["details"].append(result)

        if result["status"] == "updated":
            results["updated"] += 1
        elif result["status"] == "already_at_or_past_stage":
            results["skipped"] += 1
        else:
            results["not_found"] += 1

    logger.info(f"Sync complete: {results['updated']} updated, {results['skipped']} skipped, {results['not_found']} not found")
    return jsonify(results)


# --- Background conversation sync ---

def fetch_all_conversations():
    """Paginate through all HeyReach conversations for Hanna's account."""
    all_convos = []
    offset = 0
    limit = 100

    while True:
        resp = requests.post(
            "https://api.heyreach.io/api/public/inbox/get-conversations-v2",
            headers=HEYREACH_HEADERS,
            json={
                "linkedInAccountIds": [HANNA_ACCOUNT_ID],
                "limit": limit,
                "offset": offset,
            },
        )
        resp.raise_for_status()
        data = resp.json()
        items = data.get("items", [])
        total = data.get("totalCount", 0)
        all_convos.extend(items)

        if len(all_convos) >= total or len(items) < limit:
            break
        offset += limit
        time.sleep(0.3)

    return all_convos


# Track which replies we've already noted (by conversation ID) to avoid duplicate notes
_noted_replies = set()


def run_conversation_sync():
    """Single sync cycle: fetch conversations, update Attio stages."""
    logger.info("Conversation sync starting...")
    conversations = fetch_all_conversations()
    logger.info(f"Fetched {len(conversations)} conversations")

    updated = 0
    skipped = 0
    not_found = 0

    for convo in conversations:
        profile = convo.get("correspondentProfile", {})
        first_name = profile.get("firstName", "")
        last_name = profile.get("lastName", "")
        profile_url = profile.get("profileUrl", "")
        last_sender = convo.get("lastMessageSender", "")
        last_text = convo.get("lastMessageText", "")
        convo_id = convo.get("id", "")

        # Determine target stage
        if last_sender == "CORRESPONDENT":
            target_stage = STAGES["replied"]
            event_type = "SYNC"
            # Only attach reply text as note if we haven't already
            message_text = last_text if convo_id not in _noted_replies else None
        else:
            target_stage = STAGES["in_sequence"]
            event_type = "SYNC"
            message_text = None

        # Find in Attio
        record_id = None
        if profile_url:
            try:
                record_id = attio_find_person_by_linkedin(profile_url)
            except Exception:
                pass
        if not record_id and first_name and last_name:
            try:
                record_id = attio_find_person(first_name, last_name)
            except Exception:
                pass

        if not record_id:
            not_found += 1
            continue

        # Get pipeline entry
        try:
            entry_id, current_stage = attio_get_pipeline_entry(record_id)
        except Exception:
            not_found += 1
            continue

        if not entry_id:
            not_found += 1
            continue

        if not should_advance(current_stage, target_stage):
            skipped += 1
            continue

        # Create note for new replies
        if last_sender == "CORRESPONDENT" and convo_id not in _noted_replies and last_text:
            name = f"{first_name} {last_name}".strip()
            try:
                attio_create_note(record_id, f"LinkedIn reply from {name}", last_text)
                _noted_replies.add(convo_id)
            except Exception as e:
                logger.error(f"Failed to create reply note for {name}: {e}")

        # Update stage
        try:
            attio_update_stage(entry_id, target_stage)
            target_name = next((k for k, v in STAGES.items() if v == target_stage), "?")
            logger.info(f"[SYNC] {first_name} {last_name} → {target_name}")
            updated += 1
        except Exception as e:
            logger.error(f"[SYNC] Failed to update {first_name} {last_name}: {e}")

        time.sleep(0.1)  # rate limit

    last_sync["time"] = datetime.now(timezone.utc).isoformat()
    last_sync["status"] = "ok"
    last_sync["updated"] = updated
    last_sync["total"] = len(conversations)
    logger.info(f"Conversation sync done: {updated} updated, {skipped} skipped, {not_found} not found out of {len(conversations)}")


def sync_loop():
    """Background loop that runs conversation sync on an interval."""
    # Wait for Flask to start up
    time.sleep(10)
    logger.info(f"Background conversation sync started (interval: {SYNC_INTERVAL}s)")

    while True:
        try:
            run_conversation_sync()
        except Exception as e:
            logger.error(f"Conversation sync failed: {e}")
            last_sync["time"] = datetime.now(timezone.utc).isoformat()
            last_sync["status"] = f"error: {e}"
        time.sleep(SYNC_INTERVAL)


# --- Background email enrichment ---

def transliterate_swedish(name):
    """Convert Swedish characters to ASCII for email guessing."""
    replacements = {'å': 'a', 'ä': 'a', 'ö': 'o', 'Å': 'A', 'Ä': 'A', 'Ö': 'O',
                    'é': 'e', 'è': 'e', 'ë': 'e', 'ü': 'u', 'ñ': 'n'}
    result = name
    for orig, repl in replacements.items():
        result = result.replace(orig, repl)
    result = unicodedata.normalize('NFD', result)
    result = ''.join(c for c in result if unicodedata.category(c) != 'Mn')
    return result


def domain_has_mx(domain):
    """Check if a domain has valid MX records."""
    try:
        answers = dns.resolver.resolve(domain, 'MX')
        return len(answers) > 0
    except Exception:
        return False


def guess_domain(company_name):
    """Guess company domain from name as last resort."""
    clean = company_name.lower().strip()
    clean = re.sub(r'\s*(ab|group|holding|sweden|sverige|nät|elnät|energi\s+nät)\s*$', '', clean, flags=re.IGNORECASE).strip()
    clean = re.sub(r'[^a-zåäö0-9\s]', '', clean)
    clean = transliterate_swedish(clean)
    clean = re.sub(r'\s+', '', clean)
    return f"{clean}.se"


def resolve_company_domain(company_name):
    """Get verified domain for a company. Returns domain or None."""
    if not company_name:
        return None

    # 1. Check researched domain map (exact match)
    domain = DOMAIN_MAP.get(company_name.strip())
    if domain and domain_has_mx(domain):
        return domain

    # 2. Check map with case variations
    for key, val in DOMAIN_MAP.items():
        if key.lower().strip() == company_name.lower().strip():
            if domain_has_mx(val):
                return val

    # 3. Try to get domain from Attio company record
    # (handled in the enrichment loop where we have company_rid)

    # 4. Guess domain and verify MX
    guessed = guess_domain(company_name)
    if domain_has_mx(guessed):
        return guessed

    return None


def generate_email_candidates(first_name, last_name, domain):
    """Generate email candidate patterns."""
    first = transliterate_swedish(first_name).lower().strip()
    last = transliterate_swedish(last_name).lower().strip()
    f = first[0] if first else ""
    candidates = []
    seen = set()
    for pattern in EMAIL_PATTERNS:
        try:
            email = pattern.format(first=first, last=last, f=f, domain=domain)
            if email not in seen:
                seen.add(email)
                candidates.append(email)
        except (KeyError, IndexError):
            continue
    return candidates


def zerobounce_verify(email):
    """Verify a single email via ZeroBounce. Returns (status, is_deliverable)."""
    if not ZEROBOUNCE_API_KEY:
        return "unknown", False

    try:
        resp = requests.get("https://api.zerobounce.net/v2/validate", params={
            "api_key": ZEROBOUNCE_API_KEY,
            "email": email,
            "ip_address": "",
        }, timeout=15)
        data = resp.json()

        zb_status = data.get("status", "unknown").lower()
        status_map = {
            "valid": "valid",
            "invalid": "invalid",
            "catch-all": "catch-all",
            "unknown": "unknown",
            "spamtrap": "invalid",
            "abuse": "invalid",
            "do_not_mail": "invalid",
        }
        normalized = status_map.get(zb_status, "unknown")
        return normalized, normalized == "valid"
    except Exception as e:
        logger.error(f"ZeroBounce error for {email}: {e}")
        return "unknown", False


def pick_best_email(candidates):
    """Verify candidates and return best email. Returns (email, confidence) or (None, None)."""
    for i, email in enumerate(candidates):
        if i > 0:
            time.sleep(0.5)
        status, is_valid = zerobounce_verify(email)

        if status == "valid":
            return email, "high"
        elif status == "catch-all":
            # Catch-all: first candidate (most specific) is best
            return email, "medium"

    return None, None


def get_heyreach_company_for_lead(linkedin_slug, conversations_cache):
    """Get current company from HeyReach conversation data (LinkedIn = source of truth)."""
    for convo in conversations_cache:
        profile = convo.get("correspondentProfile", {})
        url = profile.get("profileUrl", "")
        if linkedin_slug and linkedin_slug in url.lower():
            return profile.get("companyName", "")
    return None


def fetch_all_pipeline_entries():
    """Fetch all ATLAS pipeline entries."""
    entries = []
    offset = 0
    while True:
        resp = requests.post(
            f"https://api.attio.com/v2/lists/{LIST_ID}/entries/query",
            headers=ATTIO_HEADERS,
            json={"limit": 50, "offset": offset},
        )
        resp.raise_for_status()
        batch = resp.json().get("data", [])
        entries.extend(batch)
        if len(batch) < 50:
            break
        offset += 50
        time.sleep(0.1)
    return entries


def run_email_enrichment(conversations_cache):
    """Single enrichment cycle: find leads without email, verify company, find email."""
    if not ZEROBOUNCE_API_KEY:
        logger.info("Email enrichment skipped — ZEROBOUNCE_API_KEY not set")
        return

    logger.info("Email enrichment starting...")

    # Get all pipeline entries
    entries = fetch_all_pipeline_entries()

    # Filter to entries without email AND not already searched
    to_enrich = []
    for entry in entries:
        ev = entry.get("entry_values", {})
        has_email = ev.get("has_email", [])
        already_has = has_email and has_email[0].get("value") is True
        searched = ev.get("email_searched", [])
        already_searched = searched and searched[0].get("value") is True
        if not already_has and not already_searched:
            to_enrich.append(entry)

    logger.info(f"Email enrichment: {len(to_enrich)} leads need email (out of {len(entries)} total)")

    if not to_enrich:
        last_email_enrichment["time"] = datetime.now(timezone.utc).isoformat()
        last_email_enrichment["status"] = "ok"
        last_email_enrichment["found"] = 0
        last_email_enrichment["processed"] = 0
        return

    # Process up to 10 per cycle to conserve ZeroBounce credits
    batch = to_enrich[:10]
    found = 0

    for entry in batch:
        record_id = entry.get("parent_record_id", "")
        entry_id = entry.get("id", {}).get("entry_id", "")

        # Fetch person from Attio
        try:
            resp = requests.get(
                f"https://api.attio.com/v2/objects/people/records/{record_id}",
                headers=ATTIO_HEADERS,
            )
            resp.raise_for_status()
            person = resp.json()
        except Exception as e:
            logger.error(f"Failed to fetch person {record_id}: {e}")
            continue

        values = person.get("data", {}).get("values", {})
        name_val = values.get("name", [])
        first_name = name_val[0].get("first_name", "") if name_val else ""
        last_name = name_val[0].get("last_name", "") if name_val else ""
        linkedin_vals = values.get("linkedin", [])
        linkedin_url = linkedin_vals[0].get("value", "") if linkedin_vals else ""
        linkedin_slug = linkedin_url.lower().split("/in/")[-1].rstrip("/") if "/in/" in linkedin_url else ""

        if not first_name or not last_name:
            continue

        # Step 1: Get CURRENT company from HeyReach (LinkedIn = truth)
        heyreach_company = get_heyreach_company_for_lead(linkedin_slug, conversations_cache)

        # Fallback: get company from Attio
        attio_company = ""
        company_vals = values.get("company", [])
        if company_vals:
            company_rid = company_vals[0].get("target_record_id", "")
            if company_rid:
                try:
                    cresp = requests.get(
                        f"https://api.attio.com/v2/objects/companies/records/{company_rid}",
                        headers=ATTIO_HEADERS,
                    )
                    cresp.raise_for_status()
                    cvals = cresp.json().get("data", {}).get("values", {})
                    cname = cvals.get("name", [])
                    attio_company = cname[0].get("value", "") if cname else ""
                    # Also check if company has a domain in Attio
                    domains = cvals.get("domains", [])
                    if domains:
                        attio_domain = domains[0].get("domain", "")
                        if attio_domain and domain_has_mx(attio_domain):
                            # Attio has a verified domain already
                            pass
                except Exception:
                    pass
                time.sleep(0.1)

        # Use HeyReach company (current employer) over Attio
        company = heyreach_company or attio_company
        if not company:
            logger.info(f"[EMAIL] {first_name} {last_name} — no company found, marking searched")
            try:
                requests.patch(
                    f"https://api.attio.com/v2/lists/{LIST_ID}/entries/{entry_id}",
                    headers=ATTIO_HEADERS,
                    json={"data": {"entry_values": {"email_searched": [{"value": True}]}}},
                )
            except Exception:
                pass
            continue

        # Step 2: Resolve and verify domain
        domain = resolve_company_domain(company)
        if not domain:
            logger.info(f"[EMAIL] {first_name} {last_name} @ {company} — no valid domain found, marking searched")
            try:
                requests.patch(
                    f"https://api.attio.com/v2/lists/{LIST_ID}/entries/{entry_id}",
                    headers=ATTIO_HEADERS,
                    json={"data": {"entry_values": {"email_searched": [{"value": True}]}}},
                )
            except Exception:
                pass
            continue

        # Step 3: Generate candidates and verify with ZeroBounce
        candidates = generate_email_candidates(first_name, last_name, domain)
        logger.info(f"[EMAIL] {first_name} {last_name} @ {company} → {domain} ({len(candidates)} candidates)")

        best_email, confidence = pick_best_email(candidates)

        if best_email:
            logger.info(f"[EMAIL] {first_name} {last_name} → FOUND {best_email} ({confidence})")
            found += 1
            try:
                # Add email to person
                requests.patch(
                    f"https://api.attio.com/v2/objects/people/records/{record_id}",
                    headers=ATTIO_HEADERS,
                    json={"data": {"values": {"email_addresses": [{"email_address": best_email}]}}},
                )
                # Update pipeline entry
                requests.patch(
                    f"https://api.attio.com/v2/lists/{LIST_ID}/entries/{entry_id}",
                    headers=ATTIO_HEADERS,
                    json={"data": {"entry_values": {
                        "has_email": [{"value": True}],
                        "email_searched": [{"value": True}],
                    }}},
                )
            except Exception as e:
                logger.error(f"[EMAIL] Failed to update Attio for {first_name} {last_name}: {e}")
        else:
            logger.info(f"[EMAIL] {first_name} {last_name} @ {company} → no valid email found")
            try:
                requests.patch(
                    f"https://api.attio.com/v2/lists/{LIST_ID}/entries/{entry_id}",
                    headers=ATTIO_HEADERS,
                    json={"data": {"entry_values": {"email_searched": [{"value": True}]}}},
                )
            except Exception:
                pass

        time.sleep(0.3)

    last_email_enrichment["time"] = datetime.now(timezone.utc).isoformat()
    last_email_enrichment["status"] = "ok"
    last_email_enrichment["found"] = found
    last_email_enrichment["processed"] = len(batch)
    logger.info(f"Email enrichment done: {found} found out of {len(batch)} processed")


def email_enrichment_loop():
    """Background loop for email enrichment."""
    # Wait for first conversation sync to populate cache
    time.sleep(60)
    logger.info(f"Background email enrichment started (interval: {EMAIL_ENRICHMENT_INTERVAL}s)")

    while True:
        try:
            # Fetch fresh conversations to get current company data
            conversations = fetch_all_conversations()
            run_email_enrichment(conversations)
        except Exception as e:
            logger.error(f"Email enrichment failed: {e}")
            last_email_enrichment["time"] = datetime.now(timezone.utc).isoformat()
            last_email_enrichment["status"] = f"error: {e}"
        time.sleep(EMAIL_ENRICHMENT_INTERVAL)


# Health endpoint with both sync statuses
@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({
        "status": "ok",
        "last_sync": last_sync,
        "last_email_enrichment": last_email_enrichment,
    })


# Start background threads (works with both gunicorn and direct run)
_sync_started = False
if not _sync_started:
    _sync_started = True
    sync_thread = threading.Thread(target=sync_loop, daemon=True)
    sync_thread.start()
    email_thread = threading.Thread(target=email_enrichment_loop, daemon=True)
    email_thread.start()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
