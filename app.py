"""
HeyReach → Attio Webhook Bridge

Deployed on Railway. Receives HeyReach webhook events and updates
Attio CRM pipeline stages accordingly.

Endpoints:
  POST /webhook/reply         — HeyReach MESSAGE_REPLY_RECEIVED
  POST /webhook/connection    — HeyReach CONNECTION_REQUEST_ACCEPTED
  POST /webhook/message-sent  — HeyReach MESSAGE_SENT
  POST /sync                  — Pull all HeyReach conversations and sync to Attio
  GET  /health                — Health check
"""

import os
import json
import logging
from flask import Flask, request, jsonify
import requests

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


def process_lead(first_name, last_name, linkedin_url, target_stage, event_type):
    """Core logic: find lead in Attio and update their pipeline stage."""
    # Try LinkedIn URL first, then name
    record_id = None
    if linkedin_url:
        record_id = attio_find_person_by_linkedin(linkedin_url)

    if not record_id and first_name and last_name:
        record_id = attio_find_person(first_name, last_name)

    if not record_id:
        logger.warning(f"[{event_type}] Lead not found in Attio: {first_name} {last_name} ({linkedin_url})")
        return {"status": "not_found", "name": f"{first_name} {last_name}"}

    entry_id, current_stage = attio_get_pipeline_entry(record_id)

    if not entry_id:
        logger.warning(f"[{event_type}] {first_name} {last_name} found in People but not in pipeline")
        return {"status": "not_in_pipeline", "name": f"{first_name} {last_name}"}

    if not should_advance(current_stage, target_stage):
        logger.info(f"[{event_type}] {first_name} {last_name} already at stage {current_stage}, skipping")
        return {"status": "already_at_or_past_stage", "name": f"{first_name} {last_name}"}

    attio_update_stage(entry_id, target_stage)
    logger.info(f"[{event_type}] {first_name} {last_name} → stage updated")
    return {"status": "updated", "name": f"{first_name} {last_name}"}


# --- Webhook endpoints ---

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


@app.route("/webhook/reply", methods=["POST"])
def webhook_reply():
    """HeyReach MESSAGE_REPLY_RECEIVED webhook."""
    data = request.json or {}
    logger.info(f"Reply webhook received: {json.dumps(data)[:500]}")

    first_name = data.get("firstName", "")
    last_name = data.get("lastName", "")
    linkedin_url = data.get("profileUrl", "")

    result = process_lead(first_name, last_name, linkedin_url, STAGES["replied"], "REPLY")
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

    result = process_lead(first_name, last_name, linkedin_url, STAGES["in_sequence"], "MESSAGE_SENT")
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

        result = process_lead(first_name, last_name, linkedin_url, target_stage, "SYNC")
        results["details"].append(result)

        if result["status"] == "updated":
            results["updated"] += 1
        elif result["status"] == "already_at_or_past_stage":
            results["skipped"] += 1
        else:
            results["not_found"] += 1

    logger.info(f"Sync complete: {results['updated']} updated, {results['skipped']} skipped, {results['not_found']} not found")
    return jsonify(results)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
