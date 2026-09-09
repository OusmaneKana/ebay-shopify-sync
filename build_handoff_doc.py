from pathlib import Path
from datetime import date
import textwrap

from PIL import Image, ImageDraw, ImageFont
from docx import Document
from docx.enum.section import WD_SECTION
from docx.enum.text import WD_ALIGN_PARAGRAPH, WD_BREAK
from docx.enum.table import WD_CELL_VERTICAL_ALIGNMENT, WD_TABLE_ALIGNMENT
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Inches, Pt, RGBColor

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "PROJECT_HANDOFF_AND_SYSTEM_DESIGN.docx"
DIAGRAM = ROOT / ".handoff_architecture.png"

NAVY = "17324D"
BLUE = "2E74B5"
LIGHT = "EEF3F8"
PALE = "F7F9FB"
GRAY = "5B6573"
RED = "9B1C1C"
GOLD = "8A6500"
GREEN = "276749"
WHITE = "FFFFFF"


def rgb(hex_value):
    return RGBColor.from_string(hex_value)


def set_cell_shading(cell, fill):
    tc_pr = cell._tc.get_or_add_tcPr()
    shd = tc_pr.find(qn("w:shd"))
    if shd is None:
        shd = OxmlElement("w:shd")
        tc_pr.append(shd)
    shd.set(qn("w:fill"), fill)


def set_cell_margins(cell, top=100, start=120, bottom=100, end=120):
    tc = cell._tc
    tc_pr = tc.get_or_add_tcPr()
    tc_mar = tc_pr.first_child_found_in("w:tcMar")
    if tc_mar is None:
        tc_mar = OxmlElement("w:tcMar")
        tc_pr.append(tc_mar)
    for tag, value in (("top", top), ("start", start), ("bottom", bottom), ("end", end)):
        node = tc_mar.find(qn(f"w:{tag}"))
        if node is None:
            node = OxmlElement(f"w:{tag}")
            tc_mar.append(node)
        node.set(qn("w:w"), str(value))
        node.set(qn("w:type"), "dxa")


def set_repeat_table_header(row):
    tr_pr = row._tr.get_or_add_trPr()
    tbl_header = OxmlElement("w:tblHeader")
    tbl_header.set(qn("w:val"), "true")
    tr_pr.append(tbl_header)


def set_table_geometry(table, widths):
    table.autofit = False
    table.alignment = WD_TABLE_ALIGNMENT.CENTER
    table_pr = table._tbl.tblPr
    tbl_w = table_pr.first_child_found_in("w:tblW")
    tbl_w.set(qn("w:w"), "9360")
    tbl_w.set(qn("w:type"), "dxa")
    tbl_ind = OxmlElement("w:tblInd")
    tbl_ind.set(qn("w:w"), "120")
    tbl_ind.set(qn("w:type"), "dxa")
    table_pr.append(tbl_ind)
    grid = table._tbl.tblGrid
    for child in list(grid):
        grid.remove(child)
    for width in widths:
        col = OxmlElement("w:gridCol")
        col.set(qn("w:w"), str(width))
        grid.append(col)
    for row in table.rows:
        for idx, cell in enumerate(row.cells):
            width = widths[idx]
            tc_w = cell._tc.get_or_add_tcPr().first_child_found_in("w:tcW")
            tc_w.set(qn("w:w"), str(width))
            tc_w.set(qn("w:type"), "dxa")
            set_cell_margins(cell)
            cell.vertical_alignment = WD_CELL_VERTICAL_ALIGNMENT.CENTER


def add_page_number(paragraph):
    paragraph.alignment = WD_ALIGN_PARAGRAPH.RIGHT
    run = paragraph.add_run("Page ")
    fld = OxmlElement("w:fldSimple")
    fld.set(qn("w:instr"), "PAGE")
    run._r.addnext(fld)


def style_doc(doc):
    sec = doc.sections[0]
    sec.page_width = Inches(8.5)
    sec.page_height = Inches(11)
    sec.top_margin = sec.bottom_margin = Inches(0.82)
    sec.left_margin = sec.right_margin = Inches(1.0)
    sec.header_distance = sec.footer_distance = Inches(0.45)

    styles = doc.styles
    normal = styles["Normal"]
    normal.font.name = "Aptos"
    normal.font.size = Pt(10.5)
    normal.font.color.rgb = rgb("20262E")
    normal.paragraph_format.space_after = Pt(6)
    normal.paragraph_format.line_spacing = 1.1

    for name, size, color, before, after in (
        ("Title", 28, NAVY, 0, 8),
        ("Subtitle", 13, GRAY, 0, 12),
        ("Heading 1", 17, BLUE, 16, 8),
        ("Heading 2", 13, BLUE, 12, 6),
        ("Heading 3", 11.5, NAVY, 9, 4),
    ):
        st = styles[name]
        st.font.name = "Aptos Display" if name != "Normal" else "Aptos"
        st.font.size = Pt(size)
        st.font.color.rgb = rgb(color)
        st.font.bold = name != "Subtitle"
        st.paragraph_format.space_before = Pt(before)
        st.paragraph_format.space_after = Pt(after)
        st.paragraph_format.keep_with_next = name.startswith("Heading")

    for list_name in ("List Bullet", "List Number"):
        st = styles[list_name]
        st.font.name = "Aptos"
        st.font.size = Pt(10.5)
        st.paragraph_format.left_indent = Inches(0.5)
        st.paragraph_format.first_line_indent = Inches(-0.25)
        st.paragraph_format.space_after = Pt(4)
        st.paragraph_format.line_spacing = 1.1

    header = sec.header.paragraphs[0]
    header.text = "EBAY–SHOPIFY–ETSY SYNC  |  ENGINEERING HANDOFF"
    header.alignment = WD_ALIGN_PARAGRAPH.LEFT
    for run in header.runs:
        run.font.name = "Aptos"
        run.font.size = Pt(8)
        run.font.bold = True
        run.font.color.rgb = rgb(GRAY)
    footer = sec.footer.paragraphs[0]
    add_page_number(footer)
    for run in footer.runs:
        run.font.name = "Aptos"
        run.font.size = Pt(8)
        run.font.color.rgb = rgb(GRAY)


def add_title_page(doc):
    p = doc.add_paragraph()
    p.paragraph_format.space_before = Pt(100)
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r = p.add_run("PROJECT HANDOFF")
    r.font.name = "Aptos Display"
    r.font.size = Pt(11)
    r.font.bold = True
    r.font.color.rgb = rgb(BLUE)
    r.font.letter_spacing = Pt(1.5)
    p = doc.add_paragraph(style="Title")
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p.add_run("eBay–Shopify–Etsy\nSynchronization Platform")
    p = doc.add_paragraph(style="Subtitle")
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    p.add_run("Operational handoff, high-level system design, risks, and continuity plan")
    p = doc.add_paragraph()
    p.paragraph_format.space_before = Pt(38)
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r = p.add_run("Prepared from the repository state on August 21, 2026")
    r.font.size = Pt(10)
    r.font.color.rgb = rgb(GRAY)
    add_callout(doc, "Handoff intent", "Give a new technical owner enough context to operate, debug, deploy, and safely evolve the system without relying on undocumented tribal knowledge.", BLUE)
    p = doc.add_paragraph()
    p.paragraph_format.space_before = Pt(70)
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r = p.add_run("CONFIDENTIAL — contains architecture and operational details; credentials are intentionally omitted")
    r.font.size = Pt(8)
    r.font.bold = True
    r.font.color.rgb = rgb(GRAY)
    doc.add_page_break()


def add_callout(doc, label, text, color=BLUE):
    table = doc.add_table(rows=1, cols=1)
    set_table_geometry(table, [9360])
    cell = table.cell(0, 0)
    set_cell_shading(cell, LIGHT)
    p = cell.paragraphs[0]
    p.paragraph_format.space_after = Pt(2)
    r = p.add_run(label.upper())
    r.bold = True
    r.font.size = Pt(9)
    r.font.color.rgb = rgb(color)
    p = cell.add_paragraph(text)
    p.paragraph_format.space_after = Pt(0)
    return table


def add_table(doc, headers, rows, widths):
    table = doc.add_table(rows=1, cols=len(headers))
    table.style = "Table Grid"
    hdr = table.rows[0]
    set_repeat_table_header(hdr)
    for i, text in enumerate(headers):
        cell = hdr.cells[i]
        set_cell_shading(cell, NAVY)
        p = cell.paragraphs[0]
        p.paragraph_format.space_after = Pt(0)
        r = p.add_run(str(text))
        r.bold = True
        r.font.size = Pt(9)
        r.font.color.rgb = rgb(WHITE)
    for row in rows:
        cells = table.add_row().cells
        for i, value in enumerate(row):
            p = cells[i].paragraphs[0]
            p.paragraph_format.space_after = Pt(0)
            r = p.add_run(str(value))
            r.font.size = Pt(9)
    set_table_geometry(table, widths)
    doc.add_paragraph().paragraph_format.space_after = Pt(0)
    return table


def bullet(doc, text, level=0):
    p = doc.add_paragraph(text, style="List Bullet" if level == 0 else "List Bullet 2")
    return p


def numbered(doc, text):
    return doc.add_paragraph(text, style="List Number")


def make_diagram():
    im = Image.new("RGB", (1900, 1050), "white")
    draw = ImageDraw.Draw(im)
    font_dir = Path("/System/Library/Fonts/Supplemental")
    regular = ImageFont.truetype(str(font_dir / "Arial.ttf"), 25)
    small = ImageFont.truetype(str(font_dir / "Arial.ttf"), 20)
    bold = ImageFont.truetype(str(font_dir / "Arial Bold.ttf"), 30)
    title_font = ImageFont.truetype(str(font_dir / "Arial Bold.ttf"), 40)

    def center_text(xy, text, font, fill, max_width=None):
        x1, y1, x2, y2 = xy
        lines = textwrap.wrap(text, width=max_width or 26)
        total_h = sum(draw.textbbox((0, 0), line, font=font)[3] for line in lines) + 4 * (len(lines)-1)
        y = (y1 + y2 - total_h) / 2
        for line in lines:
            box_ = draw.textbbox((0, 0), line, font=font)
            w = box_[2] - box_[0]
            draw.text(((x1+x2-w)/2, y), line, font=font, fill=fill)
            y += box_[3] - box_[1] + 4

    def box(xy, title, subtitle, fc, ec=NAVY):
        draw.rounded_rectangle(xy, radius=18, fill=f"#{fc}", outline=f"#{ec}", width=4)
        x1, y1, x2, y2 = xy
        center_text((x1, y1+10, x2, y1+65), title, bold, f"#{NAVY}")
        center_text((x1+12, y1+63, x2-12, y2-8), subtitle, small, f"#{GRAY}", 28)

    def arrow(start, end, label=""):
        draw.line([start, end], fill=f"#{BLUE}", width=5)
        import math
        angle = math.atan2(end[1]-start[1], end[0]-start[0])
        size = 18
        pts = [end,
               (end[0]-size*math.cos(angle-0.45), end[1]-size*math.sin(angle-0.45)),
               (end[0]-size*math.cos(angle+0.45), end[1]-size*math.sin(angle+0.45))]
        draw.polygon(pts, fill=f"#{BLUE}")
        if label:
            mx, my = (start[0]+end[0])//2, (start[1]+end[1])//2
            bb = draw.textbbox((0,0), label, font=small)
            w, h = bb[2]-bb[0], bb[3]-bb[1]
            draw.rectangle((mx-w//2-6, my-h//2-6, mx+w//2+6, my+h//2+6), fill="white")
            draw.text((mx-w//2, my-h//2), label, font=small, fill=f"#{GRAY}")

    center_text((0, 20, 1900, 90), "HIGH-LEVEL SYSTEM CONTEXT", title_font, f"#{NAVY}")
    center_text((0, 88, 1900, 135), "MongoDB is the durable coordination layer; SKU is the main cross-channel identity.", regular, f"#{GRAY}", 90)
    box((35, 180, 350, 330), "eBay", "Trading API, OAuth, listing and order notifications", "EAF2F8")
    box((35, 440, 350, 590), "Etsy", "API v3, OAuth/PKCE and signed webhooks", "F4EEF8")
    box((35, 700, 350, 850), "Operators", "Admin pages, reports, audits and manual actions", "F2F4F7")
    box((505, 300, 900, 505), "FastAPI service", "Routes, auth, webhook intake, normalization and orchestration", "E8F0F8")
    box((505, 675, 900, 865), "Durable workers", "Inventory jobs, Etsy publish queue and eBay listing queue", "FFF6DD", GOLD)
    box((1080, 300, 1450, 505), "MongoDB", "Raw and normalized products, tokens, events, queues and audit state", "EDF7F2", GREEN)
    box((1550, 180, 1865, 330), "Shopify", "Admin APIs and inventory levels", "EAF5EC", GREEN)
    box((1550, 440, 1865, 590), "OpenAI", "Optional enrichment, SEO and taxonomy assistance", "F2F4F7")
    box((1550, 700, 1865, 850), "Cloud Run", "Container runtime and centralized logs", "EEF3F8")
    arrow((350, 255), (505, 350), "API / webhook")
    arrow((350, 515), (505, 445), "API / webhook")
    arrow((350, 775), (505, 775), "HTTPS")
    arrow((900, 400), (1080, 400), "read / write")
    arrow((760, 505), (720, 675), "enqueue")
    arrow((900, 760), (1120, 505), "claim / persist")
    arrow((1450, 380), (1550, 255), "sync")
    arrow((900, 350), (1550, 515), "optional")
    arrow((1550, 775), (900, 470), "hosts")
    im.save(DIAGRAM)


def section(doc, title, intro=None):
    doc.add_heading(title, level=1)
    if intro:
        doc.add_paragraph(intro)


def main():
    make_diagram()
    doc = Document()
    style_doc(doc)
    add_title_page(doc)

    section(doc, "1. Executive handoff summary")
    add_callout(doc, "Current reality", "This is no longer only an eBay-to-Shopify middleware. The current code implements a three-channel inventory orchestration platform with Etsy onboarding and publishing workflows, while the README still describes Etsy as future work.", RED)
    doc.add_heading("What the system does", level=2)
    bullet(doc, "Imports active eBay listings into MongoDB, normalizes them into a channel-neutral product record, and creates or updates Shopify products and inventory.")
    bullet(doc, "Processes eBay listing and sale notifications, and Etsy signed webhook events, into durable inventory events and downstream channel-sync jobs.")
    bullet(doc, "Supports Etsy OAuth, matching/review, draft creation, bulk publishing preparation, taxonomy/SEO assistance, and an Etsy publishing queue.")
    bullet(doc, "Provides lightweight browser-based operations pages for product review, audits, reporting, multichannel status, conflict policy, and item timelines.")
    doc.add_heading("Operating posture at handoff", level=2)
    add_table(doc, ["Area", "Observed state", "Handoff implication"], [
        ("Deployment", "Dockerized FastAPI; helper deploys to Google Cloud Run via Artifact Registry.", "Confirm the live GCP project, service, region, repository, secrets, and IAM with the outgoing owner."),
        ("Scheduling", "Startup scheduler is commented out; its full_sync target is currently a placeholder.", "Assume there is no reliable periodic sync unless an external scheduler exists."),
        ("Security", "ADMIN_PASSKEY protects prod sync routes and selected pages; several reporting/pages and legacy sync routes remain open when reachable.", "Place behind authenticated ingress and complete an endpoint-access review."),
        ("Testing", "One focused test module exists for the Etsy publish queue; broad integration coverage is absent.", "Use dry runs and a test SKU before production changes."),
        ("Repository", "Working tree contains modified and untracked source files at review time.", "Reconcile, commit, and tag the intended handoff baseline before deployment."),
    ], [1650, 3100, 4610])

    doc.add_heading("First-day checklist for the new owner", level=2)
    for item in (
        "Obtain access to the Git repository, GCP project/Cloud Run, Artifact Registry, MongoDB, eBay developer account, Shopify admin/app, Etsy developer account/shop, and DNS/webhook consoles.",
        "Export the live Cloud Run configuration and secret names (never secrets themselves) into the team password manager/runbook.",
        "Confirm /health, /auth/ebay/status, /auth/etsy/status, and the operator dashboards against the production URL.",
        "Inspect recent Cloud Run logs and MongoDB queue/event collections for stuck, retrying, or unresolved work.",
        "Freeze high-risk operations—purge, bulk publish, zero-inventory propagation—until access controls and a rollback path are verified.",
    ):
        bullet(doc, item)
    doc.add_page_break()

    section(doc, "2. High-level system design", "The service follows a MongoDB-backed ingestion, normalization, event, and fan-out architecture. External marketplaces remain the systems of record for their own listings; product_normalized is the internal canonical coordination record.")
    doc.add_picture(str(DIAGRAM), width=Inches(6.45))
    cap = doc.add_paragraph("Figure 1. System context and principal data movement.")
    cap.alignment = WD_ALIGN_PARAGRAPH.CENTER
    cap.runs[0].italic = True
    cap.runs[0].font.size = Pt(8.5)
    cap.runs[0].font.color.rgb = rgb(GRAY)
    doc.add_heading("Primary architectural responsibilities", level=2)
    add_table(doc, ["Component", "Responsibility", "Key implementation"], [
        ("FastAPI application", "HTTP entry point; static operator UI; API, OAuth callbacks, webhooks, health.", "app/main.py, app/api/router.py, app/api/routes/*"),
        ("eBay adapter", "Trading API access, token lifecycle, active-listing import, listing/order notification processing.", "app/ebay/*, ebay_auth_service.py, ebay_webhook_service.py"),
        ("Normalizer", "Transforms raw eBay records into the canonical product shape and optional LLM-enriched content.", "app/normalizer/*, normalizer_service.py"),
        ("Shopify adapter", "Product creation/update, image and inventory operations, exclusion policy, throttling.", "app/shopify/*, shopify_sync.py"),
        ("Etsy adapter/workflows", "OAuth, signed events, matching/review, draft creation, publish prep and queue processing.", "etsy_auth_service.py, etsy_webhook_service.py, reporting.py, etsy_publish_queue.py"),
        ("Inventory orchestrator", "Idempotent sale events, conflict policy, target-channel jobs, retries, reconcile and dashboards.", "multichannel_sync_service.py"),
        ("MongoDB", "Durable product state, OAuth tokens, webhook audit, queues, policies, events and reports.", "app/database/mongo.py and named collections"),
    ], [1750, 3400, 4210])

    doc.add_heading("Core data model", level=2)
    bullet(doc, "product_raw — source-shaped eBay listing documents, upserted primarily by SKU; ended/missing listings can be driven to quantity zero.")
    bullet(doc, "product_normalized — canonical per-SKU product record with normalized content plus channels.ebay, channels.shopify, and channels.etsy linkage/state.")
    bullet(doc, "inventory_events — immutable-ish sale/event records with deterministic IDs for duplicate suppression and replay state.")
    bullet(doc, "channel_sync_jobs — per-target durable jobs claimed by workers, with queued/processing/completed/retry/failed status.")
    bullet(doc, "OAuth/notification/queue/report collections — operational state enumerated in Section 6.")
    add_callout(doc, "Identity rule", "SKU is the primary cross-channel join key. Missing, reused, or inconsistent SKUs are the highest-leverage source of incorrect matching or inventory propagation.", GOLD)
    doc.add_page_break()

    section(doc, "3. End-to-end workflows")
    doc.add_heading("A. Batch import and Shopify synchronization", level=2)
    for item in (
        "Call POST /sync/prod/sync-ebay-raw to fetch active eBay listings and persist product_raw records.",
        "Call POST /sync/prod/normalize-raw to transform raw records into product_normalized.",
        "Call POST /sync/prod/sync-shopify with explicit options. Start with zero_inventory=false and allow_zero_inventory_updates=false unless the inventory impact has been reviewed.",
        "Review the response/job status and verify a sample in Shopify and MongoDB before broadening scope.",
    ):
        numbered(doc, item)
    add_callout(doc, "Known defect", "POST /sync/prod/run-all should not be relied upon in the reviewed tree: _run_all_steps does not accept the env argument passed by the route, and also references an undefined env variable in its response.", RED)

    doc.add_heading("B. Near-real-time eBay listing flow", level=2)
    for item in (
        "eBay sends ItemListed to /webhooks/ebay/orders.",
        "The service acknowledges quickly and schedules in-process asynchronous handling.",
        "The handler upserts product_raw, queues the SKU in ebay_listing_sync_queue, normalizes it, and creates/updates Shopify.",
        "Webhook-provided image URLs may remain low resolution; the repository includes a re-normalize plus GraphQL image-update recovery workflow.",
    ):
        numbered(doc, item)

    doc.add_heading("C. Sale event and multichannel inventory flow", level=2)
    for item in (
        "An eBay or Etsy notification is parsed and persisted to a notification collection for audit/replay.",
        "The service resolves a SKU, derives an idempotent event ID, applies the canonical quantity change, and records inventory_events.",
        "It creates channel_sync_jobs for linked channels other than the sale source (eBay, Etsy, Shopify).",
        "A worker claims jobs and pushes the target quantity; transient failures enter retry state, terminal failures require replay or intervention.",
        "Operators use the command center, dashboard, timeline, reconcile, and replay endpoints to inspect and recover.",
    ):
        numbered(doc, item)

    doc.add_heading("D. Etsy onboarding and publishing flow", level=2)
    bullet(doc, "OAuth uses PKCE and stores refreshable tokens in etsy_oauth_tokens; startup logs token-health warnings.")
    bullet(doc, "Review tools match existing Etsy listings to normalized SKUs and record approvals/denials.")
    bullet(doc, "Publish preparation derives required listing data and can use OpenAI for SEO/tags/taxonomy assistance when configured.")
    bullet(doc, "Draft creation may be direct or queued in etsy_publish_jobs; jobs retry up to five attempts for selected failures.")
    bullet(doc, "The Etsy publish worker is manually invoked unless an external scheduler calls it.")
    doc.add_page_break()

    section(doc, "4. Operations runbook")
    doc.add_heading("Safe startup verification", level=2)
    add_table(doc, ["Check", "Expected result", "If it fails"], [
        ("GET /health", "HTTP 200 and healthy status.", "Check Cloud Run revision health, container logs, port 8080 and startup exceptions."),
        ("GET /auth/ebay/status", "Valid token or refreshable state.", "Reauthorize with /auth/ebay/login; verify RuName/callback and Mongo token document."),
        ("GET /auth/etsy/status", "Valid token with refresh token.", "Reauthorize with /auth/etsy/login; verify PKCE redirect and app configuration."),
        ("MongoDB", "Readable product and queue collections; no sustained connection errors.", "Check MONGO_URI, network allowlist/VPC, database name and credentials."),
        ("Shopify health", "Authenticated API response through protected health endpoint.", "Verify store URL, Admin API token/password, app scopes and API compatibility."),
    ], [1800, 3500, 4060])

    doc.add_heading("Routine operator cadence", level=2)
    add_table(doc, ["Cadence", "Review"], [
        ("Daily", "Cloud Run errors; invalid webhook signatures; failed/retry channel_sync_jobs; unresolved Etsy receipts; inventory discrepancies; OAuth expiry warnings."),
        ("Weekly", "Audit summary, duplicates/missing SKUs/unlinked items, low-resolution images, publish queue failures, Shopify exclusion tags, database growth."),
        ("Before deploy", "Clean/intentional Git diff, tests, Docker build, secret/config diff, dry-run target verification, rollback revision identified."),
        ("After deploy", "Health/auth status, webhook acceptance, one read-only report, one controlled test SKU, logs for exceptions, queue depth trend."),
    ], [1800, 7560])

    doc.add_heading("Incident triage sequence", level=2)
    for item in (
        "Stop amplification: pause external schedulers or avoid invoking workers/bulk endpoints. Do not purge or zero inventory as a diagnostic step.",
        "Identify scope: one SKU, one channel, one webhook type, or system-wide. Capture timestamps, SKU/listing IDs, event ID and job ID.",
        "Inspect Cloud Run logs and the relevant notification, inventory_events and channel_sync_jobs documents.",
        "Compare product_normalized channel quantities/link IDs with the live channel records.",
        "Correct identity or policy first; then use reconcile or replay-failed for the smallest possible scope.",
        "Verify all three channels and document the incident, root cause, recovery and any compensating manual updates.",
    ):
        numbered(doc, item)

    doc.add_heading("Rollback", level=2)
    bullet(doc, "Application rollback: route traffic to the last known-good Cloud Run revision.")
    bullet(doc, "Data rollback: no automated database rollback is documented. Prefer compensating writes after exporting the affected MongoDB records and live channel state.")
    bullet(doc, "Credential rollback: rotate/revoke marketplace tokens and secrets if exposure is suspected, then reauthorize and update the secret store.")
    doc.add_page_break()

    section(doc, "5. Configuration, deployment, and access transfer")
    doc.add_heading("Configuration inventory", level=2)
    add_table(doc, ["Domain", "Environment variables", "Notes"], [
        ("MongoDB", "MONGO_URI, MONGO_DB", "Required at import/startup."),
        ("eBay", "EBAY_APP_ID, EBAY_CERT_ID, EBAY_DEV_ID, EBAY_RUNAME, optional EBAY_OAUTH_TOKEN", "Database OAuth token is preferred; callback registration must exactly match."),
        ("Shopify", "SHOPIFY_API_KEY_PROD, SHOPIFY_PASSWORD_PROD, SHOPIFY_STORE_URL_PROD", "Production-only configuration; validate app scopes and token model."),
        ("Etsy", "ETSY_CLIENT_ID, ETSY_CLIENT_SECRET, ETSY_REDIRECT_URI, ETSY_CODE_VERIFIER, ETSY_SCOPES, ETSY_TOKEN, ETSY_WEBHOOK_SIGNING_SECRET, tolerance and listing defaults", "Most are optional in Settings but required by their workflows."),
        ("OpenAI", "OPENAI_API_KEY", "Optional; enables enrichment and Etsy content assistance."),
        ("Admin", "ADMIN_PASSKEY", "If absent, passkey checks allow access. Do not treat this as enterprise authentication."),
        ("Baserow", "BASEROW_API_TOKEN, BASEROW_BASE_URL, BASEROW_TABLE_ID", "Optional; used only by selected scripts."),
    ], [1450, 4520, 3390])
    add_callout(doc, "Secret handling", "Transfer credentials through an approved password/secret manager. Do not place values in this document, Git, screenshots, tickets, or chat. Rotate secrets after ownership transfer where practical.", RED)

    doc.add_heading("Deployment model", level=2)
    bullet(doc, "Python 3.12 slim container runs uvicorn app.main:app on port 8080.")
    bullet(doc, "make deploy invokes scripts/deploy_cloud_run.sh, builds/pushes a linux/amd64 image, then deploys to Cloud Run.")
    bullet(doc, "Required deploy-time shell values: PROJECT_ID, REGION, REPO, SERVICE; TAG should be an immutable commit SHA rather than latest.")
    bullet(doc, "The deploy helper does not itself configure Cloud Run environment variables, secrets, scaling, service account, ingress, timeout, concurrency, domain mappings, or scheduled triggers. Those live outside this repository and must be exported/documented.")

    doc.add_heading("Access-transfer checklist", level=2)
    for item in (
        "Repository admin/maintainer ownership and branch protection",
        "GCP project IAM, billing visibility, Cloud Run, Artifact Registry, logs, Secret Manager and any scheduler",
        "MongoDB organization/project, user/role, backups, network rules and alerting",
        "eBay developer application, production keys, RuName and notification destinations",
        "Shopify store/admin and custom app credentials/scopes",
        "Etsy developer application, shop admin, redirect URIs and webhook subscription/signing secret",
        "OpenAI project/key ownership and spend controls, if enabled",
        "Domain/DNS and any external monitoring or alert destinations",
    ):
        bullet(doc, item)
    doc.add_page_break()

    section(doc, "6. MongoDB collection map")
    add_table(doc, ["Collection", "Purpose / ownership"], [
        ("product_raw", "Source-shaped eBay listings and ingestion state."),
        ("product_normalized", "Canonical per-SKU record and channel linkage/state."),
        ("sync_log", "Processing summaries and historical sync output."),
        ("ebay_oauth_tokens / etsy_oauth_tokens", "Primary OAuth token documents."),
        ("ebay_notification_events / etsy_notification_events", "Raw webhook audit and processing status."),
        ("ebay_listing_sync_queue", "Near-real-time ItemListed normalization/Shopify work."),
        ("inventory_events", "Idempotent multichannel sale/inventory events."),
        ("channel_sync_jobs", "Durable per-target inventory fan-out jobs."),
        ("inventory_conflict_policies", "Per-SKU source-of-truth/conflict choices."),
        ("inventory_policy_history", "Audit history for conflict-policy changes."),
        ("shopify_inventory_zero_guard", "Duplicate/unsafe zero-update guard state."),
        ("etsy_match_review", "Human matching decisions."),
        ("etsy_publish_prep_cache", "Cached preparation/AI-derived Etsy fields."),
        ("etsy_bulk_reports", "Bulk validation and creation reports."),
        ("etsy_publish_jobs", "Durable Etsy draft-creation jobs."),
    ], [3200, 6160])
    doc.add_heading("Backup and retention questions to close", level=2)
    bullet(doc, "What automated MongoDB backups exist, what is their retention, and has restoration been tested?")
    bullet(doc, "How long should raw webhook bodies, request headers, event records, jobs, reports, and logs be retained?")
    bullet(doc, "Do webhook headers or payloads contain personal data that requires minimization or deletion controls?")
    bullet(doc, "Which indexes exist in production for SKU, status, timestamps, listing IDs and queue claim patterns? Index declarations are not centralized in the repository.")
    doc.add_page_break()

    section(doc, "7. Known risks and recommended priorities")
    add_table(doc, ["Priority", "Finding", "Recommended action"], [
        ("P0", "eBay webhook verification token and production endpoint URL are hard-coded in source; webhook challenge logs the token.", "Move values to Secret Manager/environment, stop logging secrets, rotate the token, and validate signatures/authenticity for notifications."),
        ("P0", "Several UI/reporting routes and legacy /sync routes are not covered by the prod_router passkey dependency; passkey absence defaults to allow.", "Use authenticated Cloud Run/IAP or equivalent ingress, inventory all routes, and require deny-by-default authorization for every mutating/operational endpoint."),
        ("P0", "Webhook work uses asyncio.create_task after acknowledging the request.", "Move processing to a durable queue/Cloud Tasks/Pub/Sub; Cloud Run instance termination can lose in-process work."),
        ("P1", "POST /sync/prod/run-all is broken by the _run_all_steps signature/undefined variable defect.", "Fix and add an endpoint test before use."),
        ("P1", "Scheduler is disabled and its full_sync implementation is a placeholder.", "Choose one scheduling model, implement the real orchestration, add locking/idempotency, and monitor executions."),
        ("P1", "README describes Etsy as not integrated, contrary to current code.", "Update the README after the intended baseline is committed; keep this handoff document versioned."),
        ("P1", "Logging stores raw Etsy webhook headers/body and emits diagnostic signature material.", "Redact sensitive headers and personal data; define retention and access controls."),
        ("P1", "No broad automated test suite or CI configuration is visible.", "Add unit tests for identity/idempotency/quantity rules, mocked channel integrations, route authorization tests, and CI gates."),
        ("P2", "Reporting route is very large and couples Etsy draft logic to queue workers through a lazy route import.", "Extract domain services and typed request/response models; keep HTTP routing thin."),
        ("P2", "Deployment script omits runtime configuration and rollback validation.", "Add infrastructure-as-code or an exported, reviewed Cloud Run configuration with immutable tags."),
    ], [850, 4120, 4390])
    add_callout(doc, "Do not assume", "The repository alone does not prove what revision is live, whether an external scheduler exists, whether MongoDB backups/alerts are configured, or which environment variables and IAM policies are currently applied in Cloud Run.", GOLD)
    doc.add_page_break()

    section(doc, "8. Verification and change procedure")
    doc.add_heading("Minimum pre-change test", level=2)
    for item in (
        "Create a fresh branch from the agreed handoff tag and record the production revision/image digest.",
        "Run the automated tests and add a regression test for the intended change.",
        "Exercise the change with mocked/sandbox APIs or a controlled SKU and dry-run flags where available.",
        "Compare MongoDB before/after documents and confirm idempotency by repeating the same event/request.",
        "Deploy an immutable image tag, observe logs and queues, then validate the live item in every linked channel.",
        "Retain the prior Cloud Run revision until the observation window is complete.",
    ):
        numbered(doc, item)
    doc.add_heading("High-risk change gates", level=2)
    add_table(doc, ["Change", "Required evidence before execution"], [
        ("Zero inventory", "Exact SKU scope, source-of-truth proof, current quantities on all channels, backup/export, approval and reversal steps."),
        ("Bulk Etsy publish", "Validation report, required fields, shipping/return policies, images, taxonomy, sample draft, rate-limit plan and duplicate guard."),
        ("Shopify purge", "Explicit business authorization, exact target store, full product export/backup, protected route, rollback/reimport plan."),
        ("SKU/link correction", "Cross-channel IDs and order history verified; downstream jobs paused; reconciliation plan documented."),
        ("Webhook change", "Provider challenge/signature test, replay/idempotency test, durable persistence and alerting."),
    ], [2200, 7160])
    doc.add_heading("Current automated test coverage observed", level=2)
    bullet(doc, "tests/test_etsy_publish_queue.py focuses on queue enqueueing, duplicate behavior, already-linked behavior, successful processing, and retry/failure behavior.")
    bullet(doc, "No repository-visible CI workflow was identified in the reviewed file list.")
    bullet(doc, "The handoff document was prepared by source inspection; it does not assert successful live API calls or production-state verification.")
    doc.add_page_break()

    section(doc, "9. Ownership and open questions")
    add_table(doc, ["Decision / fact to capture", "Owner / answer"], [
        ("Named primary and backup technical owners", "____________________________"),
        ("Production URL, GCP project, region, service and Artifact Registry repository", "____________________________"),
        ("Live Git commit/image digest and handoff tag", "____________________________"),
        ("External scheduler/trigger and cadence, if any", "____________________________"),
        ("MongoDB backup, restore, retention and alerting policy", "____________________________"),
        ("Canonical inventory source and conflict policy by scenario", "____________________________"),
        ("Alert destination and incident escalation contacts", "____________________________"),
        ("Expected order/listing volume and acceptable synchronization delay", "____________________________"),
        ("Required compliance/privacy retention for webhook and order data", "____________________________"),
        ("Business approval authority for purge, zeroing and bulk publish operations", "____________________________"),
    ], [5200, 4160])
    doc.add_heading("Recommended handoff session agenda (90 minutes)", level=2)
    add_table(doc, ["Time", "Topic"], [
        ("0–15 min", "Business purpose, current channel workflows, source-of-truth rules and known pain points."),
        ("15–35 min", "Walk through production architecture, Cloud Run configuration, MongoDB and marketplace applications."),
        ("35–55 min", "Demonstrate health/auth checks, dashboards, one controlled sync, queue inspection and incident replay."),
        ("55–70 min", "Review risks, open bugs, current working-tree changes and next priorities."),
        ("70–85 min", "Transfer access using the password manager and verify the new owner can independently reach each system."),
        ("85–90 min", "Agree on acceptance, backup owner and a short post-handoff support window."),
    ], [1600, 7760])
    add_callout(doc, "Acceptance criterion", "The handoff is complete when the new owner can identify the live revision, verify channel credentials without seeing secret values here, trace one SKU end to end, explain recovery for a failed inventory job, and safely deploy/roll back.", GREEN)
    doc.add_page_break()

    section(doc, "Appendix A. Endpoint quick reference")
    add_table(doc, ["Area", "Representative endpoints"], [
        ("Health/auth", "GET /health; GET /auth/{ebay|etsy}/status; GET /auth/{ebay|etsy}/login; passkey login/status/logout."),
        ("Batch sync", "POST /sync/prod/sync-ebay-raw; normalize-raw; sync-shopify; sync-shopify-new; sync-shopify-inventory; shopify-health."),
        ("Inventory control", "GET dashboard/command-center/timeline/policy; POST ingest-sale/run-worker/replay-failed/replay-unresolved/reconcile; PUT policy."),
        ("Etsy publish", "POST enqueue/{sku}; run-worker; reporting queue/detail/suggest-taxonomy/create-draft/bulk validate/create/report."),
        ("Webhooks", "GET|POST /webhooks/ebay/orders; POST /webhooks/etsy/events."),
        ("Audit/reporting", "GET audit summary/sold-still-live/duplicates/missing-skus/unlinked/publish-queue/low-res-images; reporting and channel comparison endpoints."),
        ("Danger", "POST /sync/prod/purge-shopify and legacy /sync/purge-shopify. Treat as destructive."),
    ], [1900, 7460])
    doc.add_heading("Appendix B. Key source map", level=1)
    add_table(doc, ["File / directory", "Why it matters"], [
        ("app/main.py", "Application lifecycle, logging, static pages and scheduler state."),
        ("app/config.py", "Configuration contract and environment variables."),
        ("app/api/routes/sync.py", "Production and legacy sync/worker/control endpoints."),
        ("app/api/routes/webhooks.py", "eBay/Etsy webhook intake and the hard-coded verification values."),
        ("app/api/routes/reporting.py", "Etsy matching, publish prep, AI assistance, bulk creation and reporting."),
        ("app/services/multichannel_sync_service.py", "Canonical inventory events, policies, durable jobs, replay and fan-out."),
        ("app/services/etsy_publish_queue.py", "Durable Etsy draft-publish jobs and retry policy."),
        ("app/services/shopify_sync.py", "Shopify product/inventory synchronization."),
        ("app/services/ebay_webhook_service.py", "ItemListed and eBay order processing."),
        ("scripts/", "Operational backfills, audits, repair and deployment helpers; review each before use."),
        ("static/", "Operator-facing HTML dashboards and review tools."),
    ], [3500, 5860])
    doc.add_paragraph("End of handoff document.").alignment = WD_ALIGN_PARAGRAPH.CENTER

    doc.core_properties.title = "eBay–Shopify–Etsy Synchronization Platform — Project Handoff and System Design"
    doc.core_properties.subject = "Engineering and operational handoff"
    doc.core_properties.author = "Prepared from repository inspection"
    doc.core_properties.comments = "Credentials intentionally omitted."
    doc.save(OUT)
    print(OUT)


if __name__ == "__main__":
    main()
