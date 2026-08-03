"""Well-Architected PDF report generator — matches v2 UI exactly."""
import io
import os
from datetime import datetime

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    HRFlowable, PageBreak, Paragraph, SimpleDocTemplate,
    Spacer, Table, TableStyle, KeepTogether,
)

W, H = A4
_M  = 16 * mm
_CW = W - 2 * _M

# ── Fonts ─────────────────────────────────────────────────────────────────────
_FONT_DIR = "/Library/Fonts"
_F  = "Helvetica"
_FB = "Helvetica-Bold"
_FI = "Helvetica-Oblique"

try:
    _candidates = {
        "Ember":      os.path.join(_FONT_DIR, "AmazonEmber_Rg.ttf"),
        "Ember-Bold": os.path.join(_FONT_DIR, "AmazonEmber_Bd.ttf"),
        "Ember-It":   os.path.join(_FONT_DIR, "AmazonEmber_RgIt.ttf"),
    }
    if all(os.path.exists(p) for p in _candidates.values()):
        for name, path in _candidates.items():
            pdfmetrics.registerFont(TTFont(name, path))
        _F, _FB, _FI = "Ember", "Ember-Bold", "Ember-It"
except Exception:
    pass

# ── Colours ───────────────────────────────────────────────────────────────────
_INK     = colors.HexColor("#16191F")
_BODY    = colors.HexColor("#414D5C")
_MUTED   = colors.HexColor("#687078")
_RULE    = colors.HexColor("#D5DBDB")
_SURFACE = colors.HexColor("#F8F8F8")
_ORANGE  = colors.HexColor("#EC7211")
_WHITE   = colors.white

_C_FAIL  = colors.HexColor("#D13212")
_C_WARN  = colors.HexColor("#906806")
_C_PASS  = colors.HexColor("#1D8102")
_C_INFO  = colors.HexColor("#687078")

_PILLAR_BG = {
    "Reliability":            colors.HexColor("#d45b07"),
    "Security":               colors.HexColor("#dd344c"),
    "Operational Excellence": colors.HexColor("#5e6b7a"),
    "Performance Efficiency": colors.HexColor("#8c4fff"),
    "Cost Optimization":      colors.HexColor("#067f68"),
    "Sustainability":         colors.HexColor("#0972d3"),
    "Other":                  colors.HexColor("#5e6b7a"),
}

# Question group heading colours — match UI left-border colours
_Q_FAIL_BG  = colors.HexColor("#FFF5F5")
_Q_WARN_BG  = colors.HexColor("#FFFBF0")
_Q_PASS_BG  = colors.HexColor("#F8F8F8")

_STATUS_COLOR = {"pass": _C_PASS, "warn": _C_WARN, "fail": _C_FAIL, "info": _C_INFO}
_STATUS_SYM   = {"pass": "v",     "warn": "!",      "fail": "X",     "info": "i"}
_PRI_COLOR    = {"Critical": _C_FAIL, "High": _C_FAIL, "Medium": _C_WARN, "Low": _C_INFO}

# ── Styles ────────────────────────────────────────────────────────────────────
def _p(name, font=None, size=9, color=None, align=TA_LEFT, leading=None, **kw):
    args = dict(fontName=font or _F, fontSize=size,
                textColor=color or _BODY, alignment=align)
    if leading:
        args["leading"] = leading
    args.update(kw)
    return ParagraphStyle(name, **args)

S_TITLE   = _p("title",  _FB, 14, _INK,   spaceAfter=0)
S_META    = _p("meta",   _F,  8,  _MUTED, spaceAfter=0)
S_PILLAR  = _p("pillar", _FB, 9,  _INK)
S_Q_HEAD  = _p("qhead",  _FB, 7,  _MUTED, leading=9)
S_ID      = _p("cid",    _FB, 6,  _MUTED, leading=9)
S_LABEL   = _p("label",  _FB, 8,  _INK,   leading=11)
S_DETAIL  = _p("detail", _F,  7,  _MUTED, leading=10)
S_AI_ACT  = _p("ai_act", _FB, 8,  _INK,   leading=11)
S_AI_SUB  = _p("ai_sub", _F,  7,  _MUTED, leading=10)
S_NOTE    = _p("note",   _FI, 8,  _MUTED, leading=11)
S_TH      = _p("th",     _FB, 7,  _MUTED)
S_TC      = _p("tc",     _F,  8,  _BODY,  TA_CENTER)
S_TCB     = _p("tcb",    _FB, 8,  _INK,   TA_CENTER)


def _hr(color=_RULE, thick=0.5, before=2, after=2):
    return HRFlowable(width="100%", thickness=thick, color=color,
                      spaceBefore=before, spaceAfter=after)


def _tbl_style(extra=None):
    base = [
        ("BOX",           (0, 0), (-1, -1), 0.5, _RULE),
        ("LINEBELOW",     (0, 0), (-1, -1), 0.3, _RULE),
        ("ROWBACKGROUNDS",(0, 0), (-1, -1), [_WHITE, _SURFACE]),
        ("VALIGN",        (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING",    (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ("LEFTPADDING",   (0, 0), (-1, -1), 4),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 4),
    ]
    if extra:
        base.extend(extra)
    return TableStyle(base)


# ── Cover ─────────────────────────────────────────────────────────────────────

def _cover(story, results, cluster_id, region, has_ai):
    from tabs.wa_v2.base import actionable_counts
    n_pass, n_warn, n_fail, n_info, actionable, score = actionable_counts(results)
    s_col = _C_PASS if score >= 80 else _C_WARN if score >= 60 else _C_FAIL

    story.append(Spacer(1, 3 * mm))
    story.append(Paragraph("Well-Architected Review", S_TITLE))
    story.append(Spacer(1, 2 * mm))
    story.append(_hr(_ORANGE, 2, 0, 3))
    story.append(Paragraph(
        f"Cluster: <b>{cluster_id}</b>  |  Region: <b>{region}</b>  |  "
        f"{datetime.utcnow().strftime('%d %b %Y  %H:%M UTC')}",
        S_META))
    story.append(Spacer(1, 5 * mm))

    def _stat(val, lbl, col):
        return [
            Paragraph(str(val), _p("sv", _FB, 11, col, TA_CENTER)),
            Paragraph(lbl,      _p("sl", _F,  9,  _MUTED, TA_CENTER)),
        ]

    summary = Table(
        [[
            [Paragraph(f"{score}%",    _p("sc", _FB, 12, s_col, TA_CENTER)),
             Paragraph("Health Score", _p("sh", _F,  9,  _MUTED, TA_CENTER))],
            _stat(f"v {n_pass}",    "Passed",   _C_PASS),
            _stat(f"! {n_warn}",    "Warnings", _C_WARN),
            _stat(f"X {n_fail}",    "Failed",   _C_FAIL),
            _stat(str(actionable),  "Checks",   _MUTED),
            _stat(str(n_info),      "Info",     _C_INFO),
        ]],
        colWidths=[_CW*0.24, _CW*0.15, _CW*0.15, _CW*0.15, _CW*0.155, _CW*0.155],
    )
    summary.setStyle(TableStyle([
        ("BOX",           (0, 0), (-1, -1), 0.5, _RULE),
        ("LINEAFTER",     (0, 0), (-2, -1), 0.5, _RULE),
        ("LINEBEFORE",    (-1, 0),(-1, -1), 0.5, _RULE),
        ("BACKGROUND",    (0, 0), (-1, -1), _SURFACE),
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING",    (0, 0), (-1, -1), 7),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
    ]))
    story.append(summary)
    story.append(Spacer(1, 5 * mm))

    # Per-pillar summary table — use same suppression as actionable_counts
    from tabs.wa_v2.base import ID_REMAP
    pillars = {}
    for r in results:
        if r.get("pillar") == "Other":
            continue
        raw_id   = r.get("id", "")
        remapped = ID_REMAP.get(raw_id, raw_id)
        if remapped is None:
            continue   # suppressed
        pillars.setdefault(r["pillar"], []).append(r)

    pillar_order = ["Reliability", "Security", "Operational Excellence",
                    "Performance Efficiency", "Cost Optimization", "Sustainability"]
    ordered = [p for p in pillar_order if p in pillars]

    rows = [[Paragraph(h, S_TH) for h in ["Pillar", "v Pass", "! Warn", "X Fail", "Info", "Score"]]]
    for pname in ordered:
        chks = pillars[pname]
        pp = sum(1 for c in chks if c["status"] == "pass")
        pw = sum(1 for c in chks if c["status"] == "warn")
        pf = sum(1 for c in chks if c["status"] == "fail")
        pi = sum(1 for c in chks if c["status"] == "info")
        pt = pp + pw + pf
        ps = int(pp / pt * 100) if pt else 0
        pc = _C_PASS if ps >= 80 else _C_WARN if ps >= 60 else _C_FAIL
        rows.append([
            Paragraph(pname,    _p(f"pn{pname}", _FB, 8, _INK)),
            Paragraph(str(pp),  _p(f"pp{pname}", _FB, 8, _C_PASS, TA_CENTER)),
            Paragraph(str(pw),  _p(f"pw{pname}", _FB, 8, _C_WARN, TA_CENTER)),
            Paragraph(str(pf),  _p(f"pf{pname}", _FB, 8, _C_FAIL, TA_CENTER)),
            Paragraph(str(pi),  _p(f"pi{pname}", _F,  8, _C_INFO, TA_CENTER)),
            Paragraph(f"{ps}%", _p(f"ps{pname}", _FB, 8, pc,      TA_CENTER)),
        ])

    pt = Table(rows, colWidths=[_CW*0.40, _CW*0.11, _CW*0.11, _CW*0.11, _CW*0.11, _CW*0.16])
    pt.setStyle(TableStyle([
        ("BOX",           (0, 0), (-1, -1), 0.5, _RULE),
        ("LINEBELOW",     (0, 0), (-1, 0),  0.8, _RULE),
        ("LINEBELOW",     (0, 1), (-1, -1), 0.3, _RULE),
        ("LINEAFTER",     (0, 0), (-2, -1), 0.3, _RULE),
        ("ROWBACKGROUNDS",(0, 1), (-1, -1), [_WHITE, _SURFACE]),
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING",    (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("LEFTPADDING",   (0, 0), (0, -1),  6),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 4),
    ]))
    story.append(pt)

    if not has_ai:
        story.append(Spacer(1, 4 * mm))
        note = Table([[Paragraph(
            "This report does not include AI recommendations. "
            "Connect to the cluster and run the review to generate "
            "AI-powered recommendations.", S_NOTE)]],
            colWidths=[_CW])
        note.setStyle(TableStyle([
            ("BOX",           (0, 0), (-1, -1), 0.5, _RULE),
            ("LINEBEFORE",    (0, 0), (0, -1),  3,   _C_WARN),
            ("BACKGROUND",    (0, 0), (-1, -1), _SURFACE),
            ("TOPPADDING",    (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ("LEFTPADDING",   (0, 0), (-1, -1), 8),
            ("RIGHTPADDING",  (0, 0), (-1, -1), 8),
        ]))
        story.append(note)

    story.append(Spacer(1, 8 * mm))


# ── Pillar sections ───────────────────────────────────────────────────────────

def _pillar_section(pillar_name, groups, story):
    """Render one pillar: header + question sub-headings + check rows.

    groups: list of (headline, [check, ...]) already normalised by normalise_for_pdf()
    """
    from tabs.wa_v2.base import DOC_LINKS
    # Aggregate counts across all groups for this pillar
    all_checks = [c for _, checks in groups for c in checks]
    n_fail = sum(1 for c in all_checks if c["status"] == "fail")
    n_warn = sum(1 for c in all_checks if c["status"] == "warn")
    n_pass = sum(1 for c in all_checks if c["status"] == "pass")
    n_info = sum(1 for c in all_checks if c["status"] == "info")

    bg = _PILLAR_BG.get(pillar_name, _INK)

    count_data = [[
        Paragraph(f"Pass {n_pass}", _p(f"cp{pillar_name}", _FB, 8, _C_PASS, TA_CENTER)),
        Paragraph(f"Warn {n_warn}", _p(f"cw{pillar_name}", _FB, 8, _C_WARN, TA_CENTER)),
        Paragraph(f"Fail {n_fail}", _p(f"cf{pillar_name}", _FB, 8, _C_FAIL, TA_CENTER)),
        Paragraph(f"Info {n_info}", _p(f"ci{pillar_name}", _F,  8, _C_INFO, TA_CENTER)),
    ]]
    counts_tbl = Table(count_data, colWidths=[_CW * 0.35 / 4] * 4)
    counts_tbl.setStyle(TableStyle([
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING",    (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
        ("LEFTPADDING",   (0, 0), (-1, -1), 2),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 2),
    ]))
    hdr = Table(
        [[Paragraph(pillar_name, S_PILLAR), counts_tbl]],
        colWidths=[_CW * 0.65, _CW * 0.35],
    )
    hdr.setStyle(TableStyle([
        ("LINEBELOW",     (0, 0), (-1, -1), 2, bg),
        ("TOPPADDING",    (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("LEFTPADDING",   (0, 0), (0, -1),  0),
        ("RIGHTPADDING",  (-1, 0),(-1, -1), 0),
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
    ]))

    first_element = True
    for headline, checks in groups:
        if not checks:
            continue

        # Question group heading — coloured left border matching UI
        has_fail = any(c["status"] == "fail" for c in checks)
        has_warn = any(c["status"] == "warn" for c in checks)
        q_border = _C_FAIL if has_fail else _C_WARN if has_warn else _MUTED
        q_bg     = _Q_FAIL_BG if has_fail else _Q_WARN_BG if has_warn else _Q_PASS_BG

        q_hdr = Table(
            [[Paragraph(headline, S_Q_HEAD)]],
            colWidths=[_CW],
        )
        q_hdr.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (-1, -1), q_bg),
            ("LINEBEFORE",    (0, 0), (0, -1),  3, q_border),
            ("LINEBELOW",     (0, 0), (-1, -1), 0.3, _RULE),
            ("TOPPADDING",    (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ("LEFTPADDING",   (0, 0), (-1, -1), 6),
            ("RIGHTPADDING",  (0, 0), (-1, -1), 4),
        ]))

        # Check rows
        data = []
        for c in checks:
            status = c["status"]
            col    = _STATUS_COLOR[status]
            sym    = _STATUS_SYM[status]

            sym_p = Paragraph(sym, _p(f"sym{id(c)}", _FB, 9, col, TA_CENTER))
            id_p  = Paragraph(c.get("id", ""), S_ID)

            text_rows = [[Paragraph(c["label"], S_LABEL)]]
            if c.get("detail"):
                text_rows.append([Paragraph(c["detail"], S_DETAIL)])
            _doc_url = DOC_LINKS.get(c.get("id", ""))
            if _doc_url:
                _safe_url = _doc_url.replace("&", "&amp;")
                text_rows.append([Paragraph(
                    f'<link href="{_safe_url}" color="#0972d3">Guidance doc \u2192</link>',
                    S_DETAIL)])
            text_cell = Table(text_rows, colWidths=[_CW - 24 * mm])
            text_cell.setStyle(TableStyle([
                ("TOPPADDING",    (0, 0), (-1, -1), 0),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
                ("LEFTPADDING",   (0, 0), (-1, -1), 0),
                ("RIGHTPADDING",  (0, 0), (-1, -1), 0),
            ]))
            data.append([sym_p, id_p, text_cell])

        if not data:
            continue

        row_style = _tbl_style()
        first_row = Table([data[0]], colWidths=[8 * mm, 14 * mm, _CW - 24 * mm])
        first_row.setStyle(row_style)

        if first_element:
            story.append(KeepTogether([hdr, q_hdr, first_row]))
            first_element = False
        else:
            story.append(KeepTogether([q_hdr, first_row]))

        if len(data) > 1:
            rest = Table(data[1:], colWidths=[8 * mm, 14 * mm, _CW - 24 * mm])
            rest.setStyle(row_style)
            story.append(rest)

    story.append(Spacer(1, 3 * mm))


# ── AI recommendations ────────────────────────────────────────────────────────

def _ai_section(ai_data, story):
    if not ai_data or not isinstance(ai_data, dict):
        return

    story.append(PageBreak())

    ai_hdr = Table(
        [[Paragraph("AI-Powered Recommendations", _p("aih", _FB, 11, _WHITE))]],
        colWidths=[_CW],
    )
    ai_hdr.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, -1), _INK),
        ("TOPPADDING",    (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ("LEFTPADDING",   (0, 0), (-1, -1), 8),
    ]))
    story.append(ai_hdr)
    story.append(Spacer(1, 3 * mm))

    pillar_order = ["Reliability", "Security", "Operational Excellence",
                    "Performance Efficiency", "Cost Optimization", "Sustainability"]
    ordered = [p for p in pillar_order if p in ai_data] + \
              [p for p in ai_data if p not in pillar_order]

    for pillar in ordered:
        recs = ai_data.get(pillar, [])
        if not recs:
            continue

        bg_ai = _PILLAR_BG.get(pillar, _INK)
        ph = Table(
            [[Paragraph(pillar, _p(f"aip{pillar}", _FB, 9, _INK))]],
            colWidths=[_CW],
        )
        ph.setStyle(TableStyle([
            ("LINEBELOW",     (0, 0), (-1, -1), 2, bg_ai),
            ("TOPPADDING",    (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ("LEFTPADDING",   (0, 0), (-1, -1), 0),
            ("RIGHTPADDING",  (0, 0), (-1, -1), 0),
        ]))

        data = []
        for r in recs:
            pri   = r.get("priority", "Medium")
            pcol  = _PRI_COLOR.get(pri, _C_INFO)
            pri_p = Paragraph(pri, _p(f"pri{id(r)}", _FB, 7, pcol, TA_CENTER))

            text_rows = [[Paragraph(r.get("action", ""), S_AI_ACT)]]
            if r.get("why"):
                text_rows.append([Paragraph(f"Why: {r['why']}", S_AI_SUB)])
            if r.get("impact"):
                text_rows.append([Paragraph(f"Impact: {r['impact']}", S_AI_SUB)])

            text_cell = Table(text_rows, colWidths=[_CW - 18 * mm])
            text_cell.setStyle(TableStyle([
                ("TOPPADDING",    (0, 0), (-1, -1), 0),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 1),
                ("LEFTPADDING",   (0, 0), (-1, -1), 0),
                ("RIGHTPADDING",  (0, 0), (-1, -1), 0),
            ]))
            data.append([pri_p, text_cell])

        ai_row_style = _tbl_style()
        first = Table([data[0]], colWidths=[18 * mm, _CW - 18 * mm])
        first.setStyle(ai_row_style)
        story.append(KeepTogether([ph, first]))

        if len(data) > 1:
            rest = Table(data[1:], colWidths=[18 * mm, _CW - 18 * mm])
            rest.setStyle(ai_row_style)
            story.append(rest)

        story.append(Spacer(1, 3 * mm))


# ── Entry point ───────────────────────────────────────────────────────────────

def generate_wa_pdf(results, ai_data, cluster_id, region):
    from tabs.wa_v2.base import normalise_for_pdf

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=_M, rightMargin=_M,
        topMargin=_M, bottomMargin=16 * mm,
        title=f"Well-Architected Review — {cluster_id}",
        author="Prism for Amazon DocumentDB",
        subject="AWS Well-Architected Review",
    )

    def _footer(canvas, doc):
        canvas.saveState()
        canvas.setFont(_F, 7)
        canvas.setFillColor(_MUTED)
        canvas.drawString(_M, 9 * mm,
            f"Prism for Amazon DocumentDB  |  Well-Architected Review  |  {cluster_id}  |  {region}")
        canvas.drawRightString(W - _M, 9 * mm,
            f"Page {doc.page}  |  {datetime.utcnow().strftime('%d %b %Y')}")
        canvas.restoreState()

    # Normalise: apply ID_REMAP, suppression, DETAIL_OVERRIDES, question grouping
    normalised = normalise_for_pdf(results)

    # Group by pillar preserving order
    pillar_groups = {}   # pillar -> [(headline, [checks])]
    for pillar, headline, checks in normalised:
        pillar_groups.setdefault(pillar, []).append((headline, checks))

    story = []
    _cover(story, results, cluster_id, region, has_ai=bool(ai_data))

    pillar_order = ["Reliability", "Security", "Operational Excellence",
                    "Performance Efficiency", "Cost Optimization",
                    "Sustainability"]
    for pname in [p for p in pillar_order if p in pillar_groups] + \
                 [p for p in pillar_groups if p not in pillar_order]:
        _pillar_section(pname, pillar_groups[pname], story)

    _ai_section(ai_data, story)

    doc.build(story, onFirstPage=_footer, onLaterPages=_footer)
    return buf.getvalue()
