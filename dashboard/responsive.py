"""
responsive.py — Mobile Responsiveness Helpers
==============================================
Provides CSS injection, JS-based viewport detection, and layout helpers
to make the Streamlit dashboard usable on small screens (phones/tablets)
without breaking the desktop layout.
"""

import streamlit as st


# ---------------------------------------------------------------------------
# CSS & JS Injection
# ---------------------------------------------------------------------------

_MOBILE_CSS = """
<style>
/* ── Viewport meta ───────────────────────────────────────────── */
/* Streamlit injects its own viewport tag; this reinforces it    */

/* ── Tab bar: horizontal scroll + touch-friendly ─────────────── */
[data-testid="stTabs"] [role="tablist"] {
    overflow-x: auto !important;
    overflow-y: hidden !important;
    flex-wrap: nowrap !important;
    scrollbar-width: none;          /* Firefox */
    -ms-overflow-style: none;       /* IE 10+ */
    padding-bottom: 2px;
}
[data-testid="stTabs"] [role="tablist"]::-webkit-scrollbar {
    display: none;                  /* Chrome/Safari */
}
[data-testid="stTabs"] [role="tab"] {
    white-space: nowrap !important;
    min-height: 44px !important;    /* WCAG touch target */
    padding: 6px 14px !important;
    font-size: 0.85rem !important;
}

/* ── Metric cards: flex-wrap so they stack gracefully ────────── */
@media (max-width: 640px) {
    /* Target the column containers that hold metric cards */
    [data-testid="stHorizontalBlock"] {
        flex-wrap: wrap !important;
        gap: 0.4rem !important;
    }
    [data-testid="stHorizontalBlock"] > [data-testid="stColumn"] {
        min-width: 45% !important;
        flex: 1 1 45% !important;
    }
    /* Shrink metric label font so text doesn't clip */
    [data-testid="stMetricLabel"] {
        font-size: 0.72rem !important;
        line-height: 1.3 !important;
    }
    [data-testid="stMetricValue"] {
        font-size: 1.05rem !important;
    }
    /* Title font size */
    h1 {
        font-size: 1.5rem !important;
    }
    h2 {
        font-size: 1.15rem !important;
    }
    /* Selectbox / multiselect: full width */
    [data-testid="stSelectbox"],
    [data-testid="stMultiSelect"] {
        width: 100% !important;
    }
    /* Data tables: enable horizontal scroll */
    [data-testid="stDataFrame"],
    [data-testid="stDataEditor"] {
        overflow-x: auto !important;
        -webkit-overflow-scrolling: touch;
    }
    /* Price date badges: wrap instead of overflow */
    p[style*="margin-top:-0.6rem"] {
        font-size: 0.75rem !important;
        white-space: normal !important;
        word-break: break-word;
    }
    /* Sidebar toggle button: make it more visible */
    [data-testid="collapsedControl"] {
        top: 0.5rem !important;
    }
    /* Main block padding reduction on small screens */
    .main .block-container {
        padding-left: 0.75rem !important;
        padding-right: 0.75rem !important;
        padding-top: 1rem !important;
    }
}

/* ── Tablet (641px–1023px) ───────────────────────────────────── */
@media (min-width: 641px) and (max-width: 1023px) {
    [data-testid="stHorizontalBlock"] > [data-testid="stColumn"] {
        min-width: 30% !important;
        flex: 1 1 30% !important;
    }
    [data-testid="stMetricLabel"] {
        font-size: 0.8rem !important;
    }
    .main .block-container {
        padding-left: 1rem !important;
        padding-right: 1rem !important;
    }
}

/* ── Sidebar: collapse animation tweak ─────────────────────────── */
[data-testid="stSidebar"] {
    min-width: 240px;
}

/* ── Plotly charts: full width on mobile ─────────────────────── */
@media (max-width: 640px) {
    .js-plotly-plot {
        width: 100% !important;
    }
}

/* ── Status/expander area for pipeline output ────────────────── */
[data-testid="stExpander"] {
    overflow-x: auto !important;
}
</style>
"""


_VIEWPORT_DETECTOR_JS = """
<script>
(function() {
    // Inject window width into ?vw= query param so Python can read it.
    // Fires once on load. A page rerun will then have the correct value.
    var w = window.innerWidth;
    var url = new URL(window.location.href);
    var current = url.searchParams.get('vw');
    if (!current || Math.abs(parseInt(current) - w) > 50) {
        url.searchParams.set('vw', w);
        // Use replaceState so it doesn't add browser history entries
        window.history.replaceState({}, '', url.toString());
    }
})();
</script>
"""


def inject_mobile_css() -> None:
    """
    Inject mobile-responsive CSS + JS viewport detector into the Streamlit page.
    Call this ONCE at the very top of app.py, right after st.set_page_config().
    """
    st.markdown(_MOBILE_CSS, unsafe_allow_html=True)
    st.markdown(_VIEWPORT_DETECTOR_JS, unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Viewport Detection
# ---------------------------------------------------------------------------

def get_viewport_width() -> int:
    """
    Read the viewport width stored by the JS detector in ?vw= query param.
    Returns 1280 (desktop default) if not yet set (first load before JS fires).
    """
    try:
        params = st.query_params
        vw = params.get("vw", "1280")
        return int(str(vw).strip())
    except Exception:
        return 1280


def is_mobile() -> bool:
    """Returns True when the viewport width is <= 640px (phone-class screen)."""
    return get_viewport_width() <= 640


def is_tablet() -> bool:
    """Returns True when viewport is between 641px and 1023px (tablet-class)."""
    w = get_viewport_width()
    return 641 <= w <= 1023


# ---------------------------------------------------------------------------
# Responsive Layout Helpers
# ---------------------------------------------------------------------------

def responsive_cols(n_desktop: int, n_mobile: int = 2, n_tablet: int | None = None):
    """
    Return st.columns() with a column count appropriate for the current viewport.

    Args:
        n_desktop:  Number of columns at full desktop width (>= 1024px).
        n_mobile:   Number of columns on phones (<= 640px). Default 2.
        n_tablet:   Number of columns on tablets (641–1023px).
                    Defaults to midpoint of n_mobile and n_desktop.
    """
    if n_tablet is None:
        n_tablet = max(n_mobile, min(n_desktop, (n_mobile + n_desktop) // 2))

    vw = get_viewport_width()
    if vw <= 640:
        return st.columns(n_mobile)
    elif vw <= 1023:
        return st.columns(n_tablet)
    else:
        return st.columns(n_desktop)


def mobile_show_amounts_toggle(key: str = "mobile_show_amounts") -> bool | None:
    """
    Render a compact 'Show Amounts' toggle at the top of the main content area,
    only when on a mobile viewport. Returns the toggle value, or None if not mobile
    (caller should use the sidebar value instead).
    """
    if is_mobile():
        cols = st.columns([3, 1])
        with cols[1]:
            return st.toggle("👁️", value=False, key=key, help="Show/hide monetary amounts")
    return None
