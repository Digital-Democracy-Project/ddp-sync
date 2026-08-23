"""Tests for OPEN-138: the two session-window bugs that made is_in_session() unsafe to gate on.

First coverage this module has had -- nothing in tests/ touched the calendar before, which is
some of why both bugs survived.

Scope note: OPEN-138 lists six findings. This covers the two the ticket sequenced first, and
deliberately not the rest. The remaining four (the API returning bool rather than a
three-state answer, ValueError being swallowed, biennial off-year handling, and making cache
warming observable) all change the shape of the public API or its callers, and the ticket says
to do these two and stop.
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from ddp_sync.services.legislative_calendar import (
    DEFAULT_PREFILING_DAYS,
    PREFILING_DAYS_BY_STATE,
    StateLegislativeCalendar,
)


class _Session:
    def __init__(self, identifier, start_date, end_date, classification="primary"):
        self.identifier = identifier
        self.start_date = start_date
        self.end_date = end_date
        self.classification = classification
        self.name = identifier


class _Juris:
    def __init__(self, sessions):
        self.sessions = sessions


@pytest.fixture
def cal():
    return StateLegislativeCalendar()


# -- Finding 1: no pre-filing window ---------------------------------------------------------
#
# Bills are drafted and published up to ~six weeks before a session convenes, so "out of
# session" never meant "no bills to collect". Both paths tested `start_date <= date`.

@pytest.mark.parametrize("state", ["AR", "NM", "WY", "FL", "VA", "UT", "AZ", "GA"])
def test_six_weeks_before_convene_is_not_in_session_by_default(cal, state):
    """Default behaviour is unchanged -- existing callers must not shift under them."""
    convene = cal.get_session_dates(state, 2026)["start_date"]
    assert cal.is_in_session(state, convene - timedelta(days=42)) is False


@pytest.mark.parametrize("state", ["AR", "NM", "WY", "FL", "VA", "UT", "AZ", "GA"])
def test_six_weeks_before_convene_counts_when_prefiling_is_requested(cal, state):
    convene = cal.get_session_dates(state, 2026)["start_date"]
    assert cal.is_in_session(state, convene - timedelta(days=42), include_prefiling=True) is True


def test_prefiling_window_spanning_a_year_boundary(cal):
    """Most states convene in January, so their pre-filing window sits in DECEMBER of the
    previous year -- a year that has no session of its own. Checking that year's session dates
    finds nothing, so the lookup has to look forward a year before concluding."""
    convene = cal.get_session_dates("FL", 2026)["start_date"]
    december = convene - timedelta(days=42)
    assert december.year == 2025 and december.month == 12
    assert cal.is_in_session("FL", december, include_prefiling=True) is True


def test_the_day_before_the_prefiling_window_opens_is_still_out(cal):
    convene = cal.get_session_dates("AR", 2026)["start_date"]
    just_outside = convene - timedelta(days=DEFAULT_PREFILING_DAYS + 1)
    assert cal.is_in_session("AR", just_outside, include_prefiling=True) is False


def test_prefiling_does_not_extend_past_the_end_of_a_session(cal):
    """The window opens early; it must not also keep a finished session open."""
    end = cal.get_session_dates("AR", 2026)["end_date"]
    day_after = end + timedelta(days=1)
    # True only if the NEXT session's window has opened, which in April it has not.
    assert cal.is_in_session("AR", day_after, include_prefiling=True) is False


def test_prefiling_window_is_per_state_and_defaults(cal):
    assert cal.prefiling_days("AR") == DEFAULT_PREFILING_DAYS
    assert cal.prefiling_days("ar") == DEFAULT_PREFILING_DAYS  # case-insensitive
    assert PREFILING_DAYS_BY_STATE == {}, (
        "no per-state override should ship without evidence for that state"
    )


def test_a_per_state_override_is_honoured(cal, monkeypatch):
    monkeypatch.setitem(PREFILING_DAYS_BY_STATE, "AR", 7)
    convene = cal.get_session_dates("AR", 2026)["start_date"]
    assert cal.prefiling_days("AR") == 7
    assert cal.is_in_session("AR", convene - timedelta(days=7), include_prefiling=True) is True
    # 6 weeks out is now outside AR's own shorter window.
    assert cal.is_in_session("AR", convene - timedelta(days=42), include_prefiling=True) is False


def test_biennial_off_year_still_sees_the_next_sessions_prefiling_window(cal):
    """MT/ND/NV/TX report zero in-session days for all of 2026 (OPEN-138 finding 2). That is a
    separate finding and not fixed here -- but December of an off year IS inside the next
    session's pre-filing window, and that much should be visible."""
    for state in ["MT", "ND", "NV", "TX"]:
        convene_2027 = cal.get_session_dates(state, 2027)["start_date"]
        assert convene_2027 is not None
        december = convene_2027 - timedelta(days=30)
        assert cal.is_in_session(state, december, include_prefiling=True) is True, state


# -- Finding 3: stale live data suppressed the hardcoded fallback -----------------------------


def test_live_data_about_other_years_falls_through_instead_of_answering_false(cal):
    """The reproduction from the ticket. AR's live data holds only a 2023 session; asked about
    2026-04-20 it used to answer a definite False, and the fallback -- which correctly says
    True -- never ran. Failing toward silence exactly when upstream data is incomplete."""
    fresh = StateLegislativeCalendar()
    assert fresh.is_in_session("AR", date(2026, 4, 20)) is True, "fallback baseline"

    stale = StateLegislativeCalendar()
    stale.warm_cache({"AR": _Juris([_Session("2023", "2023-01-09", "2023-04-07")])})
    assert stale._check_live_sessions("AR", date(2026, 4, 20)) is None
    assert stale.is_in_session("AR", date(2026, 4, 20)) is True


def test_live_data_covering_the_year_still_answers_a_real_false(cal):
    """The other half: when the live data IS about the year in question and says the state is
    not sitting, that is a real answer and must not fall through to a guess."""
    c = StateLegislativeCalendar()
    c.warm_cache({"AR": _Juris([_Session("2026", "2026-04-13", "2026-05-15")])})
    assert c._check_live_sessions("AR", date(2026, 8, 1)) is False
    assert c.is_in_session("AR", date(2026, 8, 1)) is False


def test_live_data_covering_the_year_is_used_when_it_says_in_session(cal):
    c = StateLegislativeCalendar()
    c.warm_cache({"AR": _Juris([_Session("2026", "2026-04-13", "2026-05-15")])})
    assert c.is_in_session("AR", date(2026, 4, 20)) is True


def test_a_year_named_only_in_the_identifier_still_counts_as_covered(cal):
    """A "2025-2026" session whose dates both land in 2025 is still data ABOUT 2026."""
    c = StateLegislativeCalendar()
    c.warm_cache({"MI": _Juris([_Session("2025-2026", "2025-01-08", "2025-12-31")])})
    assert c._check_live_sessions("MI", date(2026, 6, 1)) is True


def test_no_live_data_at_all_still_falls_through(cal):
    c = StateLegislativeCalendar()
    assert c._check_live_sessions("AR", date(2026, 4, 20)) is None


def test_prefiling_applies_to_the_live_path_too(cal):
    """Both paths had the bug, so both need the window -- not just the fallback."""
    c = StateLegislativeCalendar()
    c.warm_cache({"AR": _Juris([_Session("2026", "2026-04-13", "2026-05-15")])})
    six_weeks_early = date(2026, 3, 2)
    assert c.is_in_session("AR", six_weeks_early) is False
    assert c.is_in_session("AR", six_weeks_early, include_prefiling=True) is True


def test_unknown_jurisdictions_are_unchanged(cal):
    """DC/PR/US are absent from the table. OPEN-138 finding 4 covers the swallowed ValueError;
    this only pins that these two fixes did not alter that behaviour."""
    for code in ["DC", "PR", "US", "XX"]:
        assert cal.is_in_session(code) is False
        assert cal.is_in_session(code, include_prefiling=True) is False
