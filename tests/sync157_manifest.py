"""SYNC-157: the jurisdiction manifest (ddp-open-states jurisdictions.yaml) as ddp-sync reads it, for tests.

The values are copied from the manifest on 2026-11-12 (ddp-open-states SYNC-157 branch), trimmed to the fields ddp-sync
reads. `write(tmp_path, **changes)` writes it as a stand-in checkout and returns its path; set OPENSTATES_ROOT to it
(or pass it as `openstates_root`). `changes` is {"<code>.<section>.<field>": value}.
"""

from __future__ import annotations

import copy

import yaml

MANIFEST = {
 "al": {
  "archive": {
   "enabled": True,
   "timeout_s": 14400
  },
  "enrollment": {
   "broker_bill_event_signal": False,
   "cloud_path": True,
   "embedding": False,
   "legbot_replica": False,
   "replica_routing": False,
   "search_refresh": False
  },
  "name": "Alabama",
  "scrape": {
   "cadence_escalation": True,
   "fargate_max_wait_s": None,
   "full_walk": False,
   "retry": False,
   "sweep_import": False,
   "timeout_s": 21600
  },
  "status": "scraping",
  "waf": {
   "profile": "none",
   "scrapebot_fallback": False
  }
 },
 "az": {
  "archive": {
   "enabled": True,
   "timeout_s": 14400
  },
  "enrollment": {
   "broker_bill_event_signal": False,
   "cloud_path": True,
   "embedding": True,
   "legbot_replica": True,
   "replica_routing": True,
   "search_refresh": True
  },
  "name": "Arizona",
  "scrape": {
   "cadence_escalation": True,
   "fargate_max_wait_s": None,
   "full_walk": False,
   "retry": False,
   "sweep_import": False,
   "timeout_s": 21600
  },
  "status": "live",
  "waf": {
   "profile": "none",
   "scrapebot_fallback": False
  }
 },
 "fl": {
  "archive": {
   "enabled": True,
   "timeout_s": 57600
  },
  "enrollment": {
   "broker_bill_event_signal": False,
   "cloud_path": True,
   "embedding": True,
   "legbot_replica": True,
   "replica_routing": True,
   "search_refresh": True
  },
  "name": "Florida",
  "scrape": {
   "cadence_escalation": True,
   "fargate_max_wait_s": None,
   "full_walk": False,
   "retry": False,
   "sweep_import": True,
   "timeout_s": 57600
  },
  "status": "live",
  "waf": {
   "profile": "cookie_provider",
   "scrapebot_fallback": False
  }
 },
 "la": {
  "archive": {
   "enabled": True,
   "timeout_s": 14400
  },
  "enrollment": {
   "broker_bill_event_signal": False,
   "cloud_path": True,
   "embedding": False,
   "legbot_replica": True,
   "replica_routing": True,
   "search_refresh": False
  },
  "name": "Louisiana",
  "scrape": {
   "cadence_escalation": True,
   "fargate_max_wait_s": None,
   "full_walk": False,
   "retry": False,
   "sweep_import": False,
   "timeout_s": 21600
  },
  "status": "scraping",
  "waf": {
   "profile": "none",
   "scrapebot_fallback": False
  }
 },
 "ma": {
  "archive": {
   "enabled": True,
   "timeout_s": 14400
  },
  "enrollment": {
   "broker_bill_event_signal": False,
   "cloud_path": True,
   "embedding": False,
   "legbot_replica": True,
   "replica_routing": False,
   "search_refresh": False
  },
  "name": "Massachusetts",
  "scrape": {
   "cadence_escalation": True,
   "fargate_max_wait_s": 86400,
   "full_walk": False,
   "retry": True,
   "sweep_import": True,
   "timeout_s": 57600
  },
  "status": "live",
  "waf": {
   "profile": "none",
   "scrapebot_fallback": False
  }
 },
 "mi": {
  "archive": {
   "enabled": True,
   "timeout_s": 86400
  },
  "enrollment": {
   "broker_bill_event_signal": False,
   "cloud_path": True,
   "embedding": True,
   "legbot_replica": True,
   "replica_routing": True,
   "search_refresh": True
  },
  "name": "Michigan",
  "scrape": {
   "cadence_escalation": True,
   "fargate_max_wait_s": None,
   "full_walk": True,
   "retry": False,
   "sweep_import": True,
   "timeout_s": 36000
  },
  "status": "live",
  "waf": {
   "profile": "cookie_provider",
   "scrapebot_fallback": True
  }
 },
 "nc": {
  "archive": {
   "enabled": True,
   "timeout_s": 14400
  },
  "enrollment": {
   "broker_bill_event_signal": False,
   "cloud_path": True,
   "embedding": False,
   "legbot_replica": True,
   "replica_routing": True,
   "search_refresh": True
  },
  "name": "North Carolina",
  "scrape": {
   "cadence_escalation": True,
   "fargate_max_wait_s": None,
   "full_walk": False,
   "retry": False,
   "sweep_import": False,
   "timeout_s": 21600
  },
  "status": "live",
  "waf": {
   "profile": "none",
   "scrapebot_fallback": False
  }
 },
 "ne": {
  "archive": {
   "enabled": True,
   "timeout_s": 14400
  },
  "enrollment": {
   "broker_bill_event_signal": False,
   "cloud_path": True,
   "embedding": False,
   "legbot_replica": True,
   "replica_routing": True,
   "search_refresh": False
  },
  "name": "Nebraska",
  "scrape": {
   "cadence_escalation": True,
   "fargate_max_wait_s": None,
   "full_walk": False,
   "retry": False,
   "sweep_import": False,
   "timeout_s": 21600
  },
  "status": "scraping",
  "waf": {
   "profile": "none",
   "scrapebot_fallback": False
  }
 },
 "pa": {
  "archive": {
   "enabled": True,
   "timeout_s": 14400
  },
  "enrollment": {
   "broker_bill_event_signal": False,
   "cloud_path": True,
   "embedding": False,
   "legbot_replica": True,
   "replica_routing": True,
   "search_refresh": False
  },
  "name": "Pennsylvania",
  "scrape": {
   "cadence_escalation": True,
   "fargate_max_wait_s": None,
   "full_walk": False,
   "retry": False,
   "sweep_import": False,
   "timeout_s": 21600
  },
  "status": "scraping",
  "waf": {
   "profile": "none",
   "scrapebot_fallback": False
  }
 },
 "us": {
  "archive": {
   "enabled": True,
   "timeout_s": 86400
  },
  "enrollment": {
   "broker_bill_event_signal": False,
   "cloud_path": True,
   "embedding": True,
   "legbot_replica": True,
   "replica_routing": True,
   "search_refresh": True
  },
  "name": "United States (Congress)",
  "scrape": {
   "cadence_escalation": True,
   "fargate_max_wait_s": None,
   "full_walk": False,
   "retry": False,
   "sweep_import": False,
   "timeout_s": 14400
  },
  "status": "live",
  "waf": {
   "profile": "none",
   "scrapebot_fallback": False
  }
 },
 "ut": {
  "archive": {
   "enabled": True,
   "timeout_s": 14400
  },
  "enrollment": {
   "broker_bill_event_signal": False,
   "cloud_path": True,
   "embedding": True,
   "legbot_replica": True,
   "replica_routing": True,
   "search_refresh": True
  },
  "name": "Utah",
  "scrape": {
   "cadence_escalation": True,
   "fargate_max_wait_s": None,
   "full_walk": False,
   "retry": False,
   "sweep_import": False,
   "timeout_s": 21600
  },
  "status": "live",
  "waf": {
   "profile": "none",
   "scrapebot_fallback": False
  }
 },
 "va": {
  "archive": {
   "enabled": True,
   "timeout_s": 14400
  },
  "enrollment": {
   "broker_bill_event_signal": False,
   "cloud_path": True,
   "embedding": True,
   "legbot_replica": True,
   "replica_routing": True,
   "search_refresh": True
  },
  "name": "Virginia",
  "scrape": {
   "cadence_escalation": True,
   "fargate_max_wait_s": None,
   "full_walk": False,
   "retry": False,
   "sweep_import": True,
   "timeout_s": 21600
  },
  "status": "live",
  "waf": {
   "profile": "none",
   "scrapebot_fallback": False
  }
 },
 "wa": {
  "archive": {
   "enabled": True,
   "timeout_s": 28800
  },
  "enrollment": {
   "broker_bill_event_signal": False,
   "cloud_path": True,
   "embedding": True,
   "legbot_replica": True,
   "replica_routing": True,
   "search_refresh": True
  },
  "name": "Washington",
  "scrape": {
   "cadence_escalation": True,
   "fargate_max_wait_s": None,
   "full_walk": False,
   "retry": False,
   "sweep_import": False,
   "timeout_s": 28800
  },
  "status": "live",
  "waf": {
   "profile": "none",
   "scrapebot_fallback": False
  }
 }
}


def entry(**fields) -> dict:
    """One full entry (a scraped, cloud-path state with no opt-ins), with `section__field=value` overrides."""
    e = copy.deepcopy(MANIFEST["az"])
    for dotted, value in fields.items():
        section, key = dotted.split("__")
        e[section][key] = value
    return e


def write(tmp_path, manifest: dict | None = None, **changes) -> str:
    data = copy.deepcopy(MANIFEST if manifest is None else manifest)
    for dotted, value in changes.items():
        code, section, key = dotted.split(".")
        data[code][section][key] = value
    (tmp_path / "jurisdictions.yaml").write_text(yaml.safe_dump(data))
    return str(tmp_path)


def only(section: str, field: str, codes) -> dict:
    """`write()` changes enrolling exactly `codes` (manifest spelling, "us" not "usa") in `section.field`."""
    wanted = {str(c).lower() for c in codes}
    return {f"{c}.{section}.{field}": c in wanted for c in MANIFEST}


def use(tmp_path, monkeypatch, manifest: dict | None = None, **changes) -> str:
    """Write the stand-in manifest, point OPENSTATES_ROOT at it and clear the reader's caches; returns its path."""
    from ddp_sync.services import jurisdiction_enrollment as je

    je._cache.clear()
    je._logged.clear()
    root = write(tmp_path, manifest, **changes)
    monkeypatch.setenv("OPENSTATES_ROOT", root)
    return root
