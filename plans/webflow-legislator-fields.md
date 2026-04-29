# Webflow Legislators CMS — New Field Setup

This is the field-add checklist for the **Legislator Bio + Contact Sync** project (see [`PLAN-legislator-bio-sync.md`](./PLAN-legislator-bio-sync.md)). Add these fields to the existing **Legislators** collection in the Webflow Designer.

All new fields are **optional** so existing CMS items aren't blocked.

---

## ⚠️ Do NOT touch these existing fields

The sync writes to the new fields below. These existing fields stay untouched:

- `name`, `slug`
- `openstatesid` — primary join key for the sync
- `jurisdiction` (reference)
- `party-2` (or `party`)
- `chamber`
- `district`
- `email`
- `image`
- `score`
- `post-body`
- `description`

---

## Fields to add (21 total)

The "Slug" column shows the API field name Webflow should auto-generate from the display name. **If your auto-slug produces something different** (e.g. with a numeric suffix from a name collision), share the actual slug back with engineering and the sync code will be adjusted to match.

### Source identifiers (cross-source reference links)

These let the website link out to the legislator's profile on partner platforms.

| Display name | Slug | Type | Notes |
|---|---|---|---|
| Bioguide ID | `bioguide-id` | Plain text | Federal only. Drives the official Congress.gov portrait URL. Also the join-key fallback for departed federal members. |
| Wikidata ID | `wikidata-id` | Plain text | Federal only. Format: `Q12345678`. |
| OpenSecrets ID | `opensecrets-id` | Plain text | Federal only. Format: `N00012345`. |
| Ballotpedia Slug | `ballotpedia-slug` | Plain text | Federal only. Becomes part of `ballotpedia.org/{slug}`. |
| GovTrack ID | `govtrack-id` | Plain text | Federal only. Numeric. |

State legislator records will leave these blank — OpenStates' `other_identifiers` is empty for state legislators.

### Bio

| Display name | Slug | Type | Notes |
|---|---|---|---|
| Birth Year | `birth-year` | Number | **Year only** — full DOB is intentionally not stored, even though available |
| Gender | `gender` | Plain text | "M" / "F" / blank |

### Contact

| Display name | Slug | Type | Notes |
|---|---|---|---|
| Capitol Phone | `phone-capitol` | Phone | DC office for federal; capitol office for state |
| Capitol Office Address | `office-address-capitol` | Plain text (multi-line) | |
| District Phone | `phone-district` | Phone | State legislators often have only one office, populated in whichever fits |
| District Office Address | `office-address-district` | Plain text (multi-line) | |
| Contact Form URL | `contact-form-url` | Link | Federal members usually populate this **instead of** email |

The existing `email` field stays — sync populates it for state legislators (real `@government.gov` email) but typically leaves it blank for federal members (whose "email" upstream is actually a contact-form URL → routed to `contact-form-url`).

### Web & social

| Display name | Slug | Type | Notes |
|---|---|---|---|
| Official Website | `official-website` | Link | house.gov / senate.gov / state-legislature page |
| Twitter Handle | `twitter-handle` | Plain text | Handle only (no `@`, no URL). Front-end constructs the URL. |
| Facebook Handle | `facebook-handle` | Plain text | Handle / page slug |
| Instagram Handle | `instagram-handle` | Plain text | Handle |
| YouTube Handle | `youtube-handle` | Plain text | Channel handle (when present; ~49% federal coverage) |

### Term

| Display name | Slug | Type | Notes |
|---|---|---|---|
| Term Start | `term-start` | Date | Earliest term start in the upstream record |
| Term End | `term-end` | Date | Latest term end. Populated for departed members; blank for ongoing terms beyond current congress |
| Seniority Rank | `seniority-rank` | Plain text | Federal Senate only ("junior" / "senior"). Optional. |

### Photo provenance

| Display name | Slug | Type | Notes |
|---|---|---|---|
| Photo Source URL | `photo-source-url` | Plain text | The upstream image URL. Used in Phase 3 to detect when the source image has changed and the Webflow asset needs re-uploading. |

---

## Verification checklist

After adding the fields:

- [ ] All 21 fields above exist in the Legislators collection
- [ ] Each new field is **optional** (not required) so existing items aren't blocked from saving
- [ ] None of the existing fields listed in the "Do NOT touch" section above were renamed or deleted
- [ ] Capture the actual API slugs Webflow generated (especially if any auto-suffixed) and share with engineering
- [ ] Confirm your CMS license tier supports the additional field count — Webflow plans typically cap collections at 60 fields. Existing 12 + new 21 = ~33; plenty of headroom but worth checking.

---

## FAQ

**Q: Why "Plain text" for IDs and not custom field types?**
A: We want the values stored verbatim with no Webflow-side validation. Cross-source ID formats vary and we don't want to fight schema rejections.

**Q: Why "Phone" type for phones?**
A: Webflow's Phone type renders nicely on the front-end and provides click-to-call on mobile. Sync stores raw numbers; Webflow handles formatting.

**Q: Why store handles instead of full URLs for social?**
A: Smaller payloads, more flexible front-end rendering (e.g., showing `@SenRickScott` while linking to the full URL), and easier to detect missing values.

**Q: Why "Year only" for birth?**
A: Full DOB is publicly available via Bioguide for federal members but feels personal as a published field. Year-only gives "elected at age 38" framing without the privacy optics. State legislators frequently don't have any birth date in OpenStates anyway.

**Q: What if I add a field with a slightly different slug than listed?**
A: Tell engineering the actual slug. The sync code will be adjusted to match — Webflow's auto-slug isn't always identical to the display name.

**Q: Can I add the fields incrementally?**
A: Yes. The sync code gracefully no-ops on any field that doesn't yet exist in Webflow (catches `field not found` errors and logs a warning). You can roll the fields out a few at a time without breaking sync.
