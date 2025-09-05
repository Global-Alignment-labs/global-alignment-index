# Global Alignment Index (GAI)

> **Are we moving toward alignment — or away from it?**  
> GAI is a public, factual, opinion-free dashboard that tracks whether humanity is becoming **more aligned** or **more misaligned** over time — globally and per country.

---

## North Star

Our guiding objective is to **compare the rate of alignment growth with the rate of capability/power growth**.  
If capability outpaces alignment, the trajectory is **unsustainable**. The dashboard exists to make that gap visible and actionable.

---

## Core Alignment — 4 Directional Goals

**Voice & Inclusion**  
*We are building a world where everyone affected can be heard or represented.*

**Truth & Clarity**  
*We are building a world where important choices are based on information that can be checked and trusted.*

**Safety & Care**  
*We are building a world where harm is prevented as much as possible, and when it happens, people and planet are cared for and repaired.*

**Responsibility & Learning**  
*We are building a world where those who cause harm are held accountable, and where mistakes become lessons for better futures.*

Clear insight into **where alignment is rising or decaying** helps steer action and resources toward better futures — **preventing harm and promoting flourishing**.

---

## What the dashboard shows

- **Global view + country view.** World series first; country drill-downs roll out as data lands.
- **Raw metric graphs** (original, factual series).
- **Relative alignment graphs (0–100%)** to make direction obvious:  
  - **Up is better** (e.g., literacy): 0% at reference min, 100% at reference max or target.  
  - **Down is better** (e.g., mortality): 100% at the target (e.g., 0), 0% at the reference max.
- A **Whole Alignment Trend (aggregate)** will follow once weighting & robustness policies are finalized.

> **Weights:** will be **fine-tuned**. Likely factors include **population relevance** (how many people affected) and **measurement reliability** (source quality & coverage). We keep this transparent in `/docs/METHODS.md`.

---

## Metric priority tiers

We grow in layers so signals remain clear and trustworthy:

- **Tier 1 — Backbone:** direction-certain, open, reproducible, broad coverage.  
- **Tier 2 — Bounded/contested:** direction-clear but slower cadence or debated baselines.  
- **Tier 3 — Complex/model-heavy:** valuable but patchier coverage or joins/models.  
- **Tier 4 — Interpretive:** perspective-building; optional and clearly labeled.

The dashboard **never** rewards military deployments or proxy-war involvement; we measure **harm avoided/reduced** and **care provided**.

---

## Tier-1 (v1) metric set — initial focus

*(Global line first; per-country follows.)*

### Safety & Care
- **Under-5 mortality (↓)** — UN IGME / World Bank (WDI) — **live**
- **Intentional homicide per 100k (↓)** — UNODC (via WDI) — *in progress*
- **DTP3 immunization coverage % (↑)** — WHO/UNICEF (WUENIC)
- **Disaster mortality (↓)** — EM-DAT (winsorized 3-yr MA)
- **Road traffic deaths per 100k (↓)** — WHO / WDI

### Voice & Inclusion
- **Youth literacy 15–24 % (↑)** — UNESCO / WDI
- **Out-of-school (primary-age) % (↓)** — UNESCO / WDI

### Economics & Poverty (alignment—not capability)
- **Extreme poverty % (<$2.15, 2017 PPP) (↓)** — World Bank Povcal / WDI
- **Child stunting % (↓)** — UNICEF-WHO-WB JME

### Truth & Clarity
- **Death registration completeness with cause-of-death (↑)** — WHO / UNDESA (where open)
- **IMF Data Standards adoption (↑)** — IMF (e-GDDS/SDDS/SDDS+)

*(More metrics — including Tier-2/3 candidates like PM2.5, maternal mortality, turnout+integrity, etc. — are listed in `/docs/METHODS.md` and will phase in as Tier-1 stabilizes.)*

---

## Inner-country & cross-country

GAI measures both **inner-country alignment** (within each nation) and **cross-country resonance** (how countries support or hinder alignment across borders). Initial releases focus on robust **global** and **country** series; dyadic cross-country views will follow.

---

## Data sources & transparency

- **Authoritative open sources** (World Bank, WHO/UNICEF, UNHCR/IDMC, EM-DAT, UNESCO, NOAA/NASA, etc.).
- Each metric’s method & provenance is documented in `/docs/METHODS.md` and tracked in `/public/data/sources.json`.
- Data files are versioned in Git under `/public/data/<metric>.json`. The site reads these JSONs directly.

**Relative (0–100%)** uses per-metric `reference_min`, `reference_max`, and optional `target` from `/public/data/metrics_registry.json`.

---

## Tech (brief)

- **Next.js + React + Tailwind + Recharts**  
- Data pipelines via **TypeScript** fetchers (e.g., WDI/WHO APIs) run in CI and open small **auto data PRs** so you can review each dataset change before it goes live.

> **MVP note:** the initial implementation is programmed by **Codex** (AI code assistant), targeting a small, robust Tier-1 backbone first.

---

## Roadmap

1. **MVP (Tier-1 backbone)** — ship global series & relative views.  
2. **Resonance & partnerships** — collaborate with researchers/NGOs; embed widgets & basic API.  
3. **Tier-2 / Tier-3 expansion** — add depth (carefully documented).  
4. **Alignment vs capability growth** — include capability/power metrics (e.g., compute/energy diffusion) and compare their **rate of change** with alignment’s rate to judge sustainability.

---

## Getting started

```bash
# Install
npm install

# Run the site
npm run dev

# (Optional) Run data pipelines (requires internet)
npm run fetch:all
