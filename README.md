# Global Alignment Index (GAI)

> **Are we moving toward alignment — or away from it?**  
> GAI is a public, factual, opinion-free dashboard that tracks whether humanity is becoming **more aligned** or **more misaligned** over time — globally and per country.

---

## North Star

Our guiding objective is to **compare the rate of alignment growth with the rate of capability/power growth**.  
If capability outpaces alignment, the trajectory is **unsustainable**. The dashboard exists to make that gap visible and actionable.

---

## Core Alignment — 4 Directional Goals

These four goals form the **minimal foundation for sustainable world growth, harm reduction, and flourishing**.  
Without them, growth in power or capability becomes unstable and harmful:

### Safety & Care
> “We are building a world where harm is prevented as much as possible, and when it happens, people and planet are cared for and repaired.”

Protects life and wellbeing, ensuring that progress does not come at the cost of widespread harm.

### Responsibility & Learning
> “We are building a world where those who cause harm are held accountable, and where mistakes become lessons for better futures.”

Creates accountability for harm and enables mistakes to become lessons, not cycles of repeated damage.

### Truth & Clarity
> “We are building a world where important choices are based on information that can be checked and trusted.”

Provides a shared factual basis so choices can be checked, trusted, and coordinated.

### Voice & Inclusion
> “We are building a world where everyone affected can be heard or represented.”

Ensures that those affected can participate, preventing decisions from being imposed on silenced populations.

**Together, these four goals are the irreducible set:**  
If even one is missing, societies may grow in capability but collapse or self-terminate through misalignment. With all four, alignment can rise sustainably alongside power, shaping a trajectory of growth that reduces harm and expands flourishing.

---

## What the dashboard shows

- **Global view + country view.** World series first; country drill-downs roll out as data lands.  
- **Raw metric graphs** (original, factual series).  
- **Relative alignment graphs (0–100%)** to make direction obvious:  
  - **Up is better** (e.g., literacy): 0% at reference min, 100% at reference max or target.  
  - **Down is better** (e.g., mortality): 100% at the target (e.g., 0), 0% at the reference max.  
- A **Whole Alignment Trend (aggregate)** will follow once weighting & robustness policies are finalized.

> **Weights (raw v1):** Safety & Care = 40%, Responsibility & Learning = 20%, Truth & Clarity = 20%, Voice & Inclusion = 20%.  
> Within each domain, metrics are further weighted by factors such as **population relevance** (how many people are affected) and **measurement reliability** (source quality & coverage). Refinement will continue over time.

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

*(Global line first; per-country follows. ICA = Inner-country Alignment, CCA = Cross-country Alignment)*

### Safety & Care

**ICA**
- Under-5 mortality (↓) — UN IGME / World Bank (WDI)  
- Intentional homicide per 100k (↓) — UNODC (via WDI)  
- DTP3 immunization coverage % (↑) — WHO/UNICEF (WUENIC)  
- Disaster mortality (↓) — EM-DAT (winsorized 3-yr MA)  
- Road traffic deaths per 100k (↓) — WHO / WDI  
- Child stunting % (↓) — UNICEF–WHO–World Bank (JME)  
- Extreme poverty % (<$2.15, 2017 PPP) (↓) — World Bank Povcal / WDI  

**CCA**
- Battle-related deaths per 100k (↓) — UCDP / PRIO  

---

### Responsibility & Learning

**ICA**
- IMF Data Standards adoption (↑) — IMF (e-GDDS / SDDS / SDDS+)  

**CCA**
- Peace agreement persistence (↑) — UCDP / PA-X  

---

### Truth & Clarity

**ICA**
- Death registration completeness with cause-of-death (↑) — WHO / UNDESA  

**CCA**
- Student mobility residual (↑) — UNESCO / OECD (knowledge exchange)  

---

### Voice & Inclusion

**ICA**
- Youth literacy 15–24 % (↑) — UNESCO / WDI  
- Out-of-school (primary-age) % (↓) — UNESCO / WDI  

**CCA**
- Student mobility residual (↑) — UNESCO / OECD (participation across borders)  

*(More metrics — including Tier-2/3 candidates like PM2.5, maternal mortality, turnout+integrity, etc. — are listed in `/docs/METHODS.md` and will phase in as Tier-1 stabilizes.)*

---

## Inner-country & cross-country

GAI measures both **inner-country alignment** (within each nation) and **cross-country resonance** (how countries support or hinder alignment across borders).  
Initial releases focus on robust **global** and **country** series; dyadic cross-country views will follow.

---

## Data sources & transparency

- **Authoritative open sources** (World Bank, WHO/UNICEF, UNHCR/IDMC, EM-DAT, UNESCO, NOAA/NASA, UCDP, etc.).  
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

# Battle-deaths consent flow
# ---------------------------
# The UCDP battle-related deaths CSV is consent-gated.
# Download `ucdp-brd-conflict-251.csv` from https://ucdp.uu.se/downloads/battle-related-deaths/
# and save it locally as `data/raw/ucdp-brd-conflict-251.csv` before running:
npm run fetch:battle-deaths
```
