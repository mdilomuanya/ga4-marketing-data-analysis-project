# Analysis of Google Analytics 4 Marketing Data
![GA4_Banner](GA4_Logo.png)
This project evaluates digital campaign effectiveness and customer segmentation by leveraging cloud-based data engineering and analytics workflows. Using Google BigQuery for scalable SQL modeling, Python for statistical analysis, and Tableau for visualization, the pipeline transforms raw GA4 event data into actionable insights. The analysis calculates key marketing metrics (CTR, CVR, CPC, AOV), applies RFM-based clustering to segment customers, and answers five core questions around channel attribution, campaign ROI, purchase behavior, and segment value—concluding with strategic recommendations for optimizing spend allocation and targeting.
### Questions Answered
1. How has the average cost/minute played changed over time?
2. How much of the wage bill is wasted on underused players?
3. Who is overpaid relative to minutes?
4. Which players have the biggest impact on games (via GIS)?
5. Does impact align with wages—who are bargains vs inefficiencies?

---

## Tools and Technologies
- **Google BigQuery (SQL)** - Cloud-based data warehouse used for querying raw GA4 event data, staging, and building fact/dimension models
- **Python** – For statistical analysis and clustering (`pandas`, `scikit-learn`), econometric modeling (statsmodels), data visualization (`matplotlib`), campaign KPI calculations, and CSV exports
- **Tableau** – For interactive dashboards, funnel analysis, RFM segmentation visualization, and channel attribution storytelling

---

## Project Workflow

### 1. BigQuery: Dataset & Problem Setup
Loaded the GA4 BigQuery export (Google Merchandise Store sample) and scoped the business question:
- Explored the event-level schema (nested arrays like event_params, items) and mapped UTM fields (source/medium/campaign)
- Identified core dimensions (device, country, platform) and KPI definitions (CTR, CVR, AOV, ROAS where cost exists)
- Put them into separate data frames and export them as CSV files

### 2. SQL: Data Engineering / Modeling
Built dbt-style layers in BigQuery to transform raw GA4 events into analysis-ready models:
- Built dbt-style layers to transform raw GA4 events into analysis-ready models
- raw_events → direct GA4 export reference
- stg_events → flattened parameters via UNNEST, normalized UTM fields, standardized types
- fact_campaign_performance → daily channel/campaign aggregates (impressions, clicks, sessions, conversions, revenue, CTR, CVR, AOV, ROAS)
- dim_customers → user-level rollups (country, device, platform, sessions, purchases, revenue)

**Output:** sql script for the model `ga4_dtb_project.sql` and sanity checks `sanity_checks.sql`

### 3. Python: Statistical Analysis
Using Python (pandas, statsmodels, scikit-learn, matplotlib) to quantify performance, incrementality, and audience quality:
3a. Rule-Based Attribution (Baselines)
  - Grouped by channel and campaign to compute CTR, CVR, AOV, revenue
  - Produced baseline leaderboards for quick performance triage
  **Output:** `by_channel_baseline.csv`, `by_campaign_baseline.csv`
    
3b. Regression-Based Attribution (Incremental Contribution)
  - Pivoted daily clicks by channel; regressed daily conversions on clicks + controls (DOW, trend)
  - Coefficients interpreted as marginal conversions per channel; derived attribution shares
  **Output:** `regression_attribution_channels.csv`

3c. A/B-Style Difference-in-Differences (Causal Lift)
  - Selected a treated channel (e.g., CPC) and launch/change date; defined treated vs. control and pre/post windows
  - Estimated causal lift from the treated×post interaction
  **Output:** `did_daily_conversions.csv`, `did_results.csv`

3d. Segmentation (RFM & Simple LTV)
  - Computed Recency, Frequency, Monetary at user level; ran KMeans for segments
  - Fit OLS to predict revenue from early behaviors (sessions, purchases, key events)
  - Enriched segments with device/country/platform
  **Output:** `rfm_assignments.csv`, `rfm_segment_summary.csv`, `rfm_enriched.csv`, `ltv_model_coefficients.csv`

### 4. Tableau: Data Visualization
Built stakeholder-ready dashboards in Tableau (`link`) from the CSV exports with relationships (not physical joins) to avoid row explosion:
- Channel Dashboard — channel & campaign baselines (CTR, CVR, AOV), and regression-based attribution visualizations
- Customer Segmentation Dashboard - RFM clusters, revenue contribution, LTV predictors
- Wasted Wage Bill → compared total vs wasted wages by year
 **Output:** `link to channel dashboard`, `link to customer dashboard`

---

### Question 1: How has the cost/minute changed over time?
![table 1](Assets/Tables/table1b.png)

The data shows that Arsenal’s median cost per minute played dropped significantly from £2,731 in 20/21 to £2,248 in 21/22, reflecting early squad trimming and better utilization of wage spend. However, efficiency dipped in later seasons, peaking at £3,723 in 23/24 before improving slightly in 24/25 (£3,332).

This trend highlights a diagnostic insight: as Arsenal invested in higher-caliber players during their title challenge years (23/24–24/25), wage inflation outpaced the efficiency gains of previous seasons. Injuries and rotation further drove up the cost per effective minute.

From an optimization standpoint, the club should continue to monitor whether premium wages translate into consistent availability and impact. If not, future contracts need stricter alignment to minutes played or performance metrics.

Strategically, this analysis suggests Arsenal has moved from cutting “deadwood” to the harder task of ensuring elite wages are justified by elite contributions. The risk now is less about wasted contracts and more about sustaining efficiency in a high-spend, title-chasing era.

---

### Question 2: How much of the wage bill is wasted on underused players?
![dashboard 1](Assets/Tables/dashboard1.png)

Arsenal’s total weekly wage bill has fluctuated between **£2.4M** and **£3.4M** over the last five seasons, with the biggest dip in 21/22 reflecting the club’s post-pandemic reset. By 23/24 and 24/25, wages returned to the £3.2–3.3M range—close to 20/21 levels in absolute terms. However, the picture looks different when framed against revenue: with turnover growing from £367M in 20/21 to £616.6M in 23/24, wages now represent a smaller share of total income, meaning **Arsenal’s financial capacity to sustain a large wage bill has actually improved**.

At the same time, the share of wages spent on underused players has steadily declined. In 20/21, nearly a quarter of the wage bill went to players featuring in less than 20% of minutes. By 24/25, this figure was just 12.5%. The club has clearly reduced “dead money” tied up in fringe contracts, a shift that reflects smarter squad management and fewer legacy deals weighing down the balance sheet.

The strategic takeaway is twofold: **Arsenal not only trimmed inefficiencies but also grew into their wage bill as revenues surged**. With projected income in 24/25 estimated between £650M–£850M, even maintaining wages at current levels would leave the club in a stronger financial position than peers struggling with static revenues. Going forward, the challenge isn’t the size of the wage bill but ensuring new contracts continue to track player impact rather than sentiment or short-term hype.

---

### Question 3: Who is overpaid relative to minutes?
![table 2](Assets/Tables/table2.png)
The raw £/minute figures for 24/25 highlight **Takehiro Tomiyasu** as an extreme outlier, costing nearly **£743K per minute played** due to major injuries that kept him sidelined for most of the season. Because his case reflects extraordinary circumstances rather than structural inefficiency, he is excluded from some of the further analysis to avoid skewing the dataset.

![table 3](Assets/Tables/table3.png)
Looking at the adjusted table without Tomiyasu, a clearer picture emerges. Players such as **Neto (£28.9K/min)**, **Tierney (£12.5K/min)**, **Jesus (£11.4K/min)**, and **Zinchenko (£9.8K/min)** stand out as costly relative to their availability. These players not only carry high wages but also offer limited minutes, raising questions about their reliability as long-term contributors.

The next step is to monitor whether this pattern persists across other metrics. If these names continue appearing among the least efficient by team contribution as well, it strengthens the case for either renegotiation or exit. This kind of analysis ensures Arsenal doesn’t just cut “deadwood” once but maintains a disciplined wage-to-contribution balance year after year.

---

### Question 4: Which players have the biggest impact on performance (via GIS)?
![table 4](Assets/Tables/table4.png)
To measure player impact, we began by looking at **Points Per Game (PPG)** — the average amount of points Arsenal earned with a given player on the field (0=loss, 1=draw, 3=win) — and **On-Off xG**, which tracks how many goals the team was expected to score and concede changed when the player was on versus off the field (below 0 means net negative, above 0 means net positive). **PPG reflects results**, while **On-Off xG captures underlying performance**. In 24/25 these two measures were highly correlated; once we excluded players with fewer than 20% of available minutes, they provided a strong proxy for a player’s influence on team outcomes.

I then combined these into a Game Impact Score (GIS). Using a formula '(0.3 * [zPPG]) + (0.7 * [zOnOffxG])' incorporating z-scores for both PPG and On-Off xG, weighted them 70:30 in favor of On-Off xG. This tilt reflects the view that performance metrics are more reliable than raw outcomes, while still incorporating results. Finally, scores were normalized within each season so that the top-performing player received a score of 100, allowing for relative comparison within the squad.

![table 5](Assets/Tables/table5.png)
The results for 24/25 highlight **Calafiori**, **Sterling**, **Saka**, and **Ødegaard** as Arsenal’s highest-impact players, with **Havertz** and **Nwaneri** also scoring strongly. On the other end, **White**, **Partey**, and **Raya** ranked lowest. Raya’s score illustrates one weakness of GIS: as a goalkeeper who played almost every match (94.9% of available minutes) there were too few “off-pitch” samples to meaningfully measure his swing effect. Similarly, injuries or differences in quality of opposition faced (e.g., White’s position shifting, Sterling predominantly getting minutes in easier low stakes games) can shift GIS up or down despite quality or a lack thereof in certain contexts.

From a monitoring perspective, the **players to keep tabs on are those consistently scoring low GIS relative to their wages**. So far **White**, **Partey** and **Martinelli** appear as potential concerns, while Raya’s case suggests methodological caution, and Lewis-Skelly is a youth player who played in predominantly difficult games. Meanwhile, the cluster of high-impact young and core players — Saka, Ødegaard, Rice, Calafiori — reinforces the narrative that Arsenal’s wage structure is increasingly aligned with true value.

---

### Question 5: Does impact align with wages—who are bargains vs inefficiencies?
![table 6](Assets/Tables/table6.png)
The first lens is a simple one: wages vs minutes played for the entire squad. This plot shows whether players are consistently available for the team relative to their salary. Here, Arsenal’s biggest winners are the **high wage/high minutes group**—Ødegaard, Rice, Partey, and Saliba—who represent reliable, pillars delivering minutes value for their contracts. On the opposite side, **Jesus, Zinchenko, and White** appear as red flags: high wages with relatively low minutes, which weakens their overall efficiency and raises concerns about long-term value. At the bottom left, youngsters like **Nwaneri and Lewis-Skelly** register as extremely low-cost, low-minute contributors—a healthy dynamic, since they provide upside without straining the wage bill.

![table 7](Assets/Tables/table7.png)
When we add impact into the equation, the picture sharpens. The Player Impact vs Pay scatter confirms that **Arsenal’s biggest earners—Saka, Ødegaard, Rice, Havertz, Saliba—combine a strong GIS with high wages**, positioning them as justified spends. They also all fit the age profile, with all of them either already being prime age, or entering into their primes, showing a healthy core of players the squad is build around. However, **Partey and White** again surface as high-wage, low-impact players, mirroring their poor efficiency in the minutes plot, and Partey's age profile being in the red flags him again. **Martinelli**, is another player to watch out for, as he is on a high wage that is not reflecting his relatively poor impact. Him being a player with his prime still ahead of him, now might present a good opportunity to cash out on him while his value is still high. While Raya’s score is distorted by methodological quirks in evaluating goalkeepers, other players like **Calafiori, Nwaneri, and Timber** represent great contract value.

![table 8](Assets/Tables/table8.png)
The Cost per Minute vs Impact table reinforces these findings, highlighting **Jesus** as exceedingly costly relative to his actual contribution. Injuries play a role here, but the trend raises red flags for contract value if availability doesn’t improve, as this has been a consistent issue for three seasons. In contrast, **Nwaneri and Lewis-Skelly—both just 17 and 18 respectivley**—deliver exceptional impact for minimal cost, strengthening the case for continued youth integration as a cost-control mechanism. **Ben White** again is flagged as a high cost player, towards the edge of the standard deviation, with a poor GIS.

---
### Player Specific Conclusions & Recommendations Based on Data
#### Sell or release Older Low-Minute Players
- Tomiyasu, Neto, Tierney, Zinchenko: Each logged fewer than 20% of available minutes while on significant wages. Selling now avoids sunk costs and frees up squad spots.
#### Consistently Poor Performers
- Partey: Contract should be allowed to expire without renewal. At 31, recurring injuries and declining impact mean any extension would add inefficiency.
- White: At 27, still prime-age, but his first season of low minutes/low impact is concerning. Give him one more season to rebound; if not, sell while value remains.
#### Priority Extensions
- Nwaneri & Lewis-Skelly: Both project as **high-impact bargains** and symbolize Arsenal’s youth-driven efficiency. Early extensions would protect against poaching.
- Saliba: Played the second-most minutes in 24/25, combining consistency, age profile, and elite impact. Should be secured as a **top 3–5 earner** in the squad.
#### Mixed Cases
- Martinelli: Fairly valuable in the market but delivered poor impact scores, and on high wages. If a high-value replacement is available, consider cashing in.
- Jesus: High impact per minute but chronically unavailable, especially problematic as Arsenal’s second-highest paid player. Renegotiate contract with **incentives tied to availability**, or prepare to release.
#### Surprises
- Sterling: Statistically provided strong impact-to-wage value despite skepticism from fans. His loan extension would be defensible, though not essential.
- Raya: GIS underrates him because of the goalkeeper sample issue. Played 94.9% of minutes, suggesting a need for a more reliable understudy to spread risk.

---
### Alignment with Reality
Interestingly, these data-driven recommendations align closely with Arsenal’s actual moves:
- Arsenal have let go of Tomiyasu, Neto, Tierney, and Partey
- Zinchenko’s role has diminished, and has been linked with moves away with his future uncertain
- Both Nwaneri and Lewis-Skelly recently recieved long-term contract renewals with higher pay
- Two players who play in Martinelli's position have been signed, and a contract upgrade for his rival in the squad Trossard, show he might be cashed-in
- A solid backup for Raya, Kepa, was signed who can take minutes off him if need be

---
### Financials and Squad Health Conclusions
Beyond individuals, the broader financial picture shows Arsenal in a much stronger position:
- **Wages as a percentage of revenue have declined**, despite absolute wage growth. Revenues surged from **£367M in 20/21 to £616.6M in 23/24**, with 24/25 projected as high as £850M.
- The club can now comfortably sustain a £3.2–3.3M weekly wage bill without straining resources.
- The **share of wages spent on underused players has fallen by nearly half** (23% in 20/21 → 12.5% in 24/25), marking a clear reduction in “deadwood” contracts.
- The squad age profile is healthy: the wage bill is concentrated in 25–28 year olds (Rice, Saka, Ødegaard, Havertz, Saliba), who are both prime-age and high GIS scorers. Meanwhile, youth bargains (Nwaneri, Lewis-Skelly) provide cost-controlled upside, and veterans are no longer overrepresented.

---
### Strategic Takeaway
Arsenal’s financial discipline and squad management have **transitioned from clearing legacy inefficiencies to optimizing at the margins**. The next phase isn’t about cutting obvious deadwood—it’s about **deciding how long to back prime but injury-prone players** (Jesus, White) and **locking in the high-impact core for their peak years**.

The data shows a club increasingly aligned with elite standards: fewer wasted contracts, wages tied more closely to impact, and a youthful backbone capable of sustaining title challenges.

---

## Author
**Matthew-David Ilomuanya**  
Data Analyst & Researcher  
[LinkedIn](https://www.linkedin.com/in/matthew-david-ilomuanya-2498101a5/) | Portfolio Website
