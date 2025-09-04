# Analysis of Google Analytics 4 Marketing Data
![GA4_Banner](Assets/GA4_Logo.png)                         
This project evaluates digital campaign effectiveness and customer segmentation by leveraging cloud-based data engineering and analytics workflows. Using Google BigQuery for scalable SQL modeling, Python for statistical analysis, and Tableau for visualization, the pipeline transforms raw GA4 event data into actionable insights. The analysis calculates key marketing metrics (CTR, CVR, CPC, AOV), applies RFM-based clustering to segment customers, and answers five core questions around channel attribution, campaign ROI, purchase behavior, and segment value—concluding with strategic recommendations for optimizing spend allocation and targeting.
### Questions Answered
1. Which marketing channels and campaigns generate the highest baseline performance?
2. What is each channel’s incremental contribution to conversions and revenue?
3. Did specific campaign changes or launches actually cause measurable lift?
4. How can customers be segmented by value and behavior?
5. Which early behaviors predict long-term customer value (LTV)?

---

## Tools and Technologies
- **Google BigQuery (SQL)** - Cloud-based data warehouse used for querying raw GA4 event data, staging, and building fact/dimension models
- **Python** – For statistical analysis and clustering (`pandas`, `scikit-learn`), econometric modeling (`statsmodels`), data visualization (`matplotlib`), campaign KPI calculations, and CSV exports
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

### Question 1: Which marketing channels and campaigns generate the highest baseline performance?
![table 1](Assets/Tables/table1b.png)

The baseline data shows that **direct traffic ((direct)/(none)) dominated raw totals**, with over 52,000 impressions but a very low CTR (0.08%). Despite that, it still produced **231 conversions and $13.3k in revenue**, thanks to a relatively strong CVR (5.8%). This reflects the “catch-all” nature of direct visits: low click efficiency, but meaningful sales because returning or loyal users often fall into this bucket.

Organic and referral sources (e.g., …/organic, …/referral) also looked strong, contributing steady conversions and revenue at healthy AOV levels (~$58). These channels perform like reliable background drivers of traffic and sales without the explicit costs of paid campaigns.

By contrast, paid search (google/cpc) appeared smaller in raw totals. Impressions and clicks were modest, and while it drove some conversions, it did not rival the volume of direct or organic. This is typical in raw rollups: last-touch channels like direct and organic appear dominant, while incremental contributors like paid search look understated.

---

### Question 2: What is each channel’s incremental contribution to conversions and revenue?
![dashboard 1](Assets/Tables/dashboard1.png)

The regression-based attribution reshuffles the leaderboard. While direct and organic looked strongest in raw totals, the incremental model shows paid search (google/cpc) accounts for ~45% of marginal conversions—the single largest driver once overlapping exposures are controlled for. This highlights paid search as the workhorse: additional CPC clicks reliably produce additional conversions.

Organic traffic (<Other>/organic) captures ~23% of incremental share, suggesting genuine value, though lower than the raw rollups implied. Referral traffic (googlemerchandisestore.com/referral) holds ~10%, indicating steady but secondary contribution. Direct visits ((none)/(none)) shrink to ~8%, reflecting that much of their apparent strength in the baselines is overlap with other channels. Meanwhile, google/organic contributes near-zero incrementally, suggesting those sales are likely influenced by exposures elsewhere.

---

### Question 3: Did specific campaign changes or launches actually cause measurable lift?
![table 2](Assets/Tables/table2.png)
The difference-in-differences test focused on **google/cpc, treated as a new campaign beginning on 2020-12-15**. The regression shows a **significant positive interaction** (treated × post = +23.6 conversions/day, p = 0.004). This indicates that **CPC generated real incremental lift** beyond what would have occurred from background trends.

Looking at averages, the control group fell sharply from ~60.2 to 37.2 conversions/day after mid-December, reflecting a general market slowdown. By contrast, CPC jumped from ~0.05 to 0.63 conversions/day in the same period. The main effect for “post” was strongly negative (−38.9), showing that overall demand dropped, but the treated × post term demonstrates that CPC rose against this tide.

---

### Question 4: How can customers be segmented by value and behavior?
![table 4](Assets/Tables/table4.png)
The RFM segmentation reveals a classic Pareto-style imbalance: a small minority of users generate the bulk of revenue. Two standout clusters dominate. **“Growing/Promising” users** (cluster 3, ≈3,545 customers) contribute ≈$253.6k in revenue with moderate average spend (≈$72 each). Meanwhile, the **“Loyal High-Value”** group (cluster 2, ≈214 customers), though tiny in size, generates ≈$93.3k in revenue thanks to an exceptional average order value (≈$436).

By contrast, the vast majority of users fall into low-value clusters. “Occasional Buyers” (cluster 0, ≈145,000 customers) contribute only ≈$14.7k in revenue, while “Churn Risk / Low-Value” (cluster 1, ≈121,000 customers) are essentially inactive, accounting for less than $1k in total spend.

---

### Question 5: Which early behaviors predict long-term customer value (LTV)?
![table 6](Assets/Tables/table6.png)
The simple LTV regression highlights a clear signal: **early purchases are by far the strongest predictor of long-term value**. The coefficient for early purchases is ≈70.7 and highly significant, meaning that customers who transact early are overwhelmingly more likely to contribute substantial revenue over their lifetime.

Secondary predictors add nuance. Early events (≈0.044, significant) also correlate positively with revenue, reflecting that engagement signals—browsing, cart adds, or product views—matter for forecasting value. Early sessions show a borderline effect (p ≈ 0.052), suggesting that sheer visits alone are less reliable unless paired with transactions or deeper engagement. Days since first seen carries only a small positive effect, indicating that **longevity without early purchases does little to drive value**.

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
