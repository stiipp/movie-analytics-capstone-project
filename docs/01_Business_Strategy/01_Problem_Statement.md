
## 1.1 Client Situation

Meridian Film Capital is advising a mid-size production studio that wants to make film investment decisions more systematically. The studio is not facing a complete profitability problem. The deeper issue is inconsistency. Some films perform extremely well, while others underperform, and the studio lacks a clear empirical basis for explaining which combinations of budget and genre lead to better outcomes.

The client wants a more defensible way to evaluate where future capital should go. Rather than asking only which individual films earned the most money, the project asks which types of films appear attractive, repeatable, and scalable enough to support real investment decisions.

## 1.2 Main Business Problem

This project is built to answer one decision-oriented question:

> Which genre and budget-tier combinations appear most attractive for allocating a hypothetical film investment portfolio, while accounting for both return efficiency and commercial scale?

This framing matters because ROI alone can be misleading. Very small-budget films can generate high percentage returns without representing meaningful revenue scale. A segment can look efficient on paper but still be too small, too volatile, or too niche to support practical capital deployment.

For that reason, the analysis does not stop at ROI. It also considers revenue contribution, historical success rate, sample size, and broader market movement.

## 1.3 Core Business Questions

The business case is narrowed to three core questions.

### Question 1: Which genre and budget-tier combinations deliver the strongest and most reliable ROI?

**Objective:** Identify the segments that show strong historical return performance without relying on a few outlier films.

**Analytical approach:** The recommendation layer uses reported budget and reported revenue only, with a minimum budget threshold of `$10,000` and a minimum revenue threshold of `$1,000,000`. Movies are grouped into relative budget quartiles, and performance is compared by genre and budget tier. Median ROI is treated as the main signal because it reflects the typical outcome more reliably than a simple average.

**Business value:** This question gives the client the clearest shortlist of historically attractive movie segments for investment analysis.

### Question 2: Which genres are gaining or losing momentum over time in revenue, ROI, and market coverage?

**Objective:** Distinguish between genres that were historically strong but fading, versus genres that still show active market relevance.

**Analytical approach:** The pipeline builds yearly genre trend models that compare revenue and ROI movement across time. It also includes a broader market-coverage layer so trend analysis is not limited only to the strict recommendation subset.

**Business value:** This prevents the client from relying on a segment that once performed well but may no longer be expanding or commercially active.

### Question 3: How much of the movie catalog is reliable enough for recommendation-grade investment analysis?

**Objective:** Make the dashboard transparent about how much of the source data is truly suitable for investment-style conclusions.

**Analytical approach:** The pipeline separates the data into two analytical scopes:

- **Recommendation Scope:** reported budget, reported revenue, budget >= `$10,000`, revenue >= `$1,000,000`
- **Market Coverage Scope:** positive reported revenue and budget >= `$10,000`, while allowing imputed budgets

This distinction makes it possible to answer business questions without overstating the reliability of the source data.

**Business value:** The client can trust that the final recommendation is based on an explicitly defined financial-quality subset rather than the entire raw catalog.

## 1.4 Why This Framing Was Chosen

The project is intentionally scoped around downstream decision-making. It does not try to answer every possible movie question in the dataset. Instead, it models the parts of the data that support the client’s investment case clearly and reliably, while still preserving extra dimensions such as company and country for future analysis if the business focus changes later.
