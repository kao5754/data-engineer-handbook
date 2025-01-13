# Data Engineering Pipelines Management

## Pipeline Ownership

### Profit Pipelines
1. **Unit-Level Profit Needed for Experiments**
   - **Primary Owner**: Engineer A
   - **Secondary Owner**: Engineer B

2. **Aggregate Profit Reported to Investors**
   - **Primary Owner**: Engineer C
   - **Secondary Owner**: Engineer D

### Growth Pipelines
1. **Aggregate Growth Reported to Investors**
   - **Primary Owner**: Engineer B
   - **Secondary Owner**: Engineer C

2. **Daily Growth Needed for Experiments**
   - **Primary Owner**: Engineer D
   - **Secondary Owner**: Engineer A

### Engagement Pipeline
1. **Aggregate Engagement Reported to Investors**
   - **Primary Owner**: Engineer A
   - **Secondary Owner**: Engineer C

---

## On-Call Schedule

### Weekly Rotation
- **Week 1**: Engineer A
- **Week 2**: Engineer B
- **Week 3**: Engineer C
- **Week 4**: Engineer D

### Holiday Coverage
- If a primary on-call engineer is unavailable during a holiday, the secondary owner of their respective pipelines will step in.
- Engineers will swap weeks to ensure holidays are covered fairly.
- Example: If Engineer A is on-call for Christmas week, they will swap with Engineer B for a later week.

---

## Runbooks for Investor Metrics Pipelines

### Profit: Aggregate Profit Reported to Investors
**Potential Issues:**
- **Data Inconsistencies**: Mismatch between unit-level and aggregated profit values.
- **Pipeline Failures**: Scheduled job not completing due to resource limits or system outages.
- **Source Data Delays**: Late data updates from dependent systems.

### Growth: Aggregate Growth Reported to Investors
**Potential Issues:**
- **Incorrect Calculations**: Errors in growth rate formulas or logic.
- **Missing Data**: Daily growth values not captured due to upstream failures.
- **Version Mismatches**: Different pipeline components using inconsistent versions of transformation logic.

### Engagement: Aggregate Engagement Reported to Investors
**Potential Issues:**
- **Metric Definitions**: Misalignment on how engagement is calculated (e.g., daily active users).
- **Data Overlap**: Duplication of users counted in multiple categories.
- **Output Failures**: Errors in exporting metrics to reporting systems.

---

## Notes
These pipelines are critical for reporting accurate metrics to investors and facilitating experimental analysis. Regular reviews of ownership, on-call schedules, and pipeline documentation should be conducted to ensure reliability and fairness.
