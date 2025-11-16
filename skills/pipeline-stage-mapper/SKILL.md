---
name: pipeline-stage-mapper
description: Map IC'ALPS Hardware and Software pipeline stages with double granularity (No-go, Abandonnée, Perdue, Gagnée). Use for deal stage mapping and business logic.
---

# Pipeline Stage Mapper

## Overview

Maps IC'ALPS Hardware and Software pipeline stages (5 stages each) with double granularity final opportunity stages. Implements business logic for stage transitions and outcome classification.

## Pipeline Stages

### IC'ALPS Hardware Pipeline
1. 01 - Identification
2. 02 - Qualifiée
3. 03 - Evaluation technique
4. 04 - Construction propositions
5. 05 - Négociations

### IC'ALPS Software Pipeline
1. 01 - Identification
2. 02 - Qualifiée
3. 03 - Evaluation technique
4. 04 - Construction propositions
5. 05 - Négociations

### Double Granularity Final Stages
- **No-go**: Not profitable
- **Abandonnée**: Project doesn't want to go ASIC
- **Perdue**: Lost (Competition/Price)
- **Gagnée**: Won

## Usage

```python
from scripts.stage_mapper import StageMapper

mapper = StageMapper()

# Map stage
result = mapper.map_stage(
    pipeline="Hardware",
    stage="01 - Identification",
    outcome="Perdue"
)
# Returns: "Closed Lost"

# Apply to DataFrame
import pandas as pd
df['final_stage'] = df.apply(
    lambda row: mapper.map_stage(row['pipeline'], row['stage'], row['outcome']),
    axis=1
)
```

## Resources

See `scripts/stage_mapper.py` for implementation.
