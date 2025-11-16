---
name: computed-columns-calculator
description: Calculate computed columns (Weighted Forecast, Net Amount, Net Weighted Amount) for deal/opportunity data. Use for financial calculations in the pipeline.
---

# Computed Columns Calculator

## Overview

Calculates computed financial columns for deal/opportunity data including weighted forecasts, net amounts, and weighted net amounts. Implements IC'ALPS business logic for financial calculations.

## Computed Columns

### 1. IC_alps Forecast → Amount
Map IC_alps Forecast field to standard Amount field

### 2. Weighted Forecast
**Formula**: `Amount × IC_alps Certainty`

### 3. Net Amount
**Formula**: `Forecast - Cost`

### 4. Net Weighted Amount
**Formula**: `Net Amount × IC_alps Certainty`

## Usage

```python
from scripts.computed_calculator import ComputedColumnsCalculator
import pandas as pd

calculator = ComputedColumnsCalculator()

# Apply to DataFrame
df = pd.DataFrame({
    'amount': [100000, 50000, 75000],
    'certainty': [0.8, 0.5, 0.9],
    'cost': [20000, 10000, 15000]
})

# Calculate all computed columns
df = calculator.calculate_all(df)

# Result includes:
# - weighted_forecast
# - net_amount
# - net_weighted_amount
```

## Individual Calculations

```python
# Calculate individual columns
df['weighted_forecast'] = calculator.calculate_weighted_forecast(
    df['amount'],
    df['certainty']
)

df['net_amount'] = calculator.calculate_net_amount(
    df['amount'],
    df['cost']
)
```

## Resources

See `scripts/computed_calculator.py` for implementation.
