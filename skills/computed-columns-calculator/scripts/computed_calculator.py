#!/usr/bin/env python3
"""
Computed Columns Calculator

Calculate financial computed columns for deals/opportunities.
"""

import pandas as pd
from typing import Union


class ComputedColumnsCalculator:
    """
    Calculate computed columns for deal data.

    Usage:
        calculator = ComputedColumnsCalculator()
        df = calculator.calculate_all(df)
    """

    def calculate_weighted_forecast(
        self,
        amount: Union[pd.Series, float],
        certainty: Union[pd.Series, float]
    ) -> Union[pd.Series, float]:
        """
        Calculate Weighted Forecast.

        Formula: Amount × IC_alps Certainty

        Args:
            amount: Deal amount
            certainty: Certainty percentage (0-1)

        Returns:
            Weighted forecast value
        """
        return amount * certainty

    def calculate_net_amount(
        self,
        forecast: Union[pd.Series, float],
        cost: Union[pd.Series, float]
    ) -> Union[pd.Series, float]:
        """
        Calculate Net Amount.

        Formula: Forecast - Cost

        Args:
            forecast: Forecast amount
            cost: Cost amount

        Returns:
            Net amount
        """
        return forecast - cost

    def calculate_net_weighted_amount(
        self,
        net_amount: Union[pd.Series, float],
        certainty: Union[pd.Series, float]
    ) -> Union[pd.Series, float]:
        """
        Calculate Net Weighted Amount.

        Formula: Net Amount × IC_alps Certainty

        Args:
            net_amount: Net amount
            certainty: Certainty percentage (0-1)

        Returns:
            Net weighted amount
        """
        return net_amount * certainty

    def calculate_all(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate all computed columns.

        Required columns in df:
        - amount (or forecast)
        - certainty
        - cost

        Adds columns:
        - weighted_forecast
        - net_amount
        - net_weighted_amount

        Args:
            df: DataFrame with deal data

        Returns:
            DataFrame with computed columns added
        """
        df = df.copy()

        # Use 'amount' or 'forecast' column
        amount_col = 'amount' if 'amount' in df.columns else 'forecast'

        # Calculate weighted forecast
        df['weighted_forecast'] = self.calculate_weighted_forecast(
            df[amount_col],
            df['certainty']
        )

        # Calculate net amount
        df['net_amount'] = self.calculate_net_amount(
            df[amount_col],
            df['cost']
        )

        # Calculate net weighted amount
        df['net_weighted_amount'] = self.calculate_net_weighted_amount(
            df['net_amount'],
            df['certainty']
        )

        return df


if __name__ == "__main__":
    # Example usage
    calculator = ComputedColumnsCalculator()

    df = pd.DataFrame({
        'deal_id': [1, 2, 3],
        'amount': [100000, 50000, 75000],
        'certainty': [0.8, 0.5, 0.9],
        'cost': [20000, 10000, 15000]
    })

    print("Original DataFrame:")
    print(df)

    df = calculator.calculate_all(df)

    print("\nWith Computed Columns:")
    print(df)
