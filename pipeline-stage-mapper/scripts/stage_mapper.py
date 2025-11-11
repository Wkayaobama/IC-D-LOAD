#!/usr/bin/env python3
"""
Pipeline Stage Mapper

Maps IC'ALPS pipeline stages with double granularity.
"""

from typing import Dict, Literal


PipelineType = Literal["Hardware", "Software"]
StageType = Literal["01 - Identification", "02 - Qualifiée", "03 - Evaluation technique",
                     "04 - Construction propositions", "05 - Négociations"]
OutcomeType = Literal["No-go", "Abandonnée", "En cours", "Perdue", "Gagnée"]


class StageMapper:
    """
    Map IC'ALPS pipeline stages to final stages.

    Usage:
        mapper = StageMapper()
        final_stage = mapper.map_stage("Hardware", "01 - Identification", "Perdue")
        # Returns: "Closed Lost"
    """

    # Stage mapping rules
    OUTCOME_MAPPING = {
        "No-go": "Closed Lost",
        "Abandonnée": "Closed Lost",
        "Perdue": "Closed Lost",
        "Gagnée": "Closed Won",
        "En cours": "In Progress"
    }

    def map_stage(self, pipeline: PipelineType, stage: StageType, outcome: OutcomeType) -> str:
        """
        Map pipeline stage and outcome to final stage.

        Args:
            pipeline: Pipeline type (Hardware/Software)
            stage: Pipeline stage
            outcome: Final outcome

        Returns:
            Final stage classification
        """
        return self.OUTCOME_MAPPING.get(outcome, "Unknown")

    def get_stage_number(self, stage: str) -> int:
        """Extract stage number from stage string"""
        if stage.startswith("01"):
            return 1
        elif stage.startswith("02"):
            return 2
        elif stage.startswith("03"):
            return 3
        elif stage.startswith("04"):
            return 4
        elif stage.startswith("05"):
            return 5
        return 0


if __name__ == "__main__":
    mapper = StageMapper()

    # Test mappings
    print(mapper.map_stage("Hardware", "01 - Identification", "Perdue"))  # Closed Lost
    print(mapper.map_stage("Software", "05 - Négociations", "Gagnée"))   # Closed Won
