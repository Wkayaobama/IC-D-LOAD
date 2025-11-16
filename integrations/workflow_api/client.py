"""
Workflow API Client for Dynamic Query Execution
==============================================

Executes SQL queries against HubSpot database via workflow webhooks.

The workflow system provides REST endpoints that:
1. Accept query parameters (e.g., company name, contact ID)
2. Execute predefined SQL queries against synced HubSpot database
3. Return results as JSON

Webhook Structure:
    POST https://besg.api.workflows.stacksync.com/workspaces/2219/workflows/{workflow_id}:latest_draft/triggers/post_trigger/run_wait_result

Query Parameters:
    - Jinja-templated SQL with {{ input.query_parameters.param_name }}
    - Example: WHERE lower(company) like lower('%{{ input.query_parameters.company }}%')

Usage:
    from workflow_api_client import WorkflowAPIClient

    client = WorkflowAPIClient()

    # Query contacts by company
    results = client.query_contacts(company='ACME')

    # Query companies by icalps_company_id
    results = client.query_companies(icalps_company_id=123)
"""

import requests
from typing import Dict, List, Optional, Any
from loguru import logger
import json
from urllib.parse import urlencode


class WorkflowAPIClient:
    """
    Client for executing queries via Workflow API.

    Provides methods for:
    - Querying HubSpot contacts, companies, deals, communications
    - Passing dynamic query parameters
    - Handling Jinja template rendering
    - Error handling and retries
    """

    BASE_URL = "https://besg.api.workflows.stacksync.com"
    WORKSPACE_ID = "2219"

    def __init__(self, timeout: int = 30):
        """
        Initialize Workflow API client.

        Args:
            timeout: Request timeout in seconds
        """
        self.timeout = timeout
        self.session = requests.Session()

    def _build_workflow_url(self, workflow_id: str) -> str:
        """
        Build workflow execution URL.

        Args:
            workflow_id: Workflow UUID

        Returns:
            Complete workflow URL
        """
        return (
            f"{self.BASE_URL}/workspaces/{self.WORKSPACE_ID}/"
            f"workflows/{workflow_id}:latest_draft/triggers/post_trigger/run_wait_result"
        )

    def execute_workflow(
        self,
        workflow_id: str,
        query_params: Optional[Dict[str, Any]] = None,
        retry: int = 3
    ) -> Dict:
        """
        Execute a workflow with query parameters.

        Args:
            workflow_id: Workflow UUID
            query_params: Dictionary of query parameters
            retry: Number of retry attempts

        Returns:
            JSON response from workflow

        Raises:
            Exception: If workflow execution fails
        """
        url = self._build_workflow_url(workflow_id)

        # Add query parameters to URL if provided
        if query_params:
            url += "?" + urlencode(query_params)

        attempt = 0
        last_error = None

        while attempt < retry:
            try:
                logger.info(f"Executing workflow: {workflow_id}")
                logger.debug(f"URL: {url}")

                response = self.session.get(url, timeout=self.timeout)
                response.raise_for_status()

                result = response.json()
                logger.info(f"✓ Workflow executed successfully")
                return result

            except Exception as e:
                attempt += 1
                last_error = e
                logger.warning(f"✗ Workflow attempt {attempt}/{retry} failed: {e}")

                if attempt < retry:
                    import time
                    time.sleep(2 ** attempt)  # Exponential backoff

        logger.error(f"✗ Workflow failed after {retry} attempts: {last_error}")
        raise last_error

    def query_contacts(
        self,
        workflow_id: str = "e4d4b296-deb5-4075-bb5f-20c77054d86f",
        **filters
    ) -> List[Dict]:
        """
        Query HubSpot contacts via workflow.

        Filters:
            - company: Filter by company name (substring match)
            - email: Filter by email (exact match)
            - icalps_contact_id: Filter by legacy contact ID
            - firstname: Filter by first name
            - lastname: Filter by last name

        Args:
            workflow_id: Workflow UUID for contact query
            **filters: Query filter parameters

        Returns:
            List of contact records

        Example:
            contacts = client.query_contacts(company='ACME')
            contacts = client.query_contacts(icalps_contact_id=123)
        """
        result = self.execute_workflow(workflow_id, query_params=filters)

        # Extract contacts from result
        if isinstance(result, dict) and 'rows' in result:
            return result['rows']
        elif isinstance(result, list):
            return result
        else:
            return []

    def query_companies(
        self,
        workflow_id: str = "e4d4b296-deb5-4075-bb5f-20c77054d86f",
        **filters
    ) -> List[Dict]:
        """
        Query HubSpot companies via workflow.

        Filters:
            - name: Filter by company name (substring match)
            - domain: Filter by domain
            - icalps_company_id: Filter by legacy company ID
            - city: Filter by city
            - country: Filter by country

        Args:
            workflow_id: Workflow UUID for company query
            **filters: Query filter parameters

        Returns:
            List of company records

        Example:
            companies = client.query_companies(name='ACME')
            companies = client.query_companies(icalps_company_id=456)
        """
        result = self.execute_workflow(workflow_id, query_params=filters)

        if isinstance(result, dict) and 'rows' in result:
            return result['rows']
        elif isinstance(result, list):
            return result
        else:
            return []

    def query_deals(
        self,
        workflow_id: str = "e4d4b296-deb5-4075-bb5f-20c77054d86f",
        **filters
    ) -> List[Dict]:
        """
        Query HubSpot deals via workflow.

        Filters:
            - dealname: Filter by deal name (substring match)
            - icalps_deal_id: Filter by legacy deal ID
            - dealstage: Filter by deal stage
            - company: Filter by associated company

        Args:
            workflow_id: Workflow UUID for deal query
            **filters: Query filter parameters

        Returns:
            List of deal records

        Example:
            deals = client.query_deals(company='ACME')
            deals = client.query_deals(icalps_deal_id=789)
        """
        result = self.execute_workflow(workflow_id, query_params=filters)

        if isinstance(result, dict) and 'rows' in result:
            return result['rows']
        elif isinstance(result, list):
            return result
        else:
            return []

    def query_communications(
        self,
        workflow_id: str = "e4d4b296-deb5-4075-bb5f-20c77054d86f",
        **filters
    ) -> List[Dict]:
        """
        Query HubSpot communications/engagements via workflow.

        Filters:
            - icalps_communication_id: Filter by legacy communication ID
            - company: Filter by associated company
            - contact: Filter by associated contact
            - engagement_type: Filter by engagement type

        Args:
            workflow_id: Workflow UUID for communication query
            **filters: Query filter parameters

        Returns:
            List of communication records

        Example:
            comms = client.query_communications(company='ACME')
        """
        result = self.execute_workflow(workflow_id, query_params=filters)

        if isinstance(result, dict) and 'rows' in result:
            return result['rows']
        elif isinstance(result, list):
            return result
        else:
            return []

    def query_by_icalps_id(
        self,
        object_type: str,
        icalps_id: int,
        workflow_id: Optional[str] = None
    ) -> Optional[Dict]:
        """
        Query HubSpot record by icalps_* ID.

        Args:
            object_type: Object type (contacts, companies, deals, communications)
            icalps_id: Legacy ID to search for
            workflow_id: Optional workflow ID (uses default if not provided)

        Returns:
            Single record or None if not found

        Example:
            company = client.query_by_icalps_id('companies', 123)
            contact = client.query_by_icalps_id('contacts', 456)
        """
        # Map object type to icalps field name
        icalps_field_map = {
            'contacts': 'icalps_contact_id',
            'companies': 'icalps_company_id',
            'deals': 'icalps_deal_id',
            'communications': 'icalps_communication_id'
        }

        if object_type not in icalps_field_map:
            raise ValueError(f"Unknown object type: {object_type}")

        icalps_field = icalps_field_map[object_type]

        # Use default workflow if not specified
        if not workflow_id:
            workflow_id = "e4d4b296-deb5-4075-bb5f-20c77054d86f"

        # Build filter
        filters = {icalps_field: icalps_id}

        # Execute query based on object type
        if object_type == 'contacts':
            results = self.query_contacts(workflow_id, **filters)
        elif object_type == 'companies':
            results = self.query_companies(workflow_id, **filters)
        elif object_type == 'deals':
            results = self.query_deals(workflow_id, **filters)
        elif object_type == 'communications':
            results = self.query_communications(workflow_id, **filters)
        else:
            return None

        # Return first result or None
        return results[0] if results else None


# ============================================================================
# DIRECT CURL COMMAND GENERATOR
# ============================================================================

class CurlCommandGenerator:
    """
    Generates curl commands for workflow execution.

    Useful for:
    - Testing workflows from command line
    - Debugging query parameters
    - Documenting API usage
    """

    BASE_URL = "https://besg.api.workflows.stacksync.com"
    WORKSPACE_ID = "2219"

    @staticmethod
    def generate_curl_command(
        workflow_id: str,
        query_params: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Generate curl command for workflow execution.

        Args:
            workflow_id: Workflow UUID
            query_params: Query parameters

        Returns:
            Curl command string
        """
        url = (
            f"{CurlCommandGenerator.BASE_URL}/workspaces/{CurlCommandGenerator.WORKSPACE_ID}/"
            f"workflows/{workflow_id}:latest_draft/triggers/post_trigger/run_wait_result"
        )

        if query_params:
            url += "?" + urlencode(query_params)

        return f"curl --request 'GET' --url '{url}'"

    @staticmethod
    def print_examples():
        """Print example curl commands."""
        print("=" * 80)
        print("Curl Command Examples")
        print("=" * 80)

        # Example 1: Query contacts by company
        print("\n1. Query contacts by company:")
        cmd = CurlCommandGenerator.generate_curl_command(
            "e4d4b296-deb5-4075-bb5f-20c77054d86f",
            {"company": "ACME"}
        )
        print(cmd)

        # Example 2: Query companies by icalps ID
        print("\n2. Query companies by icalps_company_id:")
        cmd = CurlCommandGenerator.generate_curl_command(
            "e4d4b296-deb5-4075-bb5f-20c77054d86f",
            {"icalps_company_id": 123}
        )
        print(cmd)

        # Example 3: Query deals by legacy ID
        print("\n3. Query deals by icalps_deal_id:")
        cmd = CurlCommandGenerator.generate_curl_command(
            "e4d4b296-deb5-4075-bb5f-20c77054d86f",
            {"icalps_deal_id": 789}
        )
        print(cmd)


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    # Print curl examples
    CurlCommandGenerator.print_examples()

    # Test API client (commented out - requires actual workflow)
    # client = WorkflowAPIClient()
    # companies = client.query_companies(name='ACME')
    # print(f"\nFound {len(companies)} companies")
