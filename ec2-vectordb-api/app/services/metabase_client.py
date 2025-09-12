import requests
import json
import os
import logging


logger = logging.getLogger(__name__)


class MetabaseClient:
    def __init__(self, question_id=None):
        self.base_url = os.environ.get('METABASE_URL')
        self.api_key = os.environ.get('METABASE_API_KEY')
        
        # Use provided question_id or fallback to environment variable
        self.question_id = question_id or os.environ.get('METABASE_QUESTION_ID')
        
        if not all([self.base_url, self.api_key, self.question_id]):
            raise ValueError("Missing required Metabase configuration. Ensure METABASE_URL, METABASE_API_KEY are set, and either provide question_id parameter or set METABASE_QUESTION_ID environment variable.")
    
    def fetch_question_results(self, start_date, end_date):
        """
        Fetch results from Metabase question with date parameters.
        
        Args:
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            
        Returns:
            list: Question results
        """
        try:
            url = f"{self.base_url}/api/card/{self.question_id}/query"
            
            headers = {
                'X-API-KEY': self.api_key,  # Changed from 'X-API-Key' to match JS version
                'Content-Type': 'application/json'
            }
            
            # Match the exact format from the working JavaScript function
            payload = {
                'ignore_cache': False,
                'parameters': [
                    {
                        'type': 'text',
                        'target': ['variable', ['template-tag', 'Start_Date']],
                        'value': start_date
                    },
                    {
                        'type': 'text',
                        'target': ['variable', ['template-tag', 'End_Date']],
                        'value': end_date
                    }
                ]
            }
            
            logger.info(f"Fetching Metabase question {self.question_id} with dates {start_date} to {end_date}")
            logger.info(f"Request URL: {url}")
            logger.info(f"Request payload: {payload}")
            print(f"DEBUG: Request URL: {url}")
            print(f"DEBUG: Request payload: {payload}")
            print(f"DEBUG: Using question_id: {self.question_id}")
            
            response = requests.post(url, headers=headers, json=payload, timeout=300)
            response.raise_for_status()
            
            data = response.json()
            
            # Log response details like the JavaScript version
            logger.info(f"Metabase API Response: status={response.status_code}, "
                       f"statusText={response.reason}, "
                       f"dataKeys={list(data.keys())}, "
                       f"hasData={bool(data.get('data'))}, "
                       f"rowCount={len(data.get('data', {}).get('rows', []))}, "
                       f"colCount={len(data.get('data', {}).get('cols', []))}")
            
            # Extract rows from the response
            if 'data' in data and 'rows' in data['data']:
                rows = data['data']['rows']
                columns = data['data']['cols']
                
                # Log column information
                if columns:
                    logger.info(f"Metabase columns: {[{'name': col['name'], 'type': col.get('base_type', 'unknown'), 'display_name': col.get('display_name', col['name'])} for col in columns]}")
                
                # Convert to list of dictionaries
                results = []
                for row in rows:
                    row_dict = {}
                    for i, col in enumerate(columns):
                        row_dict[col['name']] = row[i]
                    results.append(row_dict)
                
                logger.info(f"Successfully fetched {len(results)} rows from Metabase")
                
                # Log first record as sample if available
                if results:
                    logger.info(f"Sample record: {results[0]}")
                
                return results
            else:
                logger.warning("No data found in Metabase response")
                logger.warning(f"Response data structure: {data}")
                return []
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching Metabase data: {e}")
            logger.error(f"Response status: {response.status_code if 'response' in locals() else 'No response'}")
            logger.error(f"Response text: {response.text if 'response' in locals() else 'No response'}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error fetching Metabase data: {e}")
            raise