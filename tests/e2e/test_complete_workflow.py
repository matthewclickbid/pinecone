"""End-to-end workflow tests."""

import pytest
from unittest.mock import Mock, patch, MagicMock, call
import json
import os
import uuid

class TestCompleteWorkflow:
    """Test complete end-to-end workflows."""
    
    @patch.dict(os.environ, {
        'API_KEY': 'test-api-key',
        'ASYNC_LAMBDA_NAME': 'test-async-lambda',
        'DYNAMODB_TABLE_NAME': 'test-table',
        'METABASE_URL': 'https://metabase.example.com',
        'METABASE_API_KEY': 'test-metabase-key',
        'METABASE_QUESTION_ID': '123',
        'OPENAI_API_KEY': 'test-openai-key',
        'PINECONE_API_KEY': 'test-pinecone-key',
        'PINECONE_INDEX_NAME': 'test-index',
        'PINECONE_NAMESPACE': 'test-namespace'
    })
    def test_metabase_workflow_end_to_end(self):
        """Test complete Metabase workflow from API to Pinecone."""
        from main_handler import handler as main_handler
        from async_handler import handler as async_handler
        
        with patch('main_handler.boto3.client') as mock_boto_main, \
             patch('main_handler.DynamoDBClient') as mock_db_main, \
             patch('async_handler.DynamoDBClient') as mock_db_async, \
             patch('async_handler.MetabaseClient') as mock_mb, \
             patch('async_handler.OpenAIClient') as mock_oai, \
             patch('async_handler.PineconeClient') as mock_pc:
            
            # Setup main handler mocks
            task_id = str(uuid.uuid4())
            mock_db_main_inst = Mock()
            mock_db_main_inst.create_task.return_value = task_id
            mock_db_main.return_value = mock_db_main_inst
            
            mock_lambda = Mock()
            mock_lambda.invoke.return_value = {'StatusCode': 202}
            mock_boto_main.return_value = mock_lambda
            
            # Setup async handler mocks
            mock_db_async_inst = Mock()
            mock_db_async.return_value = mock_db_async_inst
            
            mock_mb_inst = Mock()
            mock_mb_inst.fetch_data.return_value = [
                {
                    'user_id': 'user1',
                    'datetime': '2025-01-01T10:00:00Z',
                    'content': 'Test content for user 1'
                },
                {
                    'user_id': 'user2',
                    'datetime': '2025-01-01T11:00:00Z',
                    'content': 'Test content for user 2'
                }
            ]
            mock_mb.return_value = mock_mb_inst
            
            mock_oai_inst = Mock()
            mock_oai_inst.generate_embeddings.return_value = [
                [0.1] * 1536,
                [0.2] * 1536
            ]
            mock_oai.return_value = mock_oai_inst
            
            mock_pc_inst = Mock()
            mock_pc_inst.prepare_vectors.return_value = [
                {
                    'id': 'user1_2025-01-01T10:00:00Z',
                    'values': [0.1] * 1536,
                    'metadata': {
                        'user_id': 'user1',
                        'datetime': '2025-01-01T10:00:00Z',
                        'content': 'Test content for user 1'
                    }
                },
                {
                    'id': 'user2_2025-01-01T11:00:00Z',
                    'values': [0.2] * 1536,
                    'metadata': {
                        'user_id': 'user2',
                        'datetime': '2025-01-01T11:00:00Z',
                        'content': 'Test content for user 2'
                    }
                }
            ]
            mock_pc.return_value = mock_pc_inst
            
            # Step 1: API Gateway request
            api_event = {
                'httpMethod': 'GET',
                'path': '/process',
                'headers': {'x-api-key': 'test-api-key'},
                'queryStringParameters': {
                    'start_date': '2025-01-01',
                    'end_date': '2025-01-02',
                    'question_id': '123'
                }
            }
            
            api_response = main_handler(api_event, {})
            
            # Verify API response
            assert api_response['statusCode'] == 202
            body = json.loads(api_response['body'])
            assert body['task_id'] == task_id
            assert body['status'] == 'processing'
            
            # Extract Lambda invocation payload
            lambda_invoke_call = mock_lambda.invoke.call_args
            async_event = json.loads(lambda_invoke_call[1]['Payload'])
            
            # Step 2: Async Lambda processing
            async_result = async_handler(async_event, {})
            
            # Verify async processing result
            assert async_result['statusCode'] == 200
            assert async_result['status'] == 'completed'
            assert async_result['processed_records'] == 2
            
            # Verify complete data flow
            mock_mb_inst.fetch_data.assert_called_once_with(
                '123', '2025-01-01', '2025-01-02'
            )
            mock_oai_inst.generate_embeddings.assert_called_once()
            mock_pc_inst.prepare_vectors.assert_called_once()
            mock_pc_inst.upsert_vectors.assert_called_once()
            
            # Verify status updates
            assert mock_db_async_inst.update_task_status.call_count >= 2
            final_status_call = mock_db_async_inst.update_task_status.call_args_list[-1]
            assert final_status_call[0][1] == 'completed'
    
    @patch.dict(os.environ, {
        'API_KEY': 'test-api-key',
        'STATE_MACHINE_ARN': 'arn:aws:states:us-east-1:123456789012:stateMachine:test',
        'DYNAMODB_TABLE_NAME': 'test-table',
        'S3_BUCKET_NAME': 'test-bucket',
        'OPENAI_API_KEY': 'test-openai-key',
        'PINECONE_API_KEY': 'test-pinecone-key',
        'PINECONE_INDEX_NAME': 'test-index',
        'PINECONE_NAMESPACE': 'test-namespace'
    })
    def test_s3_csv_step_functions_workflow(self):
        """Test complete S3 CSV workflow with Step Functions."""
        from main_handler import handler as main_handler
        from step_functions.csv_initializer import handler as csv_init_handler
        from step_functions.chunk_processor import handler as chunk_handler
        from step_functions.result_aggregator import handler as aggregator_handler
        
        with patch('main_handler.boto3.client') as mock_boto_main, \
             patch('main_handler.DynamoDBClient') as mock_db_main, \
             patch('step_functions.csv_initializer.boto3.client') as mock_boto_init, \
             patch('step_functions.csv_initializer.DynamoDBClient') as mock_db_init, \
             patch('step_functions.chunk_processor.S3CSVClient') as mock_s3_chunk, \
             patch('step_functions.chunk_processor.OpenAIClient') as mock_oai_chunk, \
             patch('step_functions.chunk_processor.PineconeClient') as mock_pc_chunk, \
             patch('step_functions.chunk_processor.DynamoDBClient') as mock_db_chunk, \
             patch('step_functions.result_aggregator.DynamoDBClient') as mock_db_agg:
            
            # Setup main handler mocks
            task_id = str(uuid.uuid4())
            mock_db_main_inst = Mock()
            mock_db_main_inst.create_task.return_value = task_id
            mock_db_main.return_value = mock_db_main_inst
            
            mock_sf = Mock()
            mock_sf.start_execution.return_value = {
                'executionArn': 'arn:aws:states:us-east-1:123456789012:execution:test:exec-123'
            }
            mock_boto_main.return_value = mock_sf
            
            # Step 1: API Gateway request
            api_event = {
                'httpMethod': 'GET',
                'path': '/process',
                'headers': {'x-api-key': 'test-api-key'},
                'queryStringParameters': {
                    'data_source': 's3_csv',
                    's3_key': 'data/large-file.csv'
                }
            }
            
            api_response = main_handler(api_event, {})
            
            assert api_response['statusCode'] == 202
            body = json.loads(api_response['body'])
            assert body['task_id'] == task_id
            
            # Extract Step Functions input
            sf_call = mock_sf.start_execution.call_args
            sf_input = json.loads(sf_call[1]['input'])
            
            # Step 2: CSV Initializer
            mock_s3_init = Mock()
            mock_s3_init.head_object.return_value = {'ContentLength': 1000000}
            mock_s3_init.select_object_content.return_value = {
                'Payload': [{'Records': {'Payload': b'10000'}}]
            }
            mock_boto_init.return_value = mock_s3_init
            
            mock_db_init_inst = Mock()
            mock_db_init.return_value = mock_db_init_inst
            
            init_result = csv_init_handler(sf_input, {})
            
            assert init_result['total_rows'] == 10000
            assert init_result['chunk_size'] == 1000
            assert init_result['total_chunks'] == 10
            assert len(init_result['chunks']) == 10
            
            # Step 3: Process chunks (simulate 2 chunks)
            mock_s3_chunk_inst = Mock()
            mock_s3_chunk_inst.read_csv_chunk.return_value = [
                {
                    'user_id': f'user{i}',
                    'datetime': f'2025-01-01T{10+i}:00:00Z',
                    'content': f'Content {i}'
                }
                for i in range(100)
            ]
            mock_s3_chunk.return_value = mock_s3_chunk_inst
            
            mock_oai_chunk_inst = Mock()
            mock_oai_chunk_inst.generate_embeddings.return_value = [
                [0.1 * i] * 1536 for i in range(100)
            ]
            mock_oai_chunk.return_value = mock_oai_chunk_inst
            
            mock_pc_chunk_inst = Mock()
            mock_pc_chunk_inst.prepare_vectors.return_value = [
                {
                    'id': f'user{i}_2025-01-01T{10+i}:00:00Z',
                    'values': [0.1 * i] * 1536,
                    'metadata': {}
                }
                for i in range(100)
            ]
            mock_pc_chunk.return_value = mock_pc_chunk_inst
            
            mock_db_chunk_inst = Mock()
            mock_db_chunk.return_value = mock_db_chunk_inst
            
            # Process first chunk
            chunk_event = {
                'task_id': task_id,
                's3_key': 'data/large-file.csv',
                'chunk_index': 0,
                'start_row': 0,
                'end_row': 100
            }
            
            chunk_result = chunk_handler(chunk_event, {})
            
            assert chunk_result['chunk_index'] == 0
            assert chunk_result['processed'] == 100
            assert chunk_result['status'] == 'completed'
            
            # Step 4: Aggregate results
            agg_event = {
                'task_id': task_id,
                'chunk_results': [
                    {'chunk_index': i, 'processed': 100, 'status': 'completed'}
                    for i in range(10)
                ]
            }
            
            mock_db_agg_inst = Mock()
            mock_db_agg.return_value = mock_db_agg_inst
            
            agg_result = aggregator_handler(agg_event, {})
            
            assert agg_result['task_id'] == task_id
            assert agg_result['status'] == 'completed'
            assert agg_result['total_processed'] == 1000
            assert agg_result['total_chunks'] == 10
            assert agg_result['successful_chunks'] == 10
    
    def test_error_propagation_workflow(self):
        """Test error handling and propagation through the workflow."""
        from task_status import handler as status_handler
        
        with patch.dict(os.environ, {
            'API_KEY': 'test-api-key',
            'DYNAMODB_TABLE_NAME': 'test-table'
        }):
            with patch('task_status.DynamoDBClient') as mock_db:
                # Simulate a failed task
                mock_db_inst = Mock()
                mock_db_inst.get_task.return_value = {
                    'task_id': 'failed-task-123',
                    'status': 'failed',
                    'errors': ['OpenAI API rate limit exceeded', 'Pinecone connection timeout'],
                    'total_records': 1000,
                    'processed_records': 450,
                    'created_at': '2025-01-01T10:00:00Z',
                    'updated_at': '2025-01-01T10:05:00Z'
                }
                mock_db.return_value = mock_db_inst
                
                # Check status
                status_event = {
                    'httpMethod': 'GET',
                    'path': '/status',
                    'headers': {'x-api-key': 'test-api-key'},
                    'queryStringParameters': {
                        'task_id': 'failed-task-123'
                    }
                }
                
                status_response = status_handler(status_event, {})
                
                assert status_response['statusCode'] == 200
                body = json.loads(status_response['body'])
                assert body['status'] == 'failed'
                assert len(body['errors']) == 2
                assert body['processed_records'] == 450
                assert body['total_records'] == 1000