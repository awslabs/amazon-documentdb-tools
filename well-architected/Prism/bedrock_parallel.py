import boto3
import json
import re
import concurrent.futures
from bedrock_advisor import FALLBACK_MODEL_ID

def get_bedrock_suggestion_single(query_pattern, aws_region='us-east-1', collection_context=None):
    """Get index suggestion for a single query pattern from Bedrock.
    
    Args:
        query_pattern: Dict with operation, example_query, count, avg_time, ns, pattern_key
        aws_region: AWS region for Bedrock
        collection_context: Optional dict with existing indexes and stats for the collection
            {indexes: [{name, key, ops}], doc_count, avg_obj_size}
    """
    try:
        bedrock = boto3.client('bedrock-runtime', region_name=aws_region)
        
        clean_query = {k: v for k, v in query_pattern['example_query'].items() 
                      if k not in ['lsid', 'readPreference', '$readPreference']}
        
        context_section = ""
        if collection_context:
            existing_indexes = collection_context.get("indexes", [])
            if existing_indexes:
                idx_list = "\n".join(f"  - {idx.get('name', '?')}: {json.dumps(idx.get('key', {}))}" 
                                    for idx in existing_indexes[:10])
                context_section = f"""
Existing indexes on this collection:
{idx_list}
Document count: {collection_context.get('doc_count', 'unknown')}
Avg document size: {collection_context.get('avg_obj_size', 'unknown')} bytes

IMPORTANT: Do NOT suggest an index that already exists. Keep total indexes ≤5 per collection.
"""

        prompt = f"""You are an Amazon DocumentDB expert. Analyze this slow query and suggest optimal indexes.

Query: {json.dumps(clean_query, indent=2)}
Operation: {query_pattern['operation']}
Namespace: {query_pattern.get('ns', '')}
Count (in profiler window): {query_pattern['count']}
Avg Time: {query_pattern['avg_time']:.1f}ms
Max Time: {query_pattern.get('max_time', 0):.1f}ms
{context_section}
DocumentDB rules:
- Follow ESR (Equality-Sort-Range) rule for compound indexes
- Max 3 fields in compound indexes
- For inserts: slow inserts indicate too many indexes or large documents, not missing indexes
- retryWrites is not supported

Return JSON:
{{
  "suggested_indexes": [
    {{
      "index": {{"field1": 1, "field2": 1}},
      "priority": "high|medium|low",
      "collection": "collection_name",
      "reason": "brief explanation"
    }}
  ],
  "diagnosis": "one sentence explaining why this operation is slow",
  "is_insert_bottleneck": false
}}"""

        response = bedrock.invoke_model(
            modelId=FALLBACK_MODEL_ID,
            body=json.dumps({
                'messages': [{'role': 'user', 'content': prompt}],
                'max_tokens': 2000,
                'temperature': 0.1,
                'anthropic_version': 'bedrock-2023-05-31'
            })
        )
        
        result = json.loads(response['body'].read())
        response_text = result['content'][0]['text']
        
        json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
        if json_match:
            suggestion_data = json.loads(json_match.group())
            suggestions = []
            for suggestion in suggestion_data.get('suggested_indexes', []):
                suggestion['pattern_key'] = query_pattern['pattern_key']
                suggestion['query_count'] = query_pattern['count']
                suggestion['avg_execution_time'] = query_pattern['avg_time']
                suggestion['example_query'] = clean_query
                suggestions.append(suggestion)
            return suggestions
        
        return []
        
    except Exception as e:
        return []

def get_bedrock_suggestions_parallel(query_patterns, aws_region='us-east-1', max_workers=4, db_analysis_data=None):
    """Get index suggestions from Bedrock in parallel for multiple query patterns.
    
    Args:
        query_patterns: List of pattern dicts from query_analyzer
        aws_region: AWS region for Bedrock
        max_workers: Thread pool size
        db_analysis_data: Optional dict from db_analysis module {db: {coll: {indexes, count, ...}}}
            Used to provide existing index context to Bedrock for better suggestions.
    """
    if not query_patterns:
        return []
    
    def _get_collection_context(pattern):
        """Extract collection context from db_analysis_data for this pattern's namespace."""
        if not db_analysis_data:
            return None
        ns = pattern.get("ns", "")
        if "." not in ns:
            return None
        db_name, coll_name = ns.split(".", 1)
        db_data = db_analysis_data.get(db_name, {})
        if not isinstance(db_data, dict):
            return None
        coll_data = db_data.get(coll_name, {})
        if not isinstance(coll_data, dict):
            return None
        # Build context
        indexes = coll_data.get("indexes", [])
        return {
            "indexes": [{"name": idx.get("name", "?"), "key": idx.get("key", {}),
                        "ops": idx.get("accesses", {}).get("ops", 0)}
                       for idx in indexes] if indexes else [],
            "doc_count": coll_data.get("count", 0),
            "avg_obj_size": coll_data.get("avgObjSize", 0),
        }

    all_suggestions = []
    
    # Use ThreadPoolExecutor for parallel requests
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all requests with collection context
        future_to_pattern = {
            executor.submit(get_bedrock_suggestion_single, pattern, aws_region,
                          _get_collection_context(pattern)): pattern 
            for pattern in query_patterns[:10]  # Limit to first 10 patterns
        }
        
        # Collect results as they complete
        for future in concurrent.futures.as_completed(future_to_pattern):
            try:
                suggestions = future.result()
                all_suggestions.extend(suggestions)
            except Exception as e:
                continue
    
    return all_suggestions

def get_final_recommendations_parallel(index_suggestions):
    """Convert parallel suggestions to recommendations"""
    recommendations = []
    
    for suggestion in index_suggestions:
        collection_name = suggestion.get('collection', 'Unknown Collection')
        
        # Extract collection from pattern if not provided
        if collection_name == 'Unknown Collection':
            pattern_key = suggestion.get('pattern_key', '')
            if '.' in pattern_key:
                collection_name = pattern_key.split('.')[0]
        
        # Generate meaningful index name from fields
        index_fields = suggestion.get('index', {})
        field_names = list(index_fields.keys())
        if field_names:
            # Create index name from field names (max 3 fields)
            name_parts = [field.split('.')[-1] for field in field_names[:3]]
            index_name = f"idx_{'_'.join(name_parts)}"
        else:
            index_name = 'idx_suggested'
        
        recommendations.append({
            'type': 'create_index',
            'index': suggestion.get('index', {}),
            'command': f"db.{collection_name}.createIndex({json.dumps(suggestion.get('index', {}))}, {{'name': '{index_name}'}})",
            'priority': suggestion.get('priority', 'medium'),
            'explanation': f'Suggested based on slow query pattern analysis (avg: {suggestion.get("avg_execution_time", 0):.1f}ms)',
            'expected_impact': f"Should improve queries with {suggestion.get('query_count', 0)} occurrences",
            'estimated_queries_affected': suggestion.get('query_count', 0),
            'example_query': suggestion.get('example_query'),
            'collection': collection_name
        })
    
    priority_counts = {'high': 0, 'medium': 0, 'low': 0}
    for rec in recommendations:
        priority = rec.get('priority', 'medium')
        if priority in priority_counts:
            priority_counts[priority] += 1
    
    return {
        'recommendations': recommendations,
        'summary': {
            'total_recommendations': len(recommendations),
            'high_priority': priority_counts['high'],
            'medium_priority': priority_counts['medium'],
            'low_priority': priority_counts['low']
        }
    }