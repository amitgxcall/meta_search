# Context Management for NL-SQL-NL Prompt Chaining

## Context Storage Strategy

### 1. **Initial Context Extraction (NL → Context)**

```
CONTEXT EXTRACTION PROMPT:

Analyze this natural language query and extract key context information:

Query: "{user_input}"

Extract and structure the following context:

INTENT ANALYSIS:
- Primary Intent: [what the user wants to accomplish]
- Query Type: [aggregation/filter/comparison/trend/ranking]
- Business Domain: [sales/finance/operations/marketing/etc.]
- Urgency Level: [routine/priority/critical]

USER CONTEXT:
- Audience Level: [executive/analyst/general_user]
- Technical Expertise: [beginner/intermediate/advanced]
- Decision Context: [operational/strategic/exploratory]

BUSINESS CONTEXT:
- Key Metrics: [list the main business metrics involved]
- Time Scope: [current/historical/future/comparative]
- Stakeholders: [who would use this information]
- Expected Actions: [what decisions this might inform]

DATA CONTEXT:
- Entities Mentioned: [customers/orders/products/etc.]
- Filters Implied: [date ranges/categories/statuses]
- Aggregation Needed: [count/sum/average/etc.]
- Comparison Points: [vs previous period/vs benchmark/vs segments]

Return this as structured context to pass to SQL generation.
```

### 2. **Enhanced NL→SQL Prompt (with Context)**

```
NATURAL LANGUAGE TO SQL CONVERSION:

CARRIED CONTEXT:
Intent: {extracted_intent}
Query Type: {query_type}
Business Domain: {business_domain}
Audience: {audience_level}
Key Metrics: {key_metrics}
Time Scope: {time_scope}
Expected Use: {expected_actions}

SCHEMA CONTEXT:
{database_schema}

BUSINESS RULES:
{domain_specific_rules}

ORIGINAL REQUEST: "{natural_language_query}"

CONTEXT-AWARE INSTRUCTIONS:
1. Generate SQL that addresses the specific intent: {intent}
2. Optimize for {audience_level} - include appropriate level of detail
3. Consider {business_domain} context when interpreting terms
4. Structure results to support {expected_actions}
5. Include relevant {key_metrics} in the output
6. Handle {time_scope} appropriately with date functions

CONTEXT TO PRESERVE FOR EXPLANATION:
- Original Intent: {intent}
- Business Terms Used: [map technical columns to business terms]
- Query Complexity: [simple/moderate/complex]
- Key Relationships: [tables joined and why]
- Business Logic Applied: [any special rules or calculations]

Generate SQL and preserve context metadata:

SQL: [your generated query]

CONTEXT FOR EXPLANATION:
- Query Intent: {intent}
- Business Focus: {key_metrics}
- Technical Approach: [brief explanation of SQL approach]
- Data Scope: [what data is being analyzed]
- Expected Insights: [what patterns to look for in results]
```

### 3. **SQL→NL Prompt (with Full Context Chain)**

```
SQL RESULTS TO NATURAL LANGUAGE EXPLANATION:

ORIGINAL CONTEXT:
User Intent: {original_intent}
Business Domain: {business_domain}
Audience Level: {audience_level}
Key Metrics: {key_metrics}
Expected Actions: {expected_actions}
Time Scope: {time_scope}

SQL EXECUTION CONTEXT:
Original Query: "{natural_language_query}"
Generated SQL: {sql_query}
Technical Approach: {technical_approach}
Data Scope: {data_scope}
Expected Insights: {expected_insights}

RESULTS DATA:
Row Count: {row_count}
Columns: {column_names}
Sample Data: {first_few_rows}
Summary Stats: {basic_statistics}

BUSINESS TERMINOLOGY:
{technical_to_business_mapping}

EXPLANATION INSTRUCTIONS:
1. Address the original intent: "{original_intent}"
2. Use {business_domain} terminology and context
3. Tailor complexity for {audience_level} audience
4. Focus on {key_metrics} that matter most
5. Connect findings to {expected_actions}
6. Reference {time_scope} appropriately

CONTEXT-AWARE RESPONSE FORMAT:
## Answer to: "{original_intent}"
[Direct answer to the original question]

## Key Insights for {business_domain}
- [Finding 1 related to {key_metrics}]
- [Finding 2 with business context]
- [Finding 3 relevant to {expected_actions}]

## What This Means for {audience_level}
[Interpretation tailored to audience and decision context]

## Recommended Actions
[Based on {expected_actions} and findings]

Generate explanation now:
```

## Context Variables to Track

### **Core Context Elements:**
```
CONTEXT_CHAIN = {
    # Original Request Context
    "original_query": "{user_input}",
    "user_intent": "{extracted_intent}",
    "query_type": "{aggregation|filter|comparison|trend|ranking}",
    
    # Business Context
    "business_domain": "{sales|finance|operations|marketing|hr}",
    "key_metrics": ["{metric1}", "{metric2}", "{metric3}"],
    "audience_level": "{executive|analyst|general_user}",
    "decision_context": "{operational|strategic|exploratory}",
    
    # Data Context
    "time_scope": "{current|historical|comparative}",
    "entities_involved": ["{customers}", "{orders}", "{products}"],
    "expected_actions": "{optimize|investigate|report|decide}",
    
    # SQL Generation Context
    "sql_approach": "{joins_used|aggregation_method|filter_logic}",
    "technical_complexity": "{simple|moderate|complex}",
    "business_logic_applied": ["{rule1}", "{rule2}"],
    
    # Results Context
    "data_characteristics": "{large_dataset|summary_stats|trending_data}",
    "key_findings": ["{insight1}", "{insight2}"],
    "anomalies_detected": ["{outlier1}", "{pattern1}"]
}
```

## Practical Implementation Examples

### **Example 1: Sales Analysis Chain**

**Step 1 - Context Extraction:**
```
Query: "Show me our top 5 customers by revenue last quarter"

Extracted Context:
- Intent: Identify highest value customers for relationship management
- Query Type: ranking
- Business Domain: sales
- Key Metrics: customer revenue, sales performance
- Time Scope: quarterly historical
- Audience: sales management
- Expected Actions: account prioritization, relationship investment
```

**Step 2 - SQL Generation (with context):**
```
Context-Aware SQL Generation:
- Focus on customer revenue ranking (intent)
- Include quarterly date filtering (time scope)
- Structure for sales management decisions (audience)
- Prepare for account prioritization actions (expected use)

Generated SQL + Context:
- Business Focus: customer value segmentation
- Technical Approach: aggregation with ranking
- Expected Insights: revenue concentration, customer tiers
```

**Step 3 - Results Explanation (with full context):**
```
Context-Driven Explanation:
- Address original intent: customer identification for relationship management
- Use sales terminology: "top customers", "revenue contribution"
- Focus on actionable insights for account prioritization
- Include recommendations for relationship investment
```

### **Example 2: Financial Trend Chain**

**Context Flow:**
```
NL: "How has our monthly revenue changed this year?"
↓
Context: {intent: track_performance, type: trend, domain: finance, 
         metrics: [revenue_growth], scope: year_over_year}
↓
SQL: [with trend analysis focus, monthly grouping, growth calculations]
↓
Explanation: {trend_direction, growth_rate, seasonal_patterns, 
            financial_implications, forecasting_insights}
```

## Context Persistence Strategies

### **Session-Based Context:**
```
SESSION_CONTEXT = {
    "user_profile": {
        "role": "sales_manager",
        "expertise_level": "intermediate",
        "preferred_detail": "moderate"
    },
    "business_context": {
        "domain": "e_commerce",
        "current_focus": "q4_performance",
        "decision_timeline": "immediate"
    },
    "conversation_history": [
        {"query": "...", "context": "...", "results": "..."}
    ]
}
```

### **Domain-Specific Context Templates:**
```
SALES_CONTEXT_TEMPLATE = {
    "key_metrics": ["revenue", "conversion_rate", "customer_acquisition"],
    "time_periods": ["daily", "weekly", "monthly", "quarterly"],
    "segmentation": ["customer_type", "product_category", "region"],
    "business_terms": {"customer_id": "customers", "order_total": "sales"}
}

FINANCE_CONTEXT_TEMPLATE = {
    "key_metrics": ["profit_margin", "cash_flow", "cost_efficiency"],
    "reporting_periods": ["monthly", "quarterly", "annual"],
    "categories": ["revenue", "expenses", "investments"],
    "business_terms": {"account_balance": "account value", "transaction": "payment"}
}
```

## Key Success Factors

1. **Preserve Original Intent** throughout the entire chain
2. **Maintain Business Context** - don't lose domain-specific meaning
3. **Track Audience Level** - keep explanation complexity appropriate
4. **Chain Insights Forward** - build upon previous context
5. **Map Technical→Business** terms consistently
6. **Remember Expected Actions** - keep recommendations relevant

The context becomes richer and more specific as it flows through each stage, ensuring the final explanation directly addresses the original business need with appropriate detail and terminology.