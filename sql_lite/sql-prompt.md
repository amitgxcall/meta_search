Natural Language to SQL Prompt Design Guide
Core Prompt Design Criteria
1. Schema Context (Most Critical)
Always provide complete database schema information:
SCHEMA:
Table: customers
- customer_id (INTEGER, PRIMARY KEY)
- name (TEXT, NOT NULL)
- email (TEXT, UNIQUE)
- registration_date (DATE)
- status (TEXT) -- Values: 'active', 'inactive', 'pending'

Table: orders
- order_id (INTEGER, PRIMARY KEY)
- customer_id (INTEGER, FOREIGN KEY -> customers.customer_id)
- order_date (DATE)
- total_amount (DECIMAL(10,2))
- status (TEXT) -- Values: 'pending', 'shipped', 'delivered', 'cancelled'

Table: order_items
- item_id (INTEGER, PRIMARY KEY)
- order_id (INTEGER, FOREIGN KEY -> orders.order_id)
- product_name (TEXT)
- quantity (INTEGER)
- unit_price (DECIMAL(10,2))
2. Clear Instructions & Constraints
Define what the model should and shouldn't do:
INSTRUCTIONS:
- Generate only SELECT queries
- Use proper table aliases
- Include appropriate WHERE clauses for filters
- Use JOINs when querying multiple tables
- Handle NULL values appropriately
- Use LIMIT for potentially large result sets
- Return "CANNOT_QUERY" if the request is impossible with given schema
3. Example Patterns (Few-Shot Learning)
Provide diverse query examples:
EXAMPLES:

Natural Language: "Show me all active customers"
SQL: SELECT * FROM customers WHERE status = 'active';

Natural Language: "How many orders were placed last month?"
SQL: SELECT COUNT(*) FROM orders WHERE order_date >= DATE('now', 'start of month', '-1 month') AND order_date < DATE('now', 'start of month');

Natural Language: "List customers with their total order amounts"
SQL: SELECT c.name, c.email, COALESCE(SUM(o.total_amount), 0) as total_spent 
     FROM customers c 
     LEFT JOIN orders o ON c.customer_id = o.customer_id 
     GROUP BY c.customer_id, c.name, c.email;

Natural Language: "Find top 5 customers by spending"
SQL: SELECT c.name, SUM(o.total_amount) as total_spent 
     FROM customers c 
     JOIN orders o ON c.customer_id = o.customer_id 
     GROUP BY c.customer_id, c.name 
     ORDER BY total_spent DESC 
     LIMIT 5;
Handling Different Query Types
1. Basic Filtering Queries
Pattern: "Show/Find/Get [entities] where [condition]"

Examples to include:
- "Show customers from New York" → WHERE city = 'New York'
- "Find orders over $100" → WHERE total_amount > 100
- "Get active customers registered this year" → WHERE status = 'active' AND registration_date >= '2024-01-01'
2. Aggregation Queries
Pattern: "Count/Sum/Average [metric] by [group]"

Examples:
- "Count orders by month" → GROUP BY strftime('%Y-%m', order_date)
- "Average order value by customer" → GROUP BY customer_id with AVG()
- "Total sales by product" → SUM(quantity * unit_price) GROUP BY product_name
3. Comparison & Ranking Queries
Pattern: "Top/Bottom/Highest/Lowest [N] [entities] by [metric]"

Examples:
- "Top 10 customers by spending" → ORDER BY total_spent DESC LIMIT 10
- "Lowest selling products" → ORDER BY total_sold ASC
- "Customers who spent more than average" → HAVING total_spent > (SELECT AVG(...))
4. Time-Based Queries
Pattern: "[Time period] [aggregation/filter]"

Provide date function examples:
- "This month" → WHERE date >= DATE('now', 'start of month')
- "Last 30 days" → WHERE date >= DATE('now', '-30 days')
- "Year over year comparison" → Use CASE/CTE with date functions
5. Relationship Queries
Pattern: "[Entity1] with/without [Entity2]"

Examples:
- "Customers with no orders" → LEFT JOIN with WHERE orders.id IS NULL
- "Products never ordered" → NOT EXISTS subquery
- "Customers and their latest order" → Window functions or correlated subquery
Advanced Prompt Structure
Complete Template:
You are an expert SQL query generator. Convert natural language to SQL queries using the provided schema.

DATABASE SCHEMA:
[Include complete schema with data types, constraints, and sample values]

QUERY RULES:
1. Generate only SELECT statements
2. Use table aliases (c for customers, o for orders, etc.)
3. Handle edge cases (NULL values, empty results)
4. Use appropriate JOINs based on relationships
5. Include LIMIT clauses for potentially large results
6. Use proper date/time functions for temporal queries
7. Return "INSUFFICIENT_INFO" if schema lacks required information

QUERY PATTERNS:
[Include 15-20 diverse examples covering all query types]

COMMON FUNCTIONS TO USE:
- Dates: DATE(), strftime(), DATE('now', '-X days')
- Aggregation: COUNT(), SUM(), AVG(), MIN(), MAX()
- String: LIKE, LOWER(), UPPER(), TRIM()
- Conditional: CASE WHEN, COALESCE(), NULLIF()

Now convert this natural language to SQL:
"{user_input}"

SQL:
Handling Ambiguity & Edge Cases
1. Disambiguation Prompts
AMBIGUITY HANDLING:
- If multiple interpretations exist, choose the most common business use case
- For vague terms like "recent", assume last 30 days
- For "top/best", assume highest numerical value unless context suggests otherwise
- When field names are unclear, use the closest matching column name
2. Error Handling
ERROR RESPONSES:
- "SCHEMA_MISSING: [table/column] not found in schema"
- "AMBIGUOUS_REQUEST: Please specify [clarification needed]"
- "COMPLEX_QUERY: Requires multiple steps or business logic not in schema"
3. Business Logic Integration
BUSINESS CONTEXT:
- "Active customers" means status = 'active'
- "Recent orders" means last 30 days
- "High-value customers" means total spending > $1000
- "Popular products" means highest quantity sold
Testing & Validation Approach
1. Test Categories
Simple filters (WHERE clauses)
Aggregations (GROUP BY, HAVING)
Joins (INNER, LEFT, complex relationships)
Subqueries (correlated, EXISTS, IN)
Window functions (ranking, running totals)
Date/time operations
String matching and manipulation
2. Edge Case Testing
Test these scenarios:
- Empty results
- NULL value handling
- Division by zero
- Date boundary conditions
- Case sensitivity
- Special characters in data
3. Progressive Complexity
Start with simple queries and gradually increase complexity:
Single table, single condition
Single table, multiple conditions
Multiple tables, simple joins
Aggregation with grouping
Complex joins with filtering
Subqueries and window functions
Sample Implementation Strategy
1. Iterative Improvement
python
def improve_nl_to_sql_prompt(failed_cases):
    # Analyze failed conversions
    # Add new examples to prompt
    # Refine schema descriptions
    # Update business logic rules
    return updated_prompt
2. Validation Pipeline
python
def validate_generated_sql(sql, nl_query, schema):
    # Syntax validation
    # Schema compatibility check
    # Business logic verification
    # Performance consideration
    return validation_result
Key Success Factors
Rich Schema Context: Include data types, constraints, relationships, and sample values
Diverse Examples: Cover all query patterns your users might need
Clear Business Rules: Define domain-specific interpretations
Error Handling: Gracefully handle ambiguous or impossible requests
Iterative Refinement: Continuously improve based on failed cases
Performance Awareness: Include hints about query optimization
Common Pitfalls to Avoid
Insufficient schema information
Too few or too similar examples
Ignoring NULL value handling
Missing business context
No ambiguity resolution strategy
Overly complex single prompts (break into steps if needed)
Not testing edge cases
Ignoring SQL injection concerns (if user input isn't sanitized)
The key is to make the model an expert in your specific database structure and business domain through comprehensive context and examples.
