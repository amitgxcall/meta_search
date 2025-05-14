import csv
import random
import datetime
import hashlib
import numpy as np
from faker import Faker

fake = Faker()
Faker.seed(42)  # For reproducibility
random.seed(42)
np.random.seed(42)

# Define job categories and related terms for testing different search capabilities
JOB_CATEGORIES = {
    "data": {
        "prefixes": ["data", "db", "database", "file", "storage", "record", "doc", "document"],
        "actions": ["sync", "transfer", "migration", "extraction", "load", "process", "transform", "update", "validate", "clean", "export", "import"],
        "related_concepts": ["etl", "pipeline", "warehouse", "lake", "mart", "store", "repository", "collection"],
    },
    "system": {
        "prefixes": ["system", "server", "node", "cluster", "infrastructure", "platform", "network", "service"],
        "actions": ["monitor", "check", "scan", "audit", "provision", "deploy", "configure", "initialize", "restart", "reboot"],
        "related_concepts": ["health", "performance", "stability", "availability", "status", "metrics", "diagnostics"],
    },
    "user": {
        "prefixes": ["user", "account", "profile", "identity", "authentication", "authorization"],
        "actions": ["sync", "create", "update", "delete", "verify", "validate", "manage", "provision", "register", "authenticate"],
        "related_concepts": ["permission", "role", "group", "access", "security", "credential", "login"],
    },
    "security": {
        "prefixes": ["security", "protection", "firewall", "threat", "vulnerability", "risk"],
        "actions": ["scan", "analyze", "detect", "prevent", "monitor", "encrypt", "decrypt", "secure", "protect"],
        "related_concepts": ["threat", "risk", "compliance", "policy", "breach", "patch", "update"],
    },
    "backup": {
        "prefixes": ["backup", "archive", "snapshot", "copy", "replica"],
        "actions": ["create", "store", "restore", "verify", "rotate", "compress", "encrypt", "schedule"],
        "related_concepts": ["recovery", "disaster", "preservation", "retention", "redundancy"],
    },
    "notification": {
        "prefixes": ["notification", "alert", "message", "email", "sms", "communication"],
        "actions": ["send", "deliver", "process", "queue", "schedule", "template"],
        "related_concepts": ["subscription", "channel", "recipient", "broadcast", "publish"],
    },
    "report": {
        "prefixes": ["report", "analytics", "dashboard", "visualization", "statistics", "metrics"],
        "actions": ["generate", "compile", "analyze", "export", "schedule", "distribute", "calculate"],
        "related_concepts": ["kpi", "insight", "trend", "summary", "performance", "business intelligence"],
    },
    "integration": {
        "prefixes": ["integration", "connector", "api", "webhook", "interface", "synchronization"],
        "actions": ["connect", "link", "bind", "sync", "pull", "push", "exchange", "transform"],
        "related_concepts": ["middleware", "gateway", "bridge", "adapter", "protocol", "interoperability"],
    },
    "automation": {
        "prefixes": ["automation", "workflow", "bot", "process", "task", "job"],
        "actions": ["automate", "orchestrate", "schedule", "trigger", "execute", "run", "manage"],
        "related_concepts": ["efficiency", "optimization", "script", "routine", "productivity"],
    },
    "maintenance": {
        "prefixes": ["maintenance", "cleanup", "housekeeping", "upkeep", "care"],
        "actions": ["clean", "purge", "delete", "archive", "optimize", "defragment", "organize"],
        "related_concepts": ["efficiency", "performance", "storage", "space", "optimization"],
    }
}

# Environment details
ENVIRONMENTS = ["production", "staging", "development", "testing", "qa", "sandbox", "demo", "training"]
NODE_PREFIXES = ["node", "server", "vm", "container", "pod", "instance", "worker", "app"]
NODE_LOCATIONS = ["east", "west", "north", "south", "central", "eu", "us", "asia", "primary", "secondary"]
STATUS_OPTIONS = ["completed", "failed", "cancelled", "running", "queued", "scheduled", "suspended", "timeout", "warning"]
PRIORITY_OPTIONS = ["critical", "high", "medium", "low", "background"]
USERS = ["system", "admin", "scheduler", "api", "cron", "user", "pipeline", "workflow", "trigger", "event"] + [fake.user_name() for _ in range(20)]

# Generate a deterministic but varied job name
def generate_job_name(job_id):
    # Seed the random generator with the job_id for consistency
    random.seed(job_id)
    
    # Pick a category
    category = random.choice(list(JOB_CATEGORIES.keys()))
    category_info = JOB_CATEGORIES[category]
    
    # Different name generation patterns
    pattern = random.randint(1, 6)
    
    if pattern == 1:
        # prefix_action
        prefix = random.choice(category_info["prefixes"])
        action = random.choice(category_info["actions"])
        return f"{prefix}_{action}"
    elif pattern == 2:
        # action_prefix
        prefix = random.choice(category_info["prefixes"])
        action = random.choice(category_info["actions"])
        return f"{action}_{prefix}"
    elif pattern == 3:
        # prefix_concept
        prefix = random.choice(category_info["prefixes"])
        concept = random.choice(category_info["related_concepts"])
        return f"{prefix}_{concept}"
    elif pattern == 4:
        # concept_action
        concept = random.choice(category_info["related_concepts"])
        action = random.choice(category_info["actions"])
        return f"{concept}_{action}"
    elif pattern == 5:
        # standalone word
        choices = (category_info["prefixes"] + 
                  category_info["actions"] + 
                  category_info["related_concepts"])
        return random.choice(choices)
    else:
        # with adjective
        adjectives = ["scheduled", "automated", "daily", "weekly", "monthly", "incremental", "full", "periodic", 
                     "adaptive", "intelligent", "smart", "dynamic", "static", "custom", "rapid", "batch"]
        word = random.choice(category_info["prefixes"] + category_info["actions"])
        adjective = random.choice(adjectives)
        return f"{adjective}_{word}"

def generate_job_description(job_name, category, environment):
    """Generate a realistic job description based on job name and category."""
    # Split the job name into components
    components = job_name.split('_')
    
    # Start with standard templates based on category
    templates = {
        "data": [
            "Processes data from {source} to {destination}.",
            "Handles data {action} operations for {target}.",
            "Manages {type} data across {environment} environment.",
            "{Action} data from various sources into structured format.",
            "Performs ETL operations on {source} data."
        ],
        "system": [
            "{Action} system health and resources on {node}.",
            "Ensures system stability in {environment} environment.",
            "Verifies system configurations across {node} instances.",
            "Manages system resources and performance metrics.",
            "Handles system {action} operations."
        ],
        "user": [
            "Manages user accounts and permissions.",
            "Synchronizes user data between {source} and {destination}.",
            "Updates user profiles in {environment}.",
            "Handles user {action} operations.",
            "Processes user authentication and authorization."
        ],
        "security": [
            "Performs security {action} to protect system integrity.",
            "Monitors security events and logs in {environment}.",
            "Ensures compliance with security policies.",
            "Detects and mitigates security threats.",
            "Manages security configurations across {node}."
        ],
        "backup": [
            "Creates backup of {target} data in {environment}.",
            "Manages {type} backup operations.",
            "Verifies backup integrity and consistency.",
            "Handles scheduled backups for critical systems.",
            "Maintains backup rotation and retention policies."
        ],
        "notification": [
            "Sends notifications to {target} users.",
            "Processes {type} notifications across channels.",
            "Manages notification delivery and tracking.",
            "Handles message queue for {environment} alerts.",
            "Distributes system alerts and user notifications."
        ],
        "report": [
            "Generates {type} reports for {target}.",
            "Compiles analytics data into structured reports.",
            "Processes {environment} metrics for reporting.",
            "Creates dashboards and visualizations from system data.",
            "Analyzes performance metrics and generates insights."
        ],
        "integration": [
            "Connects {source} with {destination} systems.",
            "Manages data flow between integrated systems.",
            "Synchronizes data across {environment} boundaries.",
            "Handles API connections and data transformations.",
            "Ensures reliable data exchange between services."
        ],
        "automation": [
            "Automates {target} workflows in {environment}.",
            "Manages scheduled tasks and processes.",
            "Orchestrates complex operational sequences.",
            "Handles automated {action} procedures.",
            "Runs routine processes without manual intervention."
        ],
        "maintenance": [
            "Performs {type} maintenance on {target} systems.",
            "Cleans up stale data and optimizes storage.",
            "Handles routine maintenance tasks in {environment}.",
            "Manages system optimization and cleanup.",
            "Ensures system health through regular maintenance."
        ]
    }
    
    # Select template
    if category in templates:
        template = random.choice(templates[category])
    else:
        # Default template if category not found
        template = "Handles {action} operations for {target} in {environment}."
    
    # Fill in template variables
    sources = ["database", "API", "file system", "cloud storage", "data warehouse", "user directory", "external service"]
    destinations = ["data warehouse", "reporting system", "backup storage", "production system", "analytics platform"]
    actions = ["synchronization", "processing", "validation", "transformation", "aggregation", "migration", "integration"]
    targets = ["user data", "system config", "transaction records", "application logs", "customer information", "product catalog"]
    types = ["incremental", "full", "delta", "scheduled", "on-demand", "automated", "background"]
    nodes = [f"{random.choice(NODE_PREFIXES)}-{random.choice(NODE_LOCATIONS)}-{random.randint(1, 10):02d}" for _ in range(5)]
    
    # Replace template variables
    description = template.replace("{source}", random.choice(sources))
    description = description.replace("{destination}", random.choice(destinations))
    description = description.replace("{action}", random.choice(actions))
    description = description.replace("{Action}", random.choice(actions).capitalize())
    description = description.replace("{target}", random.choice(targets))
    description = description.replace("{type}", random.choice(types))
    description = description.replace("{node}", random.choice(nodes))
    description = description.replace("{environment}", environment)
    
    # Add a sentence about frequency or timing
    frequency = ["daily", "hourly", "weekly", "monthly", "every 6 hours", "twice daily", "on-demand", "event-triggered"]
    timing_sentence = f"Runs {random.choice(frequency)}."
    
    # Add a sentence about importance or criticality sometimes
    if random.random() < 0.3:
        importance = ["Critical", "Important", "Necessary", "Required", "Essential"]
        importance_sentence = f"{random.choice(importance)} for {random.choice(['business operations', 'system stability', 'data integrity', 'user experience', 'compliance requirements'])}."
        description += f" {importance_sentence}"
    
    # Add technical details sometimes
    if random.random() < 0.4:
        tech_details = [
            f"Uses {random.choice(['REST API', 'GraphQL', 'JDBC', 'ODBC', 'file transfer', 'message queue'])} for data access.",
            f"Implemented in {random.choice(['Python', 'Java', 'Go', 'JavaScript', 'C#', 'Bash'])}.",
            f"Configured with {random.choice(['high redundancy', 'automatic retries', 'failure notification', 'detailed logging'])}.",
            f"Optimized for {random.choice(['performance', 'reliability', 'low resource usage', 'scalability'])}.",
            f"Includes {random.choice(['error handling', 'validation checks', 'audit logging', 'performance metrics'])}."
        ]
        description += f" {random.choice(tech_details)}"
    
    # Add the timing sentence at the end
    description += f" {timing_sentence}"
    
    return description

def generate_datetime(base_date, min_days, max_days, min_hours=0, max_hours=23):
    """Generate a datetime within a range from the base date."""
    days_offset = random.randint(min_days, max_days)
    hours_offset = random.randint(min_hours, max_hours)
    minutes_offset = random.randint(0, 59)
    
    return base_date + datetime.timedelta(
        days=days_offset, 
        hours=hours_offset, 
        minutes=minutes_offset
    )

def generate_error_message(status, job_name):
    """Generate a realistic error message if the job failed."""
    if status != "failed":
        return ""
    
    error_types = [
        "Connection error: Unable to connect to {resource}",
        "Timeout error: Operation exceeded time limit of {timeout} seconds",
        "Resource error: Insufficient {resource} available",
        "Validation error: {resource} data failed schema validation",
        "Permission error: Access denied to {resource}",
        "Dependency error: Required dependency {resource} not available",
        "Configuration error: Invalid configuration for {parameter}",
        "Runtime error: Exception occurred during execution: {error}",
        "Data error: Invalid or corrupt data in {resource}",
        "System error: {resource} is unavailable or not responding",
        "Quota error: Exceeded {resource} quota limit",
        "Processing error: Failed to process {resource}"
    ]
    
    resources = ["database", "API endpoint", "file system", "network", "storage", "memory", "CPU", "service", "external system", "configuration", "credentials"]
    timeouts = ["30", "60", "120", "300", "600"]
    parameters = ["connection string", "authentication", "endpoint URL", "resource path", "job configuration", "input parameters"]
    errors = ["NullPointerException", "OutOfMemoryError", "IOException", "TimeoutException", "AuthenticationException", "ConfigurationException"]
    
    error_template = random.choice(error_types)
    
    # Replace template variables
    error = error_template.replace("{resource}", random.choice(resources))
    error = error.replace("{timeout}", random.choice(timeouts))
    error = error.replace("{parameter}", random.choice(parameters))
    error = error.replace("{error}", random.choice(errors))
    
    # Add job-specific context sometimes
    if random.random() < 0.5:
        components = job_name.split('_')
        if components:
            context_phrase = f" during {components[0]} operation" if len(components) > 0 else ""
            target_phrase = f" on {components[1]} system" if len(components) > 1 else ""
            error += context_phrase + target_phrase
    
    return error

def generate_test_data(num_records=1000):
    """Generate test data for job_details.csv"""
    records = []
    base_date = datetime.datetime(2025, 1, 1)
    
    # Generate job IDs in different patterns to test search
    # Mix of sequential, gaps, and different formats
    job_ids = []
    
    # Sequential block
    job_ids.extend([f"{i:04d}" for i in range(1, 501)])
    
    # IDs with specific patterns for testing
    job_ids.extend([f"J{i:03d}" for i in range(1, 101)])  # J001-J100
    job_ids.extend([f"TASK-{i:03d}" for i in range(1, 101)])  # TASK-001-TASK-100
    job_ids.extend([f"JOB-{random.randint(1000, 9999)}" for _ in range(100)])  # Random JOB-XXXX
    job_ids.extend([f"PROC-{fake.uuid4()[:8]}" for _ in range(100)])  # Process IDs with UUID
    job_ids.extend([f"ID-{i*10:04d}" for i in range(1, 101)])  # ID-0010, ID-0020, etc.
    
    # Ensure we have enough IDs
    if len(job_ids) < num_records:
        # Add more sequential IDs if needed
        additional = num_records - len(job_ids)
        job_ids.extend([f"{i+10000:05d}" for i in range(additional)])
    
    # Shuffle the IDs to mix them up
    random.shuffle(job_ids)
    
    # Take only what we need
    job_ids = job_ids[:num_records]
    
    # Now generate the records
    for i, job_id in enumerate(job_ids):
        # Reset the random seed for each job to ensure different but consistent values
        random.seed(i + 42)
        
        # Generate job name - this will determine the category
        job_name = generate_job_name(i)
        
        # Determine the category based on job name
        category = None
        for cat, info in JOB_CATEGORIES.items():
            words = job_name.split('_')
            for word in words:
                if (word in info["prefixes"] or 
                    word in info["actions"] or 
                    word in info["related_concepts"]):
                    category = cat
                    break
            if category:
                break
        
        # Default to "system" if no category identified
        if not category:
            category = "system"
        
        # Other fields
        environment = random.choice(ENVIRONMENTS)
        node_name = f"{random.choice(NODE_PREFIXES)}-{random.choice(NODE_LOCATIONS)}-{random.randint(1, 99):02d}"
        status = random.choice(STATUS_OPTIONS)
        
        # Generate execution times
        execution_start_time = generate_datetime(base_date, -90, 0, 0, 23)
        
        # Duration based on job type and status
        if status == "running":
            execution_end_time = ""
            duration_minutes = ""
        else:
            # Generate a realistic duration based on the job type
            if "backup" in job_name or "export" in job_name or "report" in job_name:
                # These jobs typically take longer
                duration = random.uniform(5, 120)
            elif "check" in job_name or "monitor" in job_name or "validate" in job_name:
                # These jobs are typically quick
                duration = random.uniform(0.5, 10)
            else:
                # Average jobs
                duration = random.uniform(1, 60)
                
            # Add random variation
            duration *= random.uniform(0.8, 1.2)
            
            # Calculate end time
            duration_minutes = round(duration, 2)
            execution_end_time = execution_start_time + datetime.timedelta(minutes=duration)
        
        # Generate other metrics
        cpu_usage_percent = round(random.uniform(5, 95), 1)
        memory_usage_mb = round(random.uniform(50, 8192), 1)
        queue_wait_time_sec = round(random.uniform(0, 300), 1)
        priority = random.choice(PRIORITY_OPTIONS)
        started_by = random.choice(USERS)
        
        # Generate error message for failed jobs
        error_message = generate_error_message(status, job_name)
        
        # Generate a detailed description
        description = generate_job_description(job_name, category, environment)
        
        # Format dates as strings
        start_time_str = execution_start_time.strftime("%Y-%m-%dT%H:%M:%S") if execution_start_time else ""
        end_time_str = execution_end_time.strftime("%Y-%m-%dT%H:%M:%S") if execution_end_time and status != "running" else ""
        
        # Create the record
        record = {
            "job_id": job_id,
            "job_name": job_name,
            "execution_start_time": start_time_str,
            "execution_end_time": end_time_str,
            "duration_minutes": duration_minutes,
            "started_by": started_by,
            "status": status,
            "error_message": error_message,
            "priority": priority,
            "environment": environment,
            "node_name": node_name,
            "cpu_usage_percent": cpu_usage_percent,
            "memory_usage_mb": memory_usage_mb,
            "queue_wait_time_sec": queue_wait_time_sec,
            "description": description,
            "category": category
        }
        
        records.append(record)
    
    return records

# Generate the data
job_records = generate_test_data(1000)

# Write to CSV
with open('job_details_expanded.csv', 'w', newline='') as csvfile:
    fieldnames = list(job_records[0].keys())
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    
    writer.writeheader()
    for record in job_records:
        writer.writerow(record)

print(f"Generated 1000 records in job_details_expanded.csv")