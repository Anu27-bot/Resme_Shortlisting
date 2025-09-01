from flask import Flask, render_template, request, jsonify, flash
import re
import pandas as pd
from io import StringIO
import logging
from contextlib import redirect_stdout, redirect_stderr
import anu
import os
import json
import traceback
from datetime import datetime, timedelta

app = Flask(__name__)
app.secret_key = 'your-secret-key-here'

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

project_root = os.path.dirname(os.path.abspath(__file__))
os.chdir(project_root)

# For production, consider using a database or file-based storage
# This simple implementation stores recent job IDs in memory
recent_job_ids = []

# Maximum number of recent job IDs to keep
MAX_RECENT_JOBS = 10

def add_to_recent_jobs(job_id):
    """Add a job ID to the recent jobs list, maintaining uniqueness and recency"""
    global recent_job_ids
    
    # Remove if already exists (to avoid duplicates)
    if job_id in recent_job_ids:
        recent_job_ids.remove(job_id)
    
    # Add to beginning of list
    recent_job_ids.insert(0, job_id)
    
    # Trim list if it exceeds maximum size
    if len(recent_job_ids) > MAX_RECENT_JOBS:
        recent_job_ids = recent_job_ids[:MAX_RECENT_JOBS]

@app.route('/', methods=['GET'])
def index():
    # Load your HTML page with recent job IDs
    return render_template('index.html', recent_job_ids=recent_job_ids)

@app.route('/process', methods=['POST'])
def process():
    job_id = request.form.get('job_id', "").strip()
    if not job_id:
        return jsonify({"error": "Job ID is required"}), 400
    
    # More flexible Job ID validation - allows any format with at least one letter and one number
    if not re.match(r'^[A-Za-z0-9-]+$', job_id) or not any(c.isalpha() for c in job_id) or not any(c.isdigit() for c in job_id):
        return jsonify({"error": "Invalid Job ID format. Job ID should contain letters, numbers, or hyphens, and must include at least one letter and one number."}), 400

    # Add to recent job IDs
    add_to_recent_jobs(job_id)

    resume_folder = os.path.join(project_root, "Resumes")
    try:
        if not os.path.exists(resume_folder):
            os.makedirs(resume_folder)
        test_file = os.path.join(resume_folder, "test_write.txt")
        with open(test_file, 'w') as f:
            f.write("Test")
        os.remove(test_file)
        logging.info(f"Write permissions verified for Resumes folder: {resume_folder}")
    except Exception as e:
        return jsonify({"error": f"Cannot write to Resumes folder: {str(e)}"}), 500

    # Capture stdout/stderr from anu.main
    stdout_buffer = StringIO()
    stderr_buffer = StringIO()
    
    try:
        with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
            anu.main(job_id)
    except Exception as e:
        error_traceback = traceback.format_exc()
        logging.error(f"Error in processing: {e}\n{error_traceback}")
        return jsonify({
            "error": f"Error processing resumes: {str(e)}",
            "details": "Check if the Job ID exists and is properly formatted",
            "stdout": stdout_buffer.getvalue(),
            "stderr": stderr_buffer.getvalue()
        }), 500

    stdout_output = stdout_buffer.getvalue()
    stderr_output = stderr_buffer.getvalue()
    logging.info(f"anu.main stdout: {stdout_output}")
    
    if stderr_output:
        logging.error(f"anu.main stderr: {stderr_output}")

    # Try to determine the output CSV path dynamically
    output_csv = getattr(anu, 'OUTPUT_CSV', None)
    if not output_csv or not os.path.exists(output_csv):
        # Try common output file names
        possible_outputs = [
            os.path.join(project_root, 'output.csv'),
            os.path.join(project_root, 'candidate_rankings.csv'),
            os.path.join(project_root, f'rankings_{job_id}.csv'),
            os.path.join(project_root, 'results', 'output.csv'),
            os.path.join(project_root, 'results', f'rankings_{job_id}.csv'),
        ]
        
        for possible_path in possible_outputs:
            if os.path.exists(possible_path):
                output_csv = possible_path
                break
        else:
            # No output file found
            return jsonify({
                "error": f'No results found for Job ID "{job_id}". Please check if the Job ID is correct.',
                "stdout": stdout_output,
                "stderr": stderr_output
            }), 404

    try:
        df = pd.read_csv(output_csv)
        if df.empty:
            return jsonify({"error": f'Results for Job ID "{job_id}" are empty.'}), 404
    except Exception as e:
        return jsonify({"error": f"Failed to read results: {str(e)}"}), 500

    # Extract job role
    job_role = "N/A"
    for line in stdout_output.splitlines():
        if "Job Role:" in line:
            job_role_part = line.split("Job Role:", 1)[1].strip()
            if job_role_part and job_role_part.lower() != "n/a":
                job_role = job_role_part
                break

    if job_role == "N/A" and 'Job Role' in df.columns:
        job_roles = df['Job Role'].dropna()
        job_roles = job_roles[job_roles != "N/A"].drop_duplicates()
        if not job_roles.empty:
            for role in job_roles:
                if any(keyword in role.lower() for keyword in 
                       ['developer', 'engineer', 'manager', 'analyst', 'architect']):
                    job_role = role
                    break
            else:
                job_role = job_roles.iloc[0]  # Use the first available role

    # Extract subject skills
    subject_skills = []
    for line in stdout_output.splitlines():
        if "Subject Skills:" in line:
            skills_part = line.split("Subject Skills:", 1)[1].strip()
            if skills_part and skills_part.lower() != "none":
                skills = [s.strip() for s in skills_part.split(",") if s.strip()]
                subject_skills.extend(skills)
            break

    # Columns to include
    available_columns = [
        "Rank", "Name", "Current Location", 
        "Experience", "Certification Count", "Government Work", 
        "Matching Skills", "Matching Skills Count"
    ]
    columns_order = [col for col in available_columns if col in df.columns]

    # Ensure required columns exist
    if "Matching Skills" not in df.columns:
        df["Matching Skills"] = "N/A"
    if "Matching Skills Count" not in df.columns:
        df["Matching Skills Count"] = 0

    table_data = df[columns_order].to_dict(orient='records')

    return jsonify({
        "job_id": job_id,
        "job_role": job_role,
        "subject_skills": subject_skills if subject_skills else [],
        "columns": columns_order,
        "table_data": table_data,
        "candidate_count": len(table_data)
    })

@app.route('/recent_jobs', methods=['GET'])
def get_recent_jobs():
    return jsonify({"recent_job_ids": recent_job_ids})

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
