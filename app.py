from flask import Flask, render_template, request, redirect, url_for, flash
import re
import pandas as pd
from io import StringIO
import sys
import logging
from contextlib import redirect_stdout, redirect_stderr
import anu
import os

app = Flask(__name__)
app.secret_key = 'your-secret-key-here'

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

project_root = os.path.dirname(os.path.abspath(__file__))
os.chdir(project_root)

@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')

@app.route('/process', methods=['GET', 'POST'])
def process():
    if request.method == 'POST':
        job_id = request.form.get('job_id').strip()
        if not job_id:
            flash('Job ID is required', 'error')
            return redirect(url_for('process'))

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
            flash(f'Cannot write to Resumes folder: {str(e)}. Please check folder permissions.', 'error')
            logging.error(f"Failed to write to Resumes folder: {e}")
            return redirect(url_for('process'))

        stdout_buffer = StringIO()
        stderr_buffer = StringIO()
        with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
            try:
                anu.main(job_id)
            except Exception as e:
                flash(f'Error processing resumes: {str(e)}', 'error')
                logging.error(f"Error in processing: {e}")
                logging.error(f"Captured stdout: {stdout_buffer.getvalue()}")
                logging.error(f"Captured stderr: {stderr_buffer.getvalue()}")
                return redirect(url_for('process'))

        stdout_output = stdout_buffer.getvalue()
        stderr_output = stderr_buffer.getvalue()
        logging.info(f"anu.main stdout: {stdout_output}")
        logging.info(f"anu.main stderr: {stderr_output}")

        output_csv = anu.OUTPUT_CSV
        if not os.path.exists(output_csv):
            error_message = f'No results found for Job ID "{job_id}". Please ensure emails with this Job ID and resume attachments exist.'
            if "No emails found related to job ID" in stdout_output:
                error_message += f' Detailed error: No emails were found matching the Job ID "{job_id}".'
            elif "Failed to process resumes" in stdout_output:
                error_message += f' Detailed error: Failed to process resumes. Check logs for details.'
            elif "No candidate data or resumes found" in stdout_output:
                error_message += f' Detailed error: No candidate data or resumes were found for comparison.'
            flash(error_message, 'error')
            logging.error(f"Output CSV not found: {output_csv}")
            return redirect(url_for('process'))

        try:
            df = pd.read_csv(output_csv)
            if df.empty:
                flash(f'Results for Job ID "{job_id}" are empty. No candidates were found.', 'error')
                logging.warning(f"Output CSV is empty: {output_csv}")
                return redirect(url_for('process'))
        except Exception as e:
            flash(f'Failed to read results: {str(e)}', 'error')
            logging.error(f"Error reading CSV: {e}")
            return redirect(url_for('process'))

        # Extract job role from stdout first, if available
        job_role = "N/A"
        for line in stdout_output.splitlines():
            if "Job Role:" in line:
                job_role_part = line.split("Job Role:", 1)[1].strip()
                if job_role_part and job_role_part.lower() != "n/a":
                    job_role = job_role_part
                    break

        # If job role not found in stdout, fall back to DataFrame
        if job_role == "N/A" and 'Job Role' in df.columns:
            job_roles = df['Job Role'].dropna()
            job_roles = job_roles[job_roles != "N/A"].drop_duplicates()
            if not job_roles.empty:
                for role in job_roles:
                    if not re.match(r'^[A-Z][a-z]+[A-Z][a-z]+$', role) and any(keyword in role.lower() for keyword in ['developer', 'engineer', 'manager', 'analyst', 'architect']):
                        job_role = role
                        break
            if job_role == "N/A":
                logging.warning("No valid job role found in DataFrame; defaulting to 'N/A'")

        # Extract subject skills from stdout
        subject_skills = []
        for line in stdout_output.splitlines():
            if "Subject Skills:" in line:
                skills_part = line.split("Subject Skills:", 1)[1].strip()
                if skills_part and skills_part.lower() != "none":
                    skills = [s.strip() for s in skills_part.split(",") if s.strip()]
                    subject_skills.extend(skills)
                break

        # Define the correct column names
        available_columns = [
            "Rank", "Name", "Current Location", 
            "Experience", "Certification Count", "Government Work", 
            "Matching Skills", "Matching Skills Count"  # Correct column names
        ]
        
        # Filter to only include columns that actually exist
        columns_order = [col for col in available_columns if col in df.columns]
        
        # Add fallback for missing columns
        if "Matching Skills" not in df.columns:
            df["Matching Skills"] = "N/A"
        if "Matching Skills Count" not in df.columns:
            df["Matching Skills Count"] = 0

        table_data = df[columns_order].to_dict(orient='records')

        return render_template(
            'process.jinja',
            subject_skills=subject_skills if subject_skills else None,
            job_role=job_role,
            table_data=table_data,
            columns=columns_order
        )

    return render_template(
        'process.jinja',
        subject_skills=None,
        job_role=None,
        table_data=[],
        columns=[]
    )

if __name__ == '__main__':
    app.run(debug=True)
    # Get port from environment variable or default to 5000
    port = int(os.environ.get('PORT', 5000))
    # Bind to 0.0.0.0 (all network interfaces)
    app.run(host='0.0.0.0', port=port, debug=False)
