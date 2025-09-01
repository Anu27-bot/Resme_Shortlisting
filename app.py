from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
import re
import pandas as pd
from io import StringIO
import sys
import logging
from contextlib import redirect_stdout, redirect_stderr
import anu
import os
import tempfile
import traceback

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

        # Create a temporary directory for this processing session
        with tempfile.TemporaryDirectory() as temp_dir:
            resume_folder = os.path.join(temp_dir, "Resumes")
            os.makedirs(resume_folder, exist_ok=True)
            
            # Set the resume folder for this session
            original_resume_folder = anu.RESUME_FOLDER
            anu.RESUME_FOLDER = resume_folder
            
            try:
                stdout_buffer = StringIO()
                stderr_buffer = StringIO()
                
                with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
                    try:
                        anu.main(job_id)
                    except Exception as e:
                        error_msg = f'Error processing resumes: {str(e)}'
                        flash(error_msg, 'error')
                        logging.error(f"Error in processing: {e}")
                        logging.error(f"Captured stdout: {stdout_buffer.getvalue()}")
                        logging.error(f"Captured stderr: {stderr_buffer.getvalue()}")
                        return redirect(url_for('process'))

                stdout_output = stdout_buffer.getvalue()
                stderr_output = stderr_buffer.getvalue()
                logging.info(f"anu.main stdout: {stdout_output}")
                logging.info(f"anu.main stderr: {stderr_output}")

                output_csv = os.path.join(resume_folder, "resume_analysis.csv")
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

                # Extract job role from the data
                job_role = "N/A"
                if 'Job Role' in df.columns:
                    job_roles = df['Job Role'].dropna().unique()
                    if len(job_roles) > 0:
                        job_role = job_roles[0]

                # Extract subject skills from the data
                subject_skills = []
                if 'Subject Skills' in df.columns:
                    skills = df['Subject Skills'].dropna().unique()
                    if len(skills) > 0:
                        subject_skills = [s.strip() for s in skills[0].split(',') if s.strip()]

                # Define the correct column names
                available_columns = [
                    "Rank", "Name", "Current Location", 
                    "Experience", "Certification Count", "Government Work", 
                    "Matching Skills", "Matching Skills Count"
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
                    job_id=job_id,
                    subject_skills=subject_skills if subject_skills else [],
                    job_role=job_role,
                    table_data=table_data,
                    columns=columns_order
                )

            finally:
                # Restore original resume folder
                anu.RESUME_FOLDER = original_resume_folder

    return render_template(
        'process.jinja',
        job_id=None,
        subject_skills=[],
        job_role=None,
        table_data=[],
        columns=[]
    )

@app.route('/api/process/<job_id>', methods=['GET'])
def api_process(job_id):
    """API endpoint for processing job IDs"""
    try:
        # Create a temporary directory for this processing session
        with tempfile.TemporaryDirectory() as temp_dir:
            resume_folder = os.path.join(temp_dir, "Resumes")
            os.makedirs(resume_folder, exist_ok=True)
            
            # Set the resume folder for this session
            original_resume_folder = anu.RESUME_FOLDER
            anu.RESUME_FOLDER = resume_folder
            
            try:
                stdout_buffer = StringIO()
                stderr_buffer = StringIO()
                
                with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
                    anu.main(job_id)

                stdout_output = stdout_buffer.getvalue()
                stderr_output = stderr_buffer.getvalue()
                
                output_csv = os.path.join(resume_folder, "resume_analysis.csv")
                if not os.path.exists(output_csv):
                    return jsonify({
                        'success': False,
                        'error': f'No results found for Job ID "{job_id}"'
                    }), 404

                df = pd.read_csv(output_csv)
                if df.empty:
                    return jsonify({
                        'success': False,
                        'error': f'Results for Job ID "{job_id}" are empty'
                    }), 404

                # Extract job information
                job_role = "N/A"
                if 'Job Role' in df.columns:
                    job_roles = df['Job Role'].dropna().unique()
                    if len(job_roles) > 0:
                        job_role = job_roles[0]

                subject_skills = []
                if 'Subject Skills' in df.columns:
                    skills = df['Subject Skills'].dropna().unique()
                    if len(skills) > 0:
                        subject_skills = [s.strip() for s in skills[0].split(',') if s.strip()]

                # Prepare response data
                response_data = {
                    'success': True,
                    'job_id': job_id,
                    'job_role': job_role,
                    'subject_skills': subject_skills,
                    'candidates': df.to_dict(orient='records')
                }
                
                return jsonify(response_data)

            except Exception as e:
                return jsonify({
                    'success': False,
                    'error': f'Error processing job ID: {str(e)}',
                    'traceback': traceback.format_exc()
                }), 500
                
            finally:
                # Restore original resume folder
                anu.RESUME_FOLDER = original_resume_folder
                
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Unexpected error: {str(e)}',
            'traceback': traceback.format_exc()
        }), 500

if __name__ == '__main__':
    app.run(debug=True)
