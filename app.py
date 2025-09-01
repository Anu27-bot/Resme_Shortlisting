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
import glob
from datetime import datetime

app = Flask(__name__)
app.secret_key = 'your-secret-key-here'

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

project_root = os.path.dirname(os.path.abspath(__file__))
os.chdir(project_root)

@app.context_processor
def inject_now():
    return {'now': datetime.now()}

def get_recent_job_ids():
    """Get recent job IDs from existing CSV files"""
    recent_jobs = []
    csv_files = glob.glob(os.path.join(project_root, "Resumes", "*.csv"))
    
    for csv_file in csv_files:
        try:
            df = pd.read_csv(csv_file)
            if 'Job Role' in df.columns and not df.empty:
                # Extract job ID from filename or data
                filename = os.path.basename(csv_file)
                if filename.startswith("resume_analysis_"):
                    job_id = filename.replace("resume_analysis_", "").replace(".csv", "")
                else:
                    # Try to get from data
                    job_ids = df.get('Job ID', pd.Series()).dropna().unique()
                    if len(job_ids) > 0:
                        job_id = job_ids[0]
                    else:
                        continue
                recent_jobs.append(job_id)
        except:
            continue
    
    # Also check for job folders
    resume_dir = os.path.join(project_root, "Resumes")
    if os.path.exists(resume_dir):
        for item in os.listdir(resume_dir):
            if item.startswith("Job_"):
                job_id = item.replace("Job_", "")
                if job_id not in recent_jobs:
                    recent_jobs.append(job_id)
    
    return recent_jobs[-5:]  # Return last 5 jobs

@app.route('/', methods=['GET'])
def index():
    recent_jobs = get_recent_job_ids()
    return render_template('index.html', recent_jobs=recent_jobs)

@app.route('/process', methods=['GET', 'POST'])
def process():
    recent_jobs = get_recent_job_ids()
    
    if request.method == 'POST':
        job_id = request.form.get('job_id').strip()
        if not job_id:
            flash('Job ID is required', 'error')
            return render_template('process.jinja', 
                                 job_id=None,
                                 recent_jobs=recent_jobs,
                                 subject_skills=[],
                                 job_role=None,
                                 table_data=[],
                                 columns=[])

        # Check if we have cached results for this job ID
        cached_csv = os.path.join(project_root, "Resumes", f"resume_analysis_{job_id}.csv")
        if os.path.exists(cached_csv):
            try:
                df = pd.read_csv(cached_csv)
                return render_processed_results(df, job_id, recent_jobs)
            except Exception as e:
                flash(f'Error reading cached results: {str(e)}', 'error')
                # Continue with processing

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
                        return render_template('process.jinja', 
                                             job_id=job_id,
                                             recent_jobs=recent_jobs,
                                             subject_skills=[],
                                             job_role=None,
                                             table_data=[],
                                             columns=[])

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
                    return render_template('process.jinja', 
                                         job_id=job_id,
                                         recent_jobs=recent_jobs,
                                         subject_skills=[],
                                         job_role=None,
                                         table_data=[],
                                         columns=[])

                try:
                    df = pd.read_csv(output_csv)
                    if df.empty:
                        flash(f'Results for Job ID "{job_id}" are empty. No candidates were found.', 'error')
                        logging.warning(f"Output CSV is empty: {output_csv}")
                        return render_template('process.jinja', 
                                             job_id=job_id,
                                             recent_jobs=recent_jobs,
                                             subject_skills=[],
                                             job_role=None,
                                             table_data=[],
                                             columns=[])
                    
                    # Save results to cache
                    os.makedirs(os.path.join(project_root, "Resumes"), exist_ok=True)
                    cache_path = os.path.join(project_root, "Resumes", f"resume_analysis_{job_id}.csv")
                    df.to_csv(cache_path, index=False)
                    
                except Exception as e:
                    flash(f'Failed to read results: {str(e)}', 'error')
                    logging.error(f"Error reading CSV: {e}")
                    return render_template('process.jinja', 
                                         job_id=job_id,
                                         recent_jobs=recent_jobs,
                                         subject_skills=[],
                                         job_role=None,
                                         table_data=[],
                                         columns=[])

                return render_processed_results(df, job_id, recent_jobs)

            finally:
                # Restore original resume folder
                anu.RESUME_FOLDER = original_resume_folder

    # Handle GET requests with job_id parameter
    job_id = request.args.get('job_id')
    if job_id:
        # Check if we have cached results for this job ID
        cached_csv = os.path.join(project_root, "Resumes", f"resume_analysis_{job_id}.csv")
        if os.path.exists(cached_csv):
            try:
                df = pd.read_csv(cached_csv)
                return render_processed_results(df, job_id, recent_jobs)
            except Exception as e:
                flash(f'Error reading cached results: {str(e)}', 'error')

    return render_template(
        'process.jinja',
        job_id=job_id,
        recent_jobs=recent_jobs,
        subject_skills=[],
        job_role=None,
        table_data=[],
        columns=[]
    )

def render_processed_results(df, job_id, recent_jobs):
    """Helper function to render processed results"""
    # Extract job role from the data - FIXED LOGIC
    job_role = "N/A"
    
    # First try to get job role from the Job Role column
    if 'Job Role' in df.columns:
        # Get the first non-NA, non-empty job role that's not generic
        non_empty_roles = df[
            df['Job Role'].notna() & 
            (df['Job Role'] != 'N/A') & 
            (df['Job Role'] != '') &
            (~df['Job Role'].str.contains('Role for', case=False))  # Exclude generic roles
        ]['Job Role']
        
        if len(non_empty_roles) > 0:
            job_role = non_empty_roles.iloc[0]
    
    # If still not found, try to extract from subject skills
    if job_role == "N/A" and 'Subject Skills' in df.columns:
        subject_skills = df['Subject Skills'].dropna().unique()
        if len(subject_skills) > 0 and subject_skills[0] != 'N/A':
            skills_str = str(subject_skills[0])
            # Extract job role from subject skills (part before 'with')
            if 'with' in skills_str.lower():
                job_role = skills_str.split('with')[0].strip()
                # Clean up any location prefixes
                job_role = re.sub(r'^\s*(Onsite/Local|Remote/Local|Hybrid/Local|Onsite|Remote|Hybrid)[/\s]*', '', job_role, flags=re.IGNORECASE).strip()
            else:
                job_role = skills_str.strip()

    # Extract subject skills from the data
    subject_skills = []
    if 'Subject Skills' in df.columns:
        skills = df['Subject Skills'].dropna().unique()
        if len(skills) > 0 and skills[0] != 'N/A':
            # Clean up the skills string - extract skills part after 'with'
            skills_str = str(skills[0])
            if 'with' in skills_str.lower():
                skills_part = skills_str.split('with')[1].strip()
                subject_skills = [s.strip() for s in skills_part.split(',') if s.strip()]
            else:
                subject_skills = [s.strip() for s in skills_str.split(',') if s.strip()]

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

    flash(f'Successfully processed Job ID: {job_id}', 'success')
    
    return render_template(
        'process.jinja',
        job_id=job_id,
        recent_jobs=recent_jobs,
        subject_skills=subject_skills if subject_skills else [],
        job_role=job_role,
        table_data=table_data,
        columns=columns_order
    )

@app.route('/api/process/<job_id>', methods=['GET'])
def api_process(job_id):
    """API endpoint for processing job IDs"""
    try:
        # Check if we have cached results for this job ID first
        cached_csv = os.path.join(project_root, "Resumes", f"resume_analysis_{job_id}.csv")
        if os.path.exists(cached_csv):
            try:
                df = pd.read_csv(cached_csv)
                return process_job_data(df, job_id)
            except Exception as e:
                return jsonify({
                    'success': False,
                    'error': f'Error reading cached results: {str(e)}'
                }), 500

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

                # Save results to cache
                os.makedirs(os.path.join(project_root, "Resumes"), exist_ok=True)
                cache_path = os.path.join(project_root, "Resumes", f"resume_analysis_{job_id}.csv")
                df.to_csv(cache_path, index=False)
                
                return process_job_data(df, job_id)

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

def process_job_data(df, job_id):
    """Process job data and return JSON response"""
    # Extract job information - FIXED LOGIC
    job_role = "N/A"
    if 'Job Role' in df.columns:
        # Get the first non-NA, non-empty job role that's not generic
        non_empty_roles = df[
            df['Job Role'].notna() & 
            (df['Job Role'] != 'N/A') & 
            (df['Job Role'] != '') &
            (~df['Job Role'].str.contains('Role for', case=False))
        ]['Job Role']
        
        if len(non_empty_roles) > 0:
            job_role = non_empty_roles.iloc[0]

    # If still not found, try to extract from subject skills
    if job_role == "N/A" and 'Subject Skills' in df.columns:
        subject_skills_list = df['Subject Skills'].dropna().unique()
        if len(subject_skills_list) > 0 and subject_skills_list[0] != 'N/A':
            skills_str = str(subject_skills_list[0])
            if 'with' in skills_str.lower():
                job_role = skills_str.split('with')[0].strip()
                job_role = re.sub(r'^\s*(Onsite/Local|Remote/Local|Hybrid/Local|Onsite|Remote|Hybrid)[/\s]*', '', job_role, flags=re.IGNORECASE).strip()
            else:
                job_role = skills_str.strip()

    subject_skills = []
    if 'Subject Skills' in df.columns:
        skills = df['Subject Skills'].dropna().unique()
        if len(skills) > 0 and skills[0] != 'N/A':
            skills_str = str(skills[0])
            if 'with' in skills_str.lower():
                skills_part = skills_str.split('with')[1].strip()
                subject_skills = [s.strip() for s in skills_part.split(',') if s.strip()]
            else:
                subject_skills = [s.strip() for s in skills_str.split(',') if s.strip()]

    # Prepare response data
    response_data = {
        'success': True,
        'job_id': job_id,
        'job_role': job_role,
        'subject_skills': subject_skills,
        'candidates': df.to_dict(orient='records')
    }
    
    return jsonify(response_data)

if __name__ == '__main__':
    app.run(debug=True, port=5001)
