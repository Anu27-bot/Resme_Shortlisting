from flask import Flask, render_template, request, jsonify, session, send_file
import os
import pandas as pd
import logging
import json
import traceback
import hashlib
from datetime import datetime
from werkzeug.utils import secure_filename
import tempfile
import shutil

# Import your existing modules
import anu  # Your main processing module

app = Flask(__name__)
app.secret_key = 'your-secret-key-here'  # Change this in production
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Configuration
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
RESUME_FOLDER = os.path.join(PROJECT_ROOT, "Resumes")
OUTPUT_CSV = os.path.join(RESUME_FOLDER, "resume_analysis.csv")
ALLOWED_EXTENSIONS = {'pdf', 'doc', 'docx'}

# Ensure directories exist
os.makedirs(RESUME_FOLDER, exist_ok=True)

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def get_recent_jobs():
    """Get recent job IDs from session"""
    return session.get('recent_jobs', [])

def add_to_recent_jobs(job_id):
    """Add a job ID to recent jobs in session"""
    recent_jobs = get_recent_jobs()
    
    # Remove if already exists
    if job_id in recent_jobs:
        recent_jobs.remove(job_id)
    
    # Add to beginning
    recent_jobs.insert(0, job_id)
    
    # Keep only the 10 most recent
    if len(recent_jobs) > 10:
        recent_jobs = recent_jobs[:10]
    
    session['recent_jobs'] = recent_jobs
    return recent_jobs

@app.route('/')
def index():
    """Main page"""
    return render_template('index.html', recent_jobs=get_recent_jobs())

@app.route('/process', methods=['POST'])
def process_job():
    """Process a job ID and analyze resumes"""
    try:
        job_id = request.form.get('job_id', '').strip()
        if not job_id:
            return jsonify({'error': 'Job ID is required'}), 400
        
        # Validate job ID format
        if not re.match(r'^[A-Za-z0-9-]+$', job_id) or not any(c.isalpha() for c in job_id) or not any(c.isdigit() for c in job_id):
            return jsonify({'error': 'Invalid Job ID format'}), 400
        
        # Add to recent jobs
        add_to_recent_jobs(job_id)
        
        # Process the job ID using your existing code
        anu.main(job_id)
        
        # Read and return results
        if not os.path.exists(OUTPUT_CSV):
            return jsonify({'error': 'No results generated'}), 500
            
        df = pd.read_csv(OUTPUT_CSV)
        
        # Extract job details
        job_role = "N/A"
        subject_skills = []
        
        if 'Job Role' in df.columns:
            job_roles = df['Job Role'].dropna().unique()
            if len(job_roles) > 0:
                job_role = job_roles[0]
        
        # Prepare response data
        response_data = {
            'job_id': job_id,
            'job_role': job_role,
            'candidate_count': len(df),
            'columns': df.columns.tolist(),
            'data': df.to_dict('records')
        }
        
        return jsonify(response_data)
        
    except Exception as e:
        logging.error(f"Error processing job: {str(e)}")
        logging.error(traceback.format_exc())
        return jsonify({'error': f'Processing failed: {str(e)}'}), 500

@app.route('/upload', methods=['POST'])
def upload_resumes():
    """Manual resume upload endpoint"""
    try:
        if 'resumes' not in request.files:
            return jsonify({'error': 'No files uploaded'}), 400
            
        files = request.files.getlist('resumes')
        if not files or files[0].filename == '':
            return jsonify({'error': 'No selected files'}), 400
        
        # Create a temporary folder for uploaded resumes
        temp_dir = tempfile.mkdtemp()
        saved_files = []
        
        for file in files:
            if file and allowed_file(file.filename):
                filename = secure_filename(file.filename)
                file_path = os.path.join(temp_dir, filename)
                file.save(file_path)
                saved_files.append(file_path)
        
        if not saved_files:
            shutil.rmtree(temp_dir)
            return jsonify({'error': 'No valid files uploaded'}), 400
        
        # Process the resumes (you'll need to adapt your processing code)
        # For now, we'll just return the file list
        result = {
            'message': f'Successfully uploaded {len(saved_files)} files',
            'files': [os.path.basename(f) for f in saved_files]
        }
        
        # Clean up
        shutil.rmtree(temp_dir)
        
        return jsonify(result)
        
    except Exception as e:
        logging.error(f"Error uploading files: {str(e)}")
        return jsonify({'error': f'Upload failed: {str(e)}'}), 500

@app.route('/download/<job_id>')
def download_results(job_id):
    """Download results as CSV"""
    try:
        csv_path = os.path.join(RESUME_FOLDER, f"resume_analysis_{job_id}.csv")
        
        # Use the main output CSV if job-specific doesn't exist
        if not os.path.exists(csv_path):
            csv_path = OUTPUT_CSV
            
        if not os.path.exists(csv_path):
            return jsonify({'error': 'Results not found'}), 404
            
        return send_file(
            csv_path,
            as_attachment=True,
            download_name=f"resume_analysis_{job_id}.csv",
            mimetype='text/csv'
        )
        
    except Exception as e:
        logging.error(f"Error downloading results: {str(e)}")
        return jsonify({'error': f'Download failed: {str(e)}'}), 500

@app.route('/api/jobs/recent')
def get_recent_jobs_api():
    """API endpoint to get recent job IDs"""
    return jsonify({'recent_jobs': get_recent_jobs()})

@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500

if __name__ == '__main__':
    # Create necessary directories
    os.makedirs(RESUME_FOLDER, exist_ok=True)
    
    # Run the app
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True)
