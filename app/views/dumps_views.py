from flask import flash, redirect, url_for
import subprocess
import os
import io
from app import app
from flask import render_template, request, send_file
from flask_login import login_required
from passwords import neon_password, local_password  # Importing sensitive info
from app.modules.decorators import local_only

# Variables for your databases
neon_host = "ep-blue-breeze-061984-pooler.eu-central-1.aws.neon.tech"
neon_user = "KonstantinPR"
neon_dbname = "neondb"
local_host = "localhost"
local_user = "postgres"
local_dbname = "postgres"


def create_db_dump(host, user, dbname, password):
    output_file = "neondb.dump"
    pg_dump_path = r"C:\Program Files\PostgreSQL\14\bin\pg_dump.exe"  # Full path to pg_dump

    command = [
        pg_dump_path,
        "-h", host,
        "-U", user,
        "-d", dbname,
        "-F", "c",
        "-b",
        "-v",
        "-f", output_file
    ]

    try:
        os.environ['PGPASSWORD'] = password
        subprocess.run(command, check=True)
        print(f"Database dump created successfully: {output_file}")
        return output_file
    except subprocess.CalledProcessError as e:
        print(f"Error occurred while creating dump: {e}")
        return None
    finally:
        if 'PGPASSWORD' in os.environ:
            del os.environ['PGPASSWORD']


def restore_db_dump_from_memory(dump_content, host, user, dbname, password):
    pg_restore_path = r"C:\Program Files\PostgreSQL\14\bin\pg_restore.exe"  # Full path to pg_restore

    command = [
        pg_restore_path,
        "-h", host,
        "-U", user,
        "-d", dbname,
        "--no-password",  # Do not prompt for password
        "--exit-on-error",  # Exit if an error occurs during restore
        "--single-transaction",  # Execute the restore as a single transaction
        "--no-owner",  # Skip restoration of object ownership
        "--no-privileges",  # Skip restoration of access privileges
        "--verbose",  # Print verbose output
        "--no-acl",  # Skip restoration of access control lists (ACLs)
        "--format=custom",  # Specify the format of the input file
        "--compress=9",  # Use the highest compression level
        "--jobs=4",  # Use 4 parallel jobs for restore
        # "--data-only",  # Restore only data (no schema)
    ]

    try:
        os.environ['PGPASSWORD'] = password
        subprocess.run(command, input=dump_content, check=True)
        print("Database dump imported successfully.")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error occurred while importing dump: {e}")
        return False
    finally:
        if 'PGPASSWORD' in os.environ:
            del os.environ['PGPASSWORD']


@app.route('/import_neon_postgres_dump', methods=['POST', 'GET'])
@login_required
def import_neon_postgres_dump():
    """
    To import dumps db
    """
    if request.method == 'GET':
        return render_template('upload_dumps.html', doc_string=get_neon_postgres_dump.__doc__)

    dump_file = create_db_dump(neon_host, neon_user, neon_dbname, neon_password)
    if dump_file:
        with open(dump_file, 'rb') as f:
            dump_content = f.read()
        os.remove(dump_file)  # Clean up the dump file after reading it

        success = restore_db_dump_from_memory(dump_content, local_host, local_user, local_dbname, local_password)

        if success:
            flash('Database dump imported successfully.', 'success')
        else:
            flash('Failed to import database dump.', 'danger')

        return redirect(url_for('get_neon_postgres_dump'))
    else:
        flash('Failed to create database dump', 'danger')
        return render_template('upload_dumps.html', doc_string=get_neon_postgres_dump.__doc__)


@app.route('/get_neon_postgres_dump', methods=['POST', 'GET'])
@login_required
@local_only
def get_neon_postgres_dump():
    """
    To get dumps db
    """
    if request.method == 'GET':
        return render_template('upload_dumps.html', doc_string=get_neon_postgres_dump.__doc__)

    dump_file = create_db_dump(neon_host, neon_user, neon_dbname, neon_password)
    if dump_file:
        with open(dump_file, 'rb') as f:
            dump = io.BytesIO(f.read())
        dump.seek(0)
        os.remove(dump_file)  # Clean up the dump file after reading it
        return send_file(dump, download_name=dump_file, as_attachment=True)
    else:
        flash('Failed to create database dump', 'danger')
        return render_template('upload_dumps.html', doc_string=get_neon_postgres_dump.__doc__)
