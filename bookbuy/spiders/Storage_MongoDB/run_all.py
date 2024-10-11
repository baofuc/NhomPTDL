import subprocess

subprocess.run(['python', 'pushdata.py'], check=True)

subprocess.run(['python', 'connect_db.py'], check=True)