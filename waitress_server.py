# Then, create a file named `waitress_server.py` in your Flask application directory:
from app import app
from waitress import serve

if __name__ == "__main__":
    serve(app, host='0.0.0.0', port=8000)