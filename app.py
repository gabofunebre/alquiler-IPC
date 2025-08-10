import os
from flask import Flask
from dotenv import load_dotenv

from routes import bp

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "changeme")
app.register_blueprint(bp)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
