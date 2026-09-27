import redis        # pip install redis
import os
from dotenv import load_dotenv   # pip install python-dotenv

load_dotenv()

ip       = os.getenv('REDIS_HOST')
port     = int(os.getenv('REDIS_PORT', 6379))
password = os.getenv('REDIS_PASSWORD')

r = redis.Redis(host=ip, port=port, db=0, password=password)

with open("ontarioTech.jpg", "rb") as f:
    value = f.read();
r.set('OntarioTech',value);
print('Image sent')