from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from web_ui.app import app

with app.test_client() as client:
    response = client.get('/api/supabase-status')
    print(response.status_code)
    print(response.get_json())
