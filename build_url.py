import json
import re
from pathlib import Path

def load_json_from_js(file_path):
    """
    Load a .js/.json file that contains nothing but a JSON literal (array or object).
    Strips any JS-specific syntax (e.g. export default) before parsing.
    """
    text = Path(file_path).read_text(encoding="utf-8")
    text = re.sub(r'^\s*export\s+default\s+', '', text, flags=re.MULTILINE)
    text = re.sub(r';\s*$', '', text, flags=re.MULTILINE)
    return json.loads(text)

def build_wellfound_urls(job_file, state_file):
    jobs = load_json_from_js(job_file)
    states_data = load_json_from_js(state_file)

    # support a top-level { "countries": [...] } wrapper
    if isinstance(states_data, dict) and "countries" in states_data:
        states = states_data["countries"]
    else:
        states = states_data

    base = "https://wellfound.com/role/l"
    urls = []

    for job in jobs:
        role = job["role"]
        for loc in states:
            name = loc.get("name")
            if name:
                urls.append(f"{base}/{role}/{name}")

    return urls

if __name__ == "__main__":
    job_file   = "./config/job_Type.json"
    state_file = "./config/states.json"

    # 1) build the raw URLs
    urls = build_wellfound_urls(job_file, state_file)

    # 2) wrap each URL in an object with "value": false
    wrapped = [{"url": u, "value": False} for u in urls]

    # 3) write out to JSON
    output_path = Path("wellfound_urls.json")
    output_path.write_text(
        json.dumps(wrapped, indent=2),
        encoding="utf-8"
    )

    print(f"Saved {len(wrapped)} entries to {output_path.resolve()}")
