templates = [
    "projects/just-creek-462112-j2/regions/asia-east1/instanceTemplates/autobahn-template-asia-east1"
] * 50
import json
print(json.dumps(templates, indent=4))