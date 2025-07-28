import os

# Define the folder and file structure
structure = {
    "backend": {
        "src": [
            "__init__.py",
            "mqtt_to_postgres.py",
            "api.py",
            "utils.py"
        ],
        "certs": [
            "ca.crt",
            "client.crt",
            "client.key"
        ],
        "logs": [
            "mqtt_client.log",
            "timestamp_debug.log"
        ],
        "config": [
            "init_db.sql"
        ],
        "tests": [
            "test_mqtt.py",
            "test_db.py"
        ],
        ".": [
            "requirements.txt",
            ".env"
        ]
    },
    ".": [
        ".gitignore",
        "README.md"
    ]
}

def create_structure(base_path=".", structure_dict=structure):
    for folder, contents in structure_dict.items():
        folder_path = os.path.join(base_path, folder)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        for item in contents:
            item_path = os.path.join(folder_path, item)
            if '.' in item:  # file
                with open(item_path, 'w') as f:
                    pass  # create empty file
            else:  # subfolder, if needed
                os.makedirs(item_path, exist_ok=True)

if __name__ == "__main__":
    create_structure()
    print("Project structure created successfully.")
