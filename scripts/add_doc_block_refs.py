import json
import os
from ruamel.yaml import YAML

def load_manifest(manifest_path):
    with open(manifest_path, 'r') as file:
        return json.load(file)

def find_docs_in_manifest(manifest):
    docs = []
    for node in manifest['docs'].values():
        docs.append(node['name'].split('.')[-1])
    return docs

def update_yaml_files(docs, models_dir):
    yaml = YAML()
    yaml.preserve_quotes = True
    yaml.indent(mapping=2, sequence=4, offset=2)

    for root, dirs, files in os.walk(models_dir):
        for file in files:
            if file.endswith('.yml'):
                file_path = os.path.join(root, file)
                with open(file_path, 'r') as f:
                    data = yaml.load(f)

                if data and 'models' in data:
                    for model in data['models']:
                        if 'columns' in model:
                            for column in model['columns']:
                                if column['name'] in docs and ('description' not in column or not column['description']):
                                    print("Updating " + file_path + " with doc block reference for " + column['name'])
                                    column['description'] = "{{ doc('" + column['name'] + "') }}"

                with open(file_path, 'w') as f:
                    yaml.dump(data, f)

def main():
    manifest_path = 'bc/target/manifest.json'
    models_dir = 'bc/models'

    manifest = load_manifest(manifest_path)
    docs = find_docs_in_manifest(manifest)
    update_yaml_files(docs, models_dir)

if __name__ == '__main__':
    main()
