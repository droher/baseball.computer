import os
import csv
from ruamel.yaml import YAML

def read_metadata(metadata_file):
    metadata = {}
    with open(metadata_file, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            relation = row['relation']
            if relation not in metadata:
                metadata[relation] = []
            metadata[relation].append({
                'name': row['column'],
                'description': row['description'],
                'data_type': row['type']
            })
    return metadata

def generate_yaml_for_seed(seed_file, columns):
    seed_name = os.path.splitext(os.path.basename(seed_file))[0]
    yaml_data = {
        'version': 2,
        'seeds': [{
            'name': seed_name,
            'config': {
                'contracts': {
                    'enforced': True
                }
            },
            'columns': columns
        }]
    }

    yaml = YAML()
    yaml.indent(mapping=2, sequence=4, offset=2)

    yaml_file_name = os.path.splitext(seed_file)[0] + '.yml'
    if not os.path.exists(yaml_file_name):
        with open(yaml_file_name, 'w') as f:
            yaml.dump(yaml_data, f)
        print(f'Generated YAML for seed: {yaml_file_name}')
    else:
        print(f'YAML for seed already exists: {yaml_file_name}')

def main():
    metadata_file = 'bc/metadata.csv'
    seeds_dir = 'bc/seeds'

    metadata = read_metadata(metadata_file)

    for root, dirs, files in os.walk(seeds_dir):
            for file in files:
                if file.endswith('.csv'):  # Assuming seed files are in CSV format
                    seed_file_path = os.path.join(root, file)
                    seed_name = os.path.splitext(file)[0]
                    if seed_name in metadata:
                        generate_yaml_for_seed(seed_file_path, metadata[seed_name])


if __name__ == '__main__':
    main()