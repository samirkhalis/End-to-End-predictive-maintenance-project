import os
import csv
import random
from flask import Flask, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

class DataServer:
    def __init__(self, data_directory):
        """
        Initialize the DataServer with a directory containing CSV data files
        
        :param data_directory: Path to directory containing CSV data files
        """
        self.data_directory = data_directory
        self.all_records = self.load_all_records()
    
    def load_all_records(self):
        """
        Load all records from CSV files in the specified directory
        
        :return: List of all records from all files
        """
        all_records = []
        
        # Check if directory exists
        if not os.path.exists(self.data_directory):
            raise ValueError(f"Directory {self.data_directory} does not exist")
        
        # Iterate through all files in the directory
        for filename in os.listdir(self.data_directory):
            if filename.endswith('.csv'):
                file_path = os.path.join(self.data_directory, filename)
                try:
                    with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
                        # Use DictReader to automatically use first row as headers
                        csv_reader = csv.DictReader(csvfile)
                        
                        # Convert each row to a dictionary and add to all_records
                        for row in csv_reader:
                            # Convert numeric strings to appropriate types
                            processed_row = {}
                            for key, value in row.items():
                                # Try to convert to int or float if possible
                                try:
                                    processed_row[key] = int(value)
                                except ValueError:
                                    try:
                                        processed_row[key] = float(value)
                                    except ValueError:
                                        processed_row[key] = value
                            
                            all_records.append(processed_row)
                
                except Exception as e:
                    print(f"Error reading {filename}: {e}")
        
        return all_records
    
    def get_random_record(self):
        """
        Get a random record from all loaded records
        
        :return: A random record or None if no records exist
        """
        if not self.all_records:
            return None
        return random.choice(self.all_records)

# Initialize DataServer (replace 'data' with your actual data directory)
data_server = DataServer('data')

@app.route('/record', methods=['GET'])
def get_record():
    """
    API endpoint to retrieve a random record
    """
    record = data_server.get_random_record()
    
    if record is None:
        return jsonify({"error": "No records found"}), 404
    
    return jsonify(record)

@app.route('/record/count', methods=['GET'])
def get_record_count():
    """
    API endpoint to get total number of records
    """
    return jsonify({"total_records": len(data_server.all_records)})

@app.route('/record/headers', methods=['GET'])
def get_record_headers():
    """
    API endpoint to get column headers from the first record
    """
    if not data_server.all_records:
        return jsonify({"error": "No records found"}), 404
    
    return jsonify({"headers": list(data_server.all_records[0].keys())})

if __name__ == '__main__':
    app.run(debug=True, port=5000)