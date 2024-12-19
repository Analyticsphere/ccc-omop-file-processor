from flask import Flask, jsonify, request
from datetime import datetime
import core.utils as utils
import os

app = Flask(__name__)

@app.route('/heartbeat', methods=['GET'])
def heartbeat():
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat(),
        'service': 'ccc-ehr-pipeline'
    }), 200

@app.route('/get_files', methods=['GET'])
def get_files():
    # Get parameters from query string with defaults
    bucket = request.args.get('bucket', 'synthea_datasets')
    folder = request.args.get('folder', 'synthea_100_raw')   # Default folder if not provided
    
    file_list = utils.list_gcs_files(bucket, folder)

    return jsonify({
        'status': 'healthy',
        'file_list': file_list,
        'service': 'ehr-pipeline'
    }), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)