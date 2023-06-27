import time

from flask import Flask, request

app = Flask(__name__)


@app.route('/', methods=['POST'])
def receive_data():
    start_time = time.time()
    print(start_time, flush=True)
    data = request.get_data()
    while time.time() < start_time + 0.15:
        pass
    data_with_start_time = {
        'FHR1_data': data.decode('utf-8'),
        'FHR1_Flask_API_Recieve_time': start_time

    }
    print(data_with_start_time, flush=True)
    return data_with_start_time


if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True, port='5001')
