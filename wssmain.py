from pywss import Pyws,route


@route('/test/example/1')
def example_1(request, data):
    return data + ' - data from pywss'


if __name__ == '__main__':
    ws = Pyws(__name__, address='127.0.0.1', port=8866)
    ws.serve_forever()