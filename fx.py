from flask import Flask, request, jsonify
from lxml import html
from flask_sqlalchemy import SQLAlchemy
import time
from tornado import httpclient, gen, ioloop, queues
from tornado.httpclient import AsyncHTTPClient
from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://root:default00@127.0.0.1/fx'
db = SQLAlchemy(app)


class Currency(db.Model):
    code = db.Column(db.String(3), primary_key=True)
    name = db.Column(db.String(75))

    def __init__(self, code, name):
        self.code = code
        self.name = name

    def __repr__(self):
        return '<Currency %r>' % self.code


class ExchangeRate(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    base = db.Column(db.String(3))
    quote = db.Column(db.String(3))
    timestamp = db.Column(db.Integer)
    rate = db.Column(db.Float)

    def __init__(self, base, quote, timestamp, rate):
        self.base = base
        self.quote = quote
        self.timestamp = timestamp
        self.rate = rate

    def __repr__(self):
        return '<ExchangeRate %r>' % self.timestamp

    @property
    def serialize(self):
        """Return object data in easily serializeable format"""
        return {
            'base': self.base,
            'quote': self.quote,
            'timestamp': self.timestamp,
            'rate': self.rate
        }


concurrency = 50
user_agent = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.82 Safari/537.36"


@app.route('/')
def response():
    currency = request.args.get('currency')
    since = int(request.args.get('since'))
    till = int(request.args.get('till'))
    rates = ExchangeRate.query.filter(ExchangeRate.base == currency) \
        .filter(since <= ExchangeRate.timestamp) \
        .filter(ExchangeRate.timestamp <= till).all()
    return jsonify(json_list=[i.serialize for i in rates])


@gen.coroutine
def get_current_exchange_rate(url):
    try:
        response = yield httpclient.AsyncHTTPClient().fetch(url, user_agent=user_agent)
        print('fetched %s' % url)

        result_html = response.body if isinstance(response.body, str) \
            else response.body.decode()
        tree = html.fromstring(result_html)
        result = tree.xpath('//*[@id="last_last"]')
        if result:
            current_exchange = ExchangeRate(url.split('/')[-1].split('-')[0],
                                            url.split('/')[-1].split('-')[1],
                                            int(time.time()),
                                            result[0].text.replace(',', ''))
            db.session.add(current_exchange)
            db.session.commit()
        else:
            print('not found ' + url + ' ' + str(response.status_code))
    except Exception as e:
        print('Exception: %s %s' % (e, url))
        raise gen.Return([])

    raise gen.Return(result)


@gen.coroutine
def fetch_all():
    q = queues.Queue()
    start = time.time()
    fetching, fetched = set(), set()

    @gen.coroutine
    def fetch_url():
        current_url = yield q.get()
        try:
            if current_url in fetching:
                return

            print('fetching %s' % current_url)
            fetching.add(current_url)
            yield get_current_exchange_rate(current_url)
            fetched.add(current_url)

        finally:
            q.task_done()

    @gen.coroutine
    def worker():
        while True:
            yield fetch_url()

    currencies = Currency.query.all()
    for base in currencies:
        base_code = base.code
        for currency in currencies:
            if currency.code != base_code:
                q.put('https://www.investing.com/currencies/' + base_code + '-' + currency.code)

    # Start workers, then wait for the work queue to be empty.
    for _ in range(concurrency):
        worker()
    try:
        yield q.join(timeout=None)
        assert fetching == fetched
        print('Done in %d seconds, fetched %s URLs.' % (
            time.time() - start, len(fetched)))
    except gen.TimeoutError:
        pass


if __name__ == '__main__':
    http_server = HTTPServer(WSGIContainer(app))
    http_server.listen(5000)
    io_loop = ioloop.IOLoop.current()
    import logging
    import tornado.options
    tornado.options.parse_command_line()
    AsyncHTTPClient.configure(None, max_clients=500)
    logging.basicConfig()
    ioloop.PeriodicCallback(fetch_all, 3600000).start()
    io_loop.instance().start()
