import json, os
from threading import Thread

from tornado.ioloop import IOLoop
import tornado.web

from hbi.model import Host, Filter
from hbi.server import Service


class RootHandler(tornado.web.RequestHandler):

    def get(self):
        self.write("boop")


class EntitiesPoster(tornado.web.RequestHandler):

    def post(self):
        hosts_json = json.loads(self.request.body)
        hosts = (Host.from_json(h) for h in hosts_json)
        ret = self.application.service.create_or_update(hosts)
        self.write(json.dumps([h.to_json() for h in ret]))


class EntitiesSearcher(tornado.web.RequestHandler):

    def post(self):
        filters_json = json.loads(self.request.body) if self.request.body else None
        filters = [Filter.from_json(h) for h in filters_json] if filters_json else None
        ret = self.application.service.get(filters)
        self.write(json.dumps([h.to_json() for h in ret]))


def serve_tornado():
    app = tornado.web.Application([
        (r"/", RootHandler),
        (r"/entities/search", EntitiesSearcher),
        (r"/entities", EntitiesPoster),
    ])
    app.listen(int(os.environ.get("PORT", "50051")))
    app.service = Service()
    loop = IOLoop.current()

    class TornadoRunThread(Thread):
        def run(self):
            loop.start()

    TornadoRunThread().start()
    return app, loop


if __name__ == "__main__":
    app, loop = serve_tornado()
