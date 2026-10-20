from aiohttp import web


async def handle(request):
    urls = dict(request.query).get("urls")
    urls = urls.split(",")
    data = {
        "urls": urls
    }
    
    return web.json_response(data)


if __name__ == '__main__':
    app = web.Application()
    app.add_routes([
        web.get('/', handle),
    ])
    web.run_app(app)

