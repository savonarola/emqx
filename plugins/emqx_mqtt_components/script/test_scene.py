#!/usr/bin/env python3
"""Run the browser scene against a broker with the plugin installed."""
import argparse
from urllib.parse import urlsplit

from playwright.sync_api import sync_playwright, expect

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--url', default='http://127.0.0.1:18083/api/v5/plugin_api/emqx_mqtt_components/ui')
parser.add_argument('--authorization', help='HTTP Authorization header for the plugin page')
parser.add_argument('--chromium', help='Chromium executable path')
parser.add_argument('--mqtt-url', help='MQTT WebSocket URL, if using a nondefault listener')
args = parser.parse_args()

with sync_playwright() as playwright:
    browser = playwright.chromium.launch(**({'executable_path': args.chromium} if args.chromium else {}))
    page = browser.new_page(viewport={'width': 1400, 'height': 1000})
    errors = []
    page.on('pageerror', lambda error: errors.append(str(error)))
    if args.authorization:
        origin = urlsplit(args.url)
        page.route(f'{origin.scheme}://{origin.netloc}/**', lambda route: route.continue_(
            headers={**route.request.headers, 'authorization': args.authorization}))
    page.goto(args.url, wait_until='networkidle')
    broker = urlsplit(args.url)
    secure = broker.scheme == 'https'
    expect(page.locator('#broker-url')).to_have_value(
        'wss://box2:8084/mqtt' if secure else 'ws://box2:8083/mqtt')
    page.locator('#broker-url').fill(
        args.mqtt_url or f'{"wss" if secure else "ws"}://{broker.hostname}:{8084 if secure else 8083}/mqtt')

    def drain():
        page.wait_for_function('!playing && queue.length === 0', timeout=30000)

    def status(name, state):
        expect(page.locator('#node-' + name)).to_have_attribute('data-state', state, timeout=15000)

    def action(name, command):
        page.locator(f'#node-{name} [data-action="{command}"]').click()

    def routes(expected):
        drain()
        expect(page.locator('#routes li[data-effect-id]')).to_have_count(len(expected))
        actual = page.evaluate('[...installed.values()].map(r => r.path_prefix).sort()')
        assert actual == sorted(expected), actual

    def all_active():
        for name in ['cache', 'router', 'workerA', 'workerB', 'workerC', 'db']:
            status(name, 'active')
        routes(['/a', '/b', '/c'])

    try:
        page.evaluate("for (const id of ['delay', 'duration']) { const input = document.getElementById(id); input.value = 0; input.dispatchEvent(new Event('input')); }")
        page.locator('#connect').click()
        expect(page.locator('#status')).to_have_text('Connected', timeout=15000)
        page.evaluate('''() => {
            window.sceneTrace = [];
            observer.on('message', (topic, payload) => {
                if (topic === '$component/debug') sceneTrace.push(JSON.parse(payload.toString()));
            });
        }''')
        page.locator('#start').click()
        all_active()
        assert page.evaluate('[...actors.values()].every(a => a.client.connected && a.client.options.protocolVersion === 5)')
        assert page.locator('.dependency').count() == 5
        declarations = page.evaluate('''Object.fromEntries([...components.values()].map(c =>
            [clientName(c.clientid), {provides:c.provides, consumes:c.consumes}]))''')
        prefix = page.evaluate("'$service/' + session + '/'")
        assert declarations['db']['provides'] == [prefix + 'db']
        assert declarations['cache']['provides'] == [prefix + 'cache']
        assert declarations['router']['consumes'] == [prefix + 'cache']
        assert prefix + 'db' in declarations['workerA']['consumes']
        assert page.evaluate("[...document.querySelectorAll('.node')].every(n => n.offsetTop + n.offsetHeight <= document.getElementById('scene').offsetHeight)")
        for path, worker in [('/a/item', 'workerA'), ('/b', 'workerB'), ('/c/item', 'workerC')]:
            page.locator('#path').fill(path)
            page.locator('#send-request').click()
            expect(page.locator('#request-result')).to_contain_text('"handler": "' + worker + '"')
        page.locator('#path').fill('/missing')
        page.locator('#send-request').click()
        expect(page.locator('#request-result')).to_contain_text('404')

        # Each rendered debug update must wait for the preceding update.
        page.evaluate("""() => {
            window.playbackTimes = [];
            window.trace = [];
            document.getElementById('delay').value = 25;
            document.getElementById('duration').value = 50;
            new MutationObserver(() => playbackTimes.push(performance.now()))
                .observe(document.getElementById('last-sequence'), {childList: true});
            observer.on('message', (topic, payload) => {
                if (topic === '$component/debug') trace.push(JSON.parse(payload.toString()));
            });
        }""")
        action('db', 'disable')
        status('db', 'inactive')
        status('workerA', 'inactive')
        status('router', 'active')
        routes(['/b', '/c'])
        times = page.evaluate('playbackTimes')
        assert len(times) > 5, times
        assert all(b - a >= 20 for a, b in zip(times, times[1:])), times
        page.evaluate("for (const id of ['delay', 'duration']) { const input = document.getElementById(id); input.value = 0; input.dispatchEvent(new Event('input')); }")
        action('db', 'enable')
        all_active()

        action('router', 'disconnect')
        status('router', 'disconnected')
        for name in ['workerA', 'workerB', 'workerC']:
            status(name, 'inactive')
        routes([])
        action('router', 'connect')
        all_active()

        page.evaluate('trace.length = 0')
        action('cache', 'disable')
        for name in ['workerA', 'workerB', 'workerC', 'router', 'cache']:
            status(name, 'inactive')
        routes([])
        stopped = page.evaluate("""trace.filter(e => e.event === 'component_changed' &&
            e.previous?.state === 'stopping' && e.current?.state === 'inactive')
            .map(e => clientName(e.current.clientid))""")
        assert all(stopped.index(name) < stopped.index('router') for name in ['workerA', 'workerB', 'workerC']), stopped
        assert stopped.index('router') < stopped.index('cache'), stopped
        action('cache', 'enable')
        all_active()

        action('workerB', 'disable')
        status('workerB', 'inactive')
        routes(['/a', '/c'])
        page.locator('#hold').check()
        action('workerB', 'enable')
        status('workerB', 'starting')
        page.wait_for_function("actors.get('workerB').initialized")
        routes(['/a', '/b', '/c'])
        action('workerB', 'abort')
        status('workerB', 'inactive')
        expect(page.locator('#node-workerB')).to_have_attribute('data-block', 'aborted')
        routes(['/a', '/c'])
        action('workerB', 'enable')
        status('workerB', 'starting')
        page.wait_for_function("actors.get('workerB').initialized")
        action('workerB', 'ready')
        all_active()
        page.locator('#hold').uncheck()

        action('workerC', 'disconnect')
        status('workerC', 'disconnected')
        routes(['/a', '/b'])
        action('workerC', 'connect')
        all_active()
        page.screenshot(path='/tmp/mqtt-components-scene.png', full_page=True)
        page.set_viewport_size({'width': 390, 'height': 844})
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
        assert page.evaluate('''!sceneTrace.some(e =>
            e.topic?.startsWith('$state/') ||
            [...(e.current?.provides || []), ...(e.current?.consumes || [])]
                .some(t => t.startsWith('$state/')) ||
            e.current?.type === 'state_write' || e.current?.type === 'state_subscription')''')
        responses = page.evaluate('''sceneTrace.filter(e => e.event === 'message')
            .map(e => ({status: e.properties?.['User-Property']
                ?.find(p => p.key === 'component-status')?.value, payload: e.payload}))
            .filter(e => e.status)''')
        assert {'ok', 'activated', 'stopped', 'aborted', 'enabled', 'disabled',
                'retracted', '200', '404'} <= {e['status'] for e in responses}
        assert all(e['payload'] == '' for e in responses if not e['status'].isdigit())
        assert any('"handler"' in e['payload'] for e in responses if e['status'] == '200')
        assert not errors, errors
    finally:
        page.evaluate('disconnectAll()')
        browser.close()
print('Browser scene passed: routes, requests, dependency cleanup, reconnect, L-Iter abort, and ordered playback.')
