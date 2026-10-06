#!/usr/bin/env python3
"""Check the source IoT demo in Chromium against a running component broker."""
import argparse
from pathlib import Path

from playwright.sync_api import sync_playwright, expect

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--mqtt-url', default='ws://127.0.0.1:8083/mqtt')
parser.add_argument('--chromium', help='Chromium executable path')
args = parser.parse_args()
html = (Path(__file__).resolve().parents[1] / 'priv/index.html').read_text()

# Verify occupancy control, late registration, and dependent-first cleanup over MQTT.
with sync_playwright() as playwright:
    browser = playwright.chromium.launch(**({'executable_path': args.chromium} if args.chromium else {}))
    page = browser.new_page(viewport={'width': 1400, 'height': 1100}, permissions=['local-network-access'])
    errors = []
    page.on('pageerror', lambda error: errors.append(str(error)))
    page.route('http://127.0.0.1:18085/**', lambda route: route.fulfill(content_type='text/html', body=html))
    page.goto('http://127.0.0.1:18085/ui', wait_until='networkidle')

    def action(name, command):
        page.locator(f'#node-{name} [data-action="{command}"]').click()

    def status(name, state):
        expect(page.locator('#node-' + name)).to_have_attribute('data-state', state, timeout=15000)

    def light(name, on):
        expect(page.locator(f'#node-{name} .lamp-light')).to_have_attribute('data-on', str(on).lower())

    def registered(count):
        page.wait_for_function('count => actors.get("switch").lamps.size === count', arg=count)
        expect(page.locator('#node-switch #lamp-registrations li[data-effect-id]')).to_have_count(count)

    try:
        # Select the IoT tab and connect lamps before their dependencies exist.
        page.locator('#tab-iot').click()
        expect(page.locator('#route-request')).to_be_hidden()
        expect(page.locator('#node-sensor #occupied')).to_be_visible()
        assert page.evaluate("[...document.querySelectorAll('.dependency')].filter(p => p.style.display !== 'none').length") == 4
        page.evaluate("$('delay').value = 0; $('duration').value = 0")
        page.locator('#broker-url').fill(args.mqtt_url)
        page.locator('#connect').click()
        expect(page.locator('#status')).to_have_text('Connected', timeout=15000)
        page.evaluate('''() => {
            window.iotTrace = [];
            observer.on('message', (topic, payload) => {
                if (topic === '$component/debug') iotTrace.push(JSON.parse(payload.toString()));
            });
        }''')
        for name in ['lamp1', 'lamp2', 'switch']:
            action(name, 'connect')
            status(name, 'inactive')
        light('lamp1', False)
        action('sensor', 'connect')
        for name in ['sensor', 'switch', 'lamp1', 'lamp2']:
            status(name, 'active')
        registered(2)
        light('lamp1', False)
        light('lamp2', False)

        # Occupancy reaches existing lamps and initializes a late lamp to on.
        page.locator('#occupied').check()
        light('lamp1', True)
        light('lamp2', True)
        action('lamp3', 'connect')
        status('lamp3', 'active')
        registered(3)
        light('lamp3', True)
        page.locator('#occupied').uncheck()
        for name in ['lamp1', 'lamp2', 'lamp3']:
            light(name, False)
        page.locator('#occupied').check()
        for name in ['lamp1', 'lamp2', 'lamp3']:
            light(name, True)

        # Per-lamp overrides remain exclusive and take precedence over occupancy changes.
        lamp1_controls = page.locator('#lamp-registrations [data-lamp="lamp1"]')
        lamp2_controls = page.locator('#lamp-registrations [data-lamp="lamp2"]')
        lamp1_controls.get_by_role('button', name='Off', exact=True).click()
        light('lamp1', False)
        expect(lamp1_controls.locator('[aria-pressed="true"]')).to_have_text('Off')
        lamp2_controls.get_by_role('button', name='On', exact=True).click()
        page.locator('#occupied').uncheck()
        light('lamp1', False)
        light('lamp2', True)
        light('lamp3', False)
        expect(lamp2_controls.locator('[aria-pressed="true"]')).to_have_text('On')
        page.locator('#occupied').check()
        light('lamp3', True)
        light('lamp1', False)
        lamp1_controls.get_by_role('button', name='On', exact=True).click()
        light('lamp1', True)
        expect(lamp1_controls.locator('[aria-pressed="true"]')).to_have_text('On')
        page.locator('#occupied').uncheck()
        light('lamp3', False)
        light('lamp1', True)
        lamp1_controls.get_by_role('button', name='Auto', exact=True).click()
        light('lamp1', False)
        expect(lamp1_controls.locator('[aria-pressed="true"]')).to_have_text('Auto')
        page.locator('#occupied').check()
        light('lamp1', True)

        # Deactivation turns off an overridden lamp and reinitialization resets its override.
        action('lamp2', 'disable')
        status('lamp2', 'inactive')
        light('lamp2', False)
        registered(2)
        light('lamp1', True)
        action('lamp2', 'enable')
        status('lamp2', 'active')
        registered(3)
        light('lamp2', True)
        expect(lamp2_controls.locator('[aria-pressed="true"]')).to_have_text('Auto')

        # Sensor withdrawal drains lamp registrations before switch teardown.
        page.evaluate('iotTrace.length = 0')
        action('sensor', 'disable')
        for name in ['lamp1', 'lamp2', 'lamp3', 'switch', 'sensor']:
            status(name, 'inactive')
        registered(0)
        for name in ['lamp1', 'lamp2', 'lamp3']:
            light(name, False)
        stopped = page.evaluate('''iotTrace.filter(e => e.event === 'component_changed' &&
            e.previous?.state === 'stopping' && e.current?.state === 'inactive')
            .map(e => clientName(e.current.clientid))''')
        assert all(stopped.index(name) < stopped.index('switch') for name in ['lamp1', 'lamp2', 'lamp3']), stopped
        assert stopped.index('switch') < stopped.index('sensor'), stopped
        action('sensor', 'enable')
        for name in ['sensor', 'switch', 'lamp1', 'lamp2', 'lamp3']:
            status(name, 'active')
        registered(3)
        for name in ['lamp1', 'lamp2', 'lamp3']:
            light(name, True)

        # Switch reconnection reads retained occupancy without a new sensor write.
        action('switch', 'disconnect')
        status('switch', 'disconnected')
        for name in ['lamp1', 'lamp2', 'lamp3']:
            status(name, 'inactive')
            light(name, False)
        action('switch', 'connect')
        status('switch', 'active')
        for name in ['lamp1', 'lamp2', 'lamp3']:
            status(name, 'active')
            light(name, True)
        registered(3)

        # Physical sensor loss also stops lamps and permits fresh registrations on return.
        action('sensor', 'disconnect')
        status('sensor', 'disconnected')
        for name in ['switch', 'lamp1', 'lamp2', 'lamp3']:
            status(name, 'inactive')
        registered(0)
        for name in ['lamp1', 'lamp2', 'lamp3']:
            light(name, False)
        action('sensor', 'connect')
        for name in ['sensor', 'switch', 'lamp1', 'lamp2', 'lamp3']:
            status(name, 'active')
        registered(3)

        # Held initialization keeps a lamp dark until ready, and abort removes its registration.
        action('lamp3', 'disconnect')
        status('lamp3', 'disconnected')
        registered(2)
        page.locator('#hold').check()
        action('lamp3', 'connect')
        status('lamp3', 'starting')
        page.wait_for_function("actors.get('lamp3').initialized")
        registered(3)
        light('lamp3', False)
        action('lamp3', 'abort')
        status('lamp3', 'inactive')
        registered(2)
        page.locator('#hold').uncheck()
        action('lamp3', 'enable')
        status('lamp3', 'active')
        light('lamp3', True)

        # Switching tabs preserves clients and displays only the selected dependency graph.
        page.locator('#tab-web').click()
        expect(page.locator('#node-lamp1')).to_be_hidden()
        expect(page.locator('#route-request')).to_be_visible()
        assert page.evaluate("[...document.querySelectorAll('.dependency')].filter(p => p.style.display !== 'none').length") == 5
        page.locator('#tab-iot').click()
        light('lamp1', True)
        assert page.evaluate('''[...document.querySelectorAll('.node')].filter(n => !n.hidden)
            .every(n => n.offsetTop + n.offsetHeight <= $('scene').offsetHeight)''')
        page.screenshot(path='/tmp/opencode/mqtt-components-iot.png', full_page=True)
        page.set_viewport_size({'width': 390, 'height': 844})
        assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
        assert not errors, errors
        expect(page.locator('#detail')).to_contain_text('Observer ready.')
    except Exception:
        print(page.locator('#detail').inner_text())
        print(page.evaluate('Object.fromEntries([...actors].map(([name, actor]) => [name, actor.status]))'))
        raise
    finally:
        page.evaluate('disconnectAll()')
        browser.close()
print('IoT demo passed: occupancy, late lamps, retained replay, cleanup, reconnect, and abort.')
