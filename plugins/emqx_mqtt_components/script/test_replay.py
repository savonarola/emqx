#!/usr/bin/env python3
"""Check replay pause and resume in Chromium without a broker."""
import argparse
import json
from pathlib import Path

from playwright.sync_api import sync_playwright, expect

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--chromium', help='Chromium executable path')
args = parser.parse_args()
html = (Path(__file__).resolve().parents[1] / 'priv/index.html').read_text()

with sync_playwright() as playwright:
    browser = playwright.chromium.launch(**({'executable_path': args.chromium} if args.chromium else {}))
    page = browser.new_page(viewport={'width': 1400, 'height': 1100})
    errors = []
    page.on('pageerror', lambda error: errors.append(str(error)))
    page.route('https://unpkg.com/**', lambda route: route.abort())
    page.route('http://scene.test/**', lambda route: route.fulfill(content_type='text/html', body=html))
    page.goto('http://scene.test/ui')
    page.evaluate("""() => {
        pidNames.set('worker', 'workerA');
        $('duration').value = 800;
        $('delay').value = 600;
        window.pushEvent = sequence => observe('$component/debug', JSON.stringify({
            event: 'message', component_id: 'worker', direction: 'received',
            topic: '$component/ready', payload: '', sequence
        }));
    }""")
    pause = page.locator('#pause-replay')
    packet = page.locator('#packet')
    sequence = page.locator('#last-sequence')

    def assert_arrived_at(name):
        assert packet.evaluate('e => [parseFloat(e.style.left), parseFloat(e.style.top)]') == page.locator(
            '#node-' + name).evaluate('e => [e.offsetLeft + e.offsetWidth / 2, e.offsetTop + e.offsetHeight / 2]')

    expect(page.locator('#node-control')).to_be_hidden()
    expect(page.locator('#node-component')).to_be_visible()

    pause.click()
    page.evaluate('pushEvent(1); pushEvent(2)')
    page.wait_for_timeout(250)
    expect(sequence).to_have_text('')
    expect(packet).to_be_hidden()
    expect(page.locator('#backlog')).to_have_text('Paused · 2 queued')

    pause.click()
    expect(packet).to_be_visible()
    page.wait_for_timeout(100)
    pause.click()
    position = packet.evaluate('e => [e.style.left, e.style.top]')
    page.evaluate('pushEvent(3)')
    page.wait_for_timeout(1000)
    assert position == packet.evaluate('e => [e.style.left, e.style.top]')
    expect(packet).to_be_visible()
    expect(sequence).to_have_text('')
    expect(pause).to_have_attribute('aria-pressed', 'true')
    expect(pause).to_have_text('Resume replay')

    pause.click()
    expect(sequence).to_have_text('#1')
    assert_arrived_at('component')
    pause.click()
    page.wait_for_timeout(900)
    expect(sequence).to_have_text('#1')
    expect(packet).to_be_hidden()
    page.evaluate("$('duration').value = 0")
    pause.click()
    page.wait_for_timeout(100)
    expect(sequence).to_have_text('#1')
    page.wait_for_function('!playing && queue.length === 0')
    assert page.locator('#event-log li').evaluate_all(
        'items => items.map(item => item.dataset.sequence)') == ['3', '2', '1']

    pause.click()
    page.evaluate('pushEvent(4)')
    page.wait_for_timeout(150)
    expect(sequence).to_have_text('#3')
    page.evaluate('disconnectAll()')
    page.wait_for_function('!playing && queue.length === 0')
    expect(pause).to_have_text('Pause replay')
    expect(packet).to_be_hidden()
    animate_commands = page.get_by_label('Animate control commands', exact=True)
    expect(animate_commands).not_to_be_checked()
    page.evaluate("""() => {
        pidNames.set('worker', 'workerA');
        $('duration').value = 800;
        $('delay').value = 0;
        window.commandEvent = sequence => observe('$component/debug', JSON.stringify({
            event: 'message', direction: 'received',
            topic: control + '/workerA/connect', payload: '{}', sequence
        }));
    }""")
    page.evaluate('commandEvent(5)')
    expect(sequence).to_have_text('#5', timeout=500)
    expect(packet).to_be_hidden()
    animate_commands.check()
    expect(page.locator('#node-control')).to_be_visible()
    page.evaluate('commandEvent(6)')
    expect(packet).to_be_visible()
    expect(page.locator('#packet-topic')).to_have_text(
        page.evaluate("control + '/workerA/connect'"))
    page.wait_for_function('!playing && queue.length === 0')
    animate_commands.uncheck()
    expect(page.locator('#node-control')).to_be_hidden()
    page.evaluate('commandEvent(7); pushEvent(8)')
    expect(sequence).to_have_text('#7', timeout=500)
    expect(packet).to_be_visible()
    expect(page.locator('#packet-topic')).to_have_text('$component/ready')
    page.wait_for_function('!playing && queue.length === 0')
    page.evaluate("""() => {
        pidNames.clear();
        window.declarationEvent = {
            event: 'subscription', action: 'subscribe_requested', component_id: 'new-router',
            topics: ['$component/' + session + '-router/events',
                '$provide/service/' + session + '/reg-route',
                '$consume/service/' + session + '/cache'], sequence: 9
        };
        observe('$component/debug', JSON.stringify(declarationEvent));
    }""")
    expect(animate_commands).not_to_be_checked()
    expect(packet).to_be_visible()
    expect(page.locator('#packet-label')).to_contain_text('SUBSCRIBE · declarations')
    expect(page.locator('#packet-topic')).to_have_text(
        page.evaluate("declarationEvent.topics.slice(1).join('\\n')"))
    assert page.evaluate("pidNames.get('new-router')") == 'router'
    page.wait_for_function('!playing && queue.length === 0')
    expect(page.locator('#event-log li').first).to_contain_text('$provide/service/')
    assert_arrived_at('component')
    page.evaluate("observe('$component/debug', JSON.stringify({...declarationEvent, action:'subscribe_allowed', sequence:10}))")
    expect(sequence).to_have_text('#10', timeout=500)
    expect(packet).to_be_hidden()
    animate_commands.check()
    page.evaluate("""observe('$component/debug', JSON.stringify({
        event: 'message', component_id: 'new-router', direction: 'received',
        topic: control + '/reply', payload: '{}', sequence: 11
    }))""")
    expect(packet).to_be_visible()
    page.wait_for_function('!playing && queue.length === 0')
    assert_arrived_at('control')
    animate_commands.uncheck()
    page.evaluate("""observe('$component/debug', JSON.stringify({
        event: 'message', component_id: 'new-router', direction: 'received',
        topic: '$component-admin/disable', payload: '', sequence: 12
    }))""")
    expect(packet).to_be_visible()
    expect(packet).to_have_attribute('data-message-kind', 'administrative')
    page.wait_for_function('!playing && queue.length === 0')
    assert_arrived_at('component')
    assert not errors, errors
    page.evaluate("""() => {
        window.routerState = {clientid: session + '-router', state: 'stopping',
            connected: true, activation_block: 'disabled'};
        observe('$component/debug', JSON.stringify({event: 'component_changed',
            component_id: 'new-router', previous: null, current: routerState, sequence: 13}));
    }""")
    router = page.locator('#node-router')
    expect(router).to_have_attribute('data-state', 'stopping')
    expect(router.locator('.node-status')).to_have_text('Stopping · disabled')
    page.evaluate("""observe('$component/debug', JSON.stringify({event: 'component_changed',
        component_id: 'new-router', previous: routerState,
        current: {...routerState, connected: false}, sequence: 14}))""")
    expect(router).to_have_attribute('data-state', 'disconnected')
    expect(router.locator('.node-status')).to_have_text('Disconnected · cleanup pending')
    page.evaluate("""observe('$component/debug', JSON.stringify({event: 'component_changed',
        component_id: 'new-router', previous: {...routerState, connected: false},
        current: null, sequence: 15}))""")
    expect(router.locator('.node-status')).to_have_text('Disconnected')
    animate_commands.check()
    lifecycle_topic = page.evaluate("'$component/' + session + '-router/events'")
    response_cases = [
        ('$component/reply/258', 'received', 'ok', '', 'effect'),
        ('$component/reply/258', 'received', 'error', '{"event":"application data"}', 'effect'),
        ('$component/retracted/258', 'received', 'retracted', '', 'effect'),
        ('$component/cleanup_complete', 'received', None, '', 'control'),
        (page.evaluate("control + '/observer/reply'"), 'received', '200', '{"handler":"workerA"}', 'control'),
    ]
    response_cases += [
        (lifecycle_topic, 'sent', event, '', kind)
        for event, kind in [('activated', 'control'), ('aborted', 'control'),
                            ('retry_accepted', 'control'), ('released', 'effect'),
                            ('enabled', 'administrative'), ('disabled', 'administrative'),
                            ('error', 'control'), ('stopped', 'control')]
    ]
    for number, (topic, direction, status, payload, kind) in enumerate(response_cases, start=16):
        properties = [{'key': 'component-status', 'value': status}] if status else []
        page.evaluate("""event => observe('$component/debug', JSON.stringify(event))""", {
            'event': 'message', 'component_id': 'new-router', 'direction': direction,
            'topic': topic, 'payload': payload, 'sequence': number,
            'properties': {'User-Property': properties},
        })
        expect(packet).to_be_visible()
        expect(page.locator('#packet-label')).to_have_text('Response: ' + (status or 'cleanup_complete'))
        expect(page.locator('#packet-topic')).to_have_text(payload)
        expect(packet).to_have_attribute('data-message-kind', kind)
        page.wait_for_function('!playing && queue.length === 0')
        expect(page.locator('#event-log li').first).to_contain_text(topic)
    animate_commands.uncheck()
    for event in ['initialize', 'deactivated', 'cleanup_requested']:
        page.evaluate("""event => observe('$component/debug', JSON.stringify(event))""", {
            'event': 'message', 'component_id': 'new-router', 'direction': 'sent',
            'topic': lifecycle_topic, 'payload': json.dumps({'event': event}), 'sequence': 28,
        })
        expect(packet).to_be_visible()
        expect(page.locator('#packet-label')).to_have_text('Control · ' + event)
        expect(page.locator('#packet-topic')).to_have_text(lifecycle_topic)
        page.wait_for_function('!playing && queue.length === 0')
    page.evaluate("""observe('$component/debug', JSON.stringify({
        event: 'message', component_id: 'new-router', direction: 'received',
        topic: control + '/observer/reply', payload: '', sequence: 31
    }))""")
    expect(sequence).to_have_text('#31', timeout=500)
    expect(packet).to_be_hidden()
    animate_commands.check()
    page.evaluate('commandEvent(32)')
    expect(packet).to_be_visible()
    pause.click()
    animate_commands.uncheck()
    expect(page.locator('#node-control')).to_be_hidden()
    expect(packet).to_be_hidden()
    pause.click()
    page.wait_for_function('!playing && queue.length === 0')
    expect(sequence).to_have_text('#32')
    assert not errors, errors
    browser.close()
print('Replay checks passed: pause before replay, freeze mid-animation, preserve delay, queue order, and cancel while paused.')
