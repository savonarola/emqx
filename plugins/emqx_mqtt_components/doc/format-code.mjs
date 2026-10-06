import {readFile, realpath, rename, writeFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {pathToFileURL} from 'node:url';

const [cli, format, scale, config, source] = process.argv.slice(2);
const slugs = ['connect-and-declare', 'activate', 'deactivate-and-clean-up', 'interrupt-initialization', 'handle-dependent-loss'];
const require = createRequire(await realpath(cli));
const {default: puppeteer} = await import(pathToFileURL(require.resolve('puppeteer')));
const browser = await puppeteer.launch(JSON.parse(await readFile(config, 'utf8')));
try {
  for (const [index, slug] of slugs.entries()) {
    const path = `images/${source}-${slug}.svg`;
    if (format === 'svg') await rename(`images/${source}-${index + 1}.svg`, path);
    const page = await browser.newPage();
    try {
      await page.setViewport({width: 1920, height: 1080, deviceScaleFactor: Number(scale)});
      await page.goto(pathToFileURL(await realpath(path)).href);
      const svg = await page.evaluate(() => {
        const root = document.documentElement;
        const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
        const nodes = [];
        while (walker.nextNode()) {
          if (walker.currentNode.parentElement.closest('text')) nodes.push(walker.currentNode);
        }
        for (const node of nodes) {
          const parts = node.textContent.split(/(`[^`]+`)/g);
          if (parts.length === 1) continue;
          const fragment = document.createDocumentFragment();
          for (const part of parts) {
            if (part.startsWith('`') && part.endsWith('`')) {
              const span = document.createElementNS(root.namespaceURI, 'tspan');
              span.setAttribute('class', 'inline-code');
              span.setAttribute('style', 'font-family: monospace; font-size: 0.9em');
              span.textContent = part.slice(1, -1);
              fragment.append(span);
            } else {
              fragment.append(document.createTextNode(part));
            }
          }
          node.replaceWith(fragment);
        }
        for (const background of root.querySelectorAll('.inline-code-background')) background.remove();
        for (const span of root.querySelectorAll('.inline-code')) {
          const box = span.getBBox();
          const text = span.closest('text');
          const background = document.createElementNS(root.namespaceURI, 'rect');
          background.setAttribute('class', 'inline-code-background');
          background.setAttribute('x', box.x - 2);
          background.setAttribute('y', box.y - 1);
          background.setAttribute('width', box.width + 4);
          background.setAttribute('height', box.height + 2);
          background.setAttribute('rx', '4');
          background.setAttribute('fill', '#eaecef');
          if (text.hasAttribute('transform')) background.setAttribute('transform', text.getAttribute('transform'));
          text.before(background);
        }
        return new XMLSerializer().serializeToString(root);
      });
      await writeFile(path, svg);
      if (format === 'png') {
        const element = await page.$('svg');
        await element.screenshot({path: path.replace(/\.svg$/, '.png'), omitBackground: true});
      }
    } finally {
      await page.close();
    }
  }
} finally {
  await browser.close();
}
