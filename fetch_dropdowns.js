// fetch_dropdowns.js — dump every dropdown on the new analytics report page to dropdowns.json
// node fetch_dropdowns.js
const fs = require('fs');

const BASE = 'https://analytics.parivahan.gov.in/analytics';
const HEADERS = { 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36' };

const get = async (url, as = 'json') => {
    const res = await fetch(url, { headers: HEADERS });
    if (!res.ok) throw new Error(`HTTP ${res.status} ${url}`);
    return res[as]();
};

// ponytail: regex HTML parsing — fine for this page's flat <select>/<option> markup
const unescape = s => s.replace(/&amp;/g, '&').replace(/&lt;/g, '<').replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&#39;/g, "'");
const text = s => unescape(s.replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim());

function parseSelects(html) {
    const out = {};
    html = html.replace(/<!--[\s\S]*?-->/g, '');
    for (const [, attrs, body] of html.matchAll(/<select([^>]*)>([\s\S]*?)<\/select>/g)) {
        const id = attrs.match(/\bid="([^"]+)"/)?.[1];
        out[id] = [...body.matchAll(/<option([^>]*)>([\s\S]*?)<\/option>/g)].map(([, a, t]) => ({
            value: unescape(a.match(/value="([^"]*)"/)?.[1] ?? text(t)),
            text: text(t) || unescape(a.match(/label="([^"]*)"/)?.[1] ?? '').trim(),
        }));
    }
    return out;
}

// xAxis options are built in JS from the chosen yAxis: if (yAxisValue === 'x') { options = [...] }
function parseXAxisByYAxis(html) {
    const js = html.replace(/\/\*[\s\S]*?\*\//g, '').replace(/^\s*\/\/.*$/gm, '');
    const out = {};
    for (const [, y, body] of js.matchAll(/yAxisValue === '(\w+)'[^{]*\{[^\[]*?options = \[([\s\S]*?)\];/g))
        out[y] ??= [...body.matchAll(/value: "([^"]+)", label: "([^"]+)"/g)].map(([, value, text]) => ({ value, text }));
    return out;
}

async function main() {
    const html = await get(`${BASE}/vahanpublicreport?lang=en`, 'text');
    const dropdowns = parseSelects(html);
    dropdowns.xAxis = parseXAxisByYAxis(html); // keyed by yAxis value

    // rtoCode is filled per state via AJAX
    dropdowns.rtoCode = {};
    for (const { value: stateCode } of dropdowns.stateName) {
        const rtos = await get(`${BASE}/json_rtos?stateCode=${stateCode}`);
        dropdowns.rtoCode[stateCode] = rtos.map(r => ({ value: String(r.rtoCode), text: r.rtoName }));
        console.log(`RTOs ${stateCode}: ${rtos.length}`);
    }

    // vehicleMaker is lazy-loaded, paginated alphabetically
    const makers = [];
    for (let page = 0, batch; ; page++) {
        batch = await get(`${BASE}/vahanpublicreport/lazy/vehicle-makers?page=${page}&size=1000&search=`);
        makers.push(...batch);
        if (batch.length < 1000) break;
    }
    dropdowns.vehicleMaker = makers.map(m => ({ value: m, text: m }));

    // fail loudly if the page markup changes under the regex parsers
    const assert = require('assert');
    assert(dropdowns.stateName.length > 30, 'stateName parse failed');
    assert(dropdowns.yAxis.length > 10 && dropdowns.yAxis.every(o => o.text), 'yAxis parse failed');
    assert(Object.keys(dropdowns.xAxis).length > 10, 'xAxis parse failed');
    assert(makers.length > 1000, 'maker fetch failed');

    fs.writeFileSync('dropdowns.json', JSON.stringify(dropdowns, null, 2));
    for (const [id, v] of Object.entries(dropdowns))
        console.log(`${id}: ${Array.isArray(v) ? v.length : Object.values(v).flat().length}`);
}

if (require.main === module) main().catch(err => { console.error(err); process.exit(1); });

module.exports = { parseSelects };
