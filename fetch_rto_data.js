// fetch_rto_data.js — Download correct RTO names from VAHAN website
// Saves output to rto_reference.json
// Usage: node fetch_rto_data.js

require('dotenv').config();
const { chromium } = require('playwright');
const fs = require('fs');

const BASE_URL     = 'https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml';
const OUTPUT_FILE  = './rto_reference.json';
const STEP_DELAY   = 1200;
const NAV_TIMEOUT  = 90_000;
const AJAX_TIMEOUT = 90_000;

const STATES = [
    'AN','AP','AR','AS','BR','CG','CH','DD','DL','GA','GJ','HP','HR','JH',
    'JK','KA','KL','LA','LD','MH','ML','MN','MP','MZ','NL','OR','PB','PY',
    'RJ','SK','TG','TN','TR','UK','UP','WB',
];

// Strip ONLY a trailing date like "( 29-NOV-2024 )" from the end of a name
function stripTrailingDate(text) {
    return text.replace(/\(\s*\d{1,2}-[A-Z]{3}-\d{4}\s*\)\s*$/, '').trim();
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function waitForAjax(page) {
    await sleep(300);
    await page.waitForFunction(() => {
        const blocker = document.querySelector('.ui-blockui');
        if (!blocker) return true;
        return blocker.style.display !== 'block';
    }, { timeout: AJAX_TIMEOUT });
    await sleep(STEP_DELAY);
}

async function pfSelect(page, widgetKey, value) {
    await page.evaluate(({ widgetKey, value }) => {
        let w = PrimeFaces.widgets[widgetKey];
        if (!w) {
            for (const k of Object.keys(PrimeFaces.widgets)) {
                const ww = PrimeFaces.widgets[k];
                if (ww && ww.input && ww.input[0] && ww.input[0].name === widgetKey) {
                    w = ww; break;
                }
            }
        }
        if (!w) throw new Error(`PrimeFaces widget not found: ${widgetKey}`);
        w.selectValue(value);
    }, { widgetKey, value });
    await waitForAjax(page);
}

async function getStateWidgetKey(page) {
    return page.evaluate(() => {
        for (const k of Object.keys(PrimeFaces.widgets)) {
            const w = PrimeFaces.widgets[k];
            if (w && w.input && w.input[0]) {
                const oc = w.input[0].getAttribute('onchange') || '';
                if (oc.includes('selectedRto yaxisVar')) return k;
            }
        }
        return null;
    });
}

async function getRtosForState(page, stateCode, stateWidgetKey) {
    await pfSelect(page, stateWidgetKey, stateCode);
    await sleep(STEP_DELAY);

    await page.waitForFunction(() => {
        const sel = document.getElementById('selectedRto_input');
        return sel && sel.options.length > 1;
    }, { timeout: 20_000 });
    await sleep(STEP_DELAY);

    const rtos = await page.evaluate(() => {
        const sel = document.getElementById('selectedRto_input');
        return Array.from(sel.options)
            .filter(o => o.value !== '-1')
            .map(o => ({ value: o.value, rawText: o.text }));
    });

    return rtos.map(r => ({
        value: r.value,
        rawText: r.rawText,
        text: stripTrailingDate(r.rawText),
    }));
}

async function main() {
    console.log('Launching browser…');
    const browser = await chromium.launch({ headless: true, args: ['--no-sandbox'] });
    const context = await browser.newContext({ viewport: { width: 1366, height: 768 } });
    const page = await context.newPage();
    page.setDefaultNavigationTimeout(NAV_TIMEOUT);
    page.setDefaultTimeout(30_000);

    console.log('Loading VAHAN page…');
    await page.goto(BASE_URL, { waitUntil: 'networkidle', timeout: NAV_TIMEOUT });
    await page.waitForFunction(
        () => window.PrimeFaces && Object.keys(PrimeFaces.widgets).length > 5,
        { timeout: 30_000 }
    );
    await sleep(STEP_DELAY);

    // Configure axis/year — needed to make state dropdown trigger RTO loading
    await pfSelect(page, 'widget_yaxisVar', 'Maker');
    await pfSelect(page, 'widget_xaxisVar', 'Month Wise');
    console.log('Page configured ✓');

    const stateWidgetKey = await getStateWidgetKey(page);
    if (!stateWidgetKey) throw new Error('State dropdown widget not found');

    const result = {};
    let totalRtos = 0;

    for (const stateCode of STATES) {
        process.stdout.write(`  ${stateCode}… `);
        try {
            const rtos = await getRtosForState(page, stateCode, stateWidgetKey);
            result[stateCode] = rtos;
            totalRtos += rtos.length;
            console.log(`${rtos.length} RTOs`);
        } catch (err) {
            console.log(`ERROR: ${err.message.split('\n')[0]}`);
            result[stateCode] = [];
        }
    }

    await browser.close();

    fs.writeFileSync(OUTPUT_FILE, JSON.stringify(result, null, 2));
    console.log(`\nSaved ${OUTPUT_FILE} — ${STATES.length} states, ${totalRtos} RTOs total`);
}

main().catch(err => {
    console.error('Fatal:', err.message);
    process.exit(1);
});
