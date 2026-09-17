// loadtest.js — find the rate limit on the new analytics API
// node loadtest.js        (output also written to loadtest.log)
//
// Solves one captcha, checks whether wrong captchas lock the session, then ramps
// concurrency 1 → 3 → 5 → 10 per endpoint. Stops at the first non-200 response.
const fs = require('fs');
const path = require('path');
const { createWorker } = require('tesseract.js');

const BASE = 'https://analytics.parivahan.gov.in/analytics';
const UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36';
const FORM = 'archivedFlags=ACTIVE_COMPLIANT&archivedFlags=ACTIVE_NON_COMPLIANT&archivedFlags=PERMANENT_ARCHIVE&archivedFlags=TEMPORARY_ARCHIVE&_archivedFlags=1'
    + '&timePeriod=0&_financialYearList=1&fromYear=2026&toYear=2026&fromDate=1+Jan+2026&toDate=31+Dec+2026&reportYear=2027&reportMonth='
    + '&_stateMultiple=1&_rtoCodeMultiple=1&_vehicleEmissions=1&_vehicleMakers=1&selectedMakersCsv=&_vehicleCategoryGroup=1&_vehicleSubCategories=1'
    + '&_vehicleClasses=1&_vehicleFuels=1&_evType=1&_vehicleStatus=1&_vehicleOwnerType=1&vehicleType=&fitnessCheck=0&delhiNcr=0'
    + '&yAxis=vehicleMakerName&xAxis=monthWise&last5financialYearList=';

// [label, endpoint, requests, concurrency]
const RAMP = [
    ['report seq',    'report', 60,  1],
    ['report conc3',  'report', 90,  3],
    ['report conc5',  'report', 150, 5],
    ['report conc10', 'report', 200, 10],
    ['makers conc10', 'makers', 100, 10],
    ['rtos conc10',   'rtos',   100, 10],
];

const LOG = path.join(__dirname, 'loadtest.log');
const log = (...a) => { const line = a.join(' '); console.log(line); fs.appendFileSync(LOG, line + '\n'); };
const time = () => new Date().toTimeString().slice(0, 8);
const sleep = ms => new Promise(r => setTimeout(r, ms));

const dd = require('./dropdowns.json');
const jar = new Map();
let csrf;

async function req(url, opts = {}) {
    const res = await fetch(url, {
        ...opts,
        redirect: 'manual',
        signal: AbortSignal.timeout(60_000),
        headers: {
            'User-Agent': UA,
            Referer: `${BASE}/vahanpublicreport?lang=en`,
            Origin: 'https://analytics.parivahan.gov.in',
            Cookie: [...jar].map(([k, v]) => `${k}=${v}`).join('; '),
            ...opts.headers,
        },
    });
    for (const c of res.headers.getSetCookie()) {
        const [kv] = c.split(';');
        const i = kv.indexOf('=');
        jar.set(kv.slice(0, i), kv.slice(i + 1));
    }
    return res;
}

const post = extra => ({
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8' },
    body: `${FORM}&_csrf=${csrf}&${extra}`,
});

async function tryCaptcha(ocr, forcedGuess) {
    const img = Buffer.from(await (await req(`${BASE}/captcha-gen?_ts=${Date.now()}&_seq=1`)).arrayBuffer());
    const guess = forcedGuess ?? (await ocr.recognize(img)).data.text.replace(/\s/g, '');
    await (await req(`${BASE}/vahanpublicreport?lang=en`, post(`captcha=${encodeURIComponent(guess)}`))).text();
    const r = await req(`${BASE}/vahanpublicreport/maker-report-page`, post('pageSize=25&afterMaker='));
    await r.text();
    return { guess, unlocked: r.status === 200 };
}

// Delhi RTO × class combos so requests differ (a server-side cache would hide the real limit)
const combos = dd.rtoCode.DL.flatMap(r => dd.vehicleClass.slice(0, 20).map(vc =>
    `stateMultiple=DL&rtoCodeMultiple=${r.value}&vehicleClasses=${encodeURIComponent(vc.value)}`));
let seq = 0;

async function hit(endpoint) {
    const t0 = Date.now();
    try {
        let r;
        if (endpoint === 'report')
            r = await req(`${BASE}/vahanpublicreport/maker-report-page`, post(`${combos[seq++ % combos.length]}&pageSize=25&afterMaker=`));
        else if (endpoint === 'makers')
            r = await req(`${BASE}/vahanpublicreport/lazy/vehicle-makers?page=${seq++ % 300}&size=25&search=`);
        else
            r = await req(`${BASE}/json_rtos?stateCode=${dd.stateName[seq++ % dd.stateName.length].value}`);
        const body = await r.text();
        const limitHeaders = [...r.headers].filter(([k]) => /rate|retry|limit/i.test(k)).map(([k, v]) => `${k}=${v}`).join(',');
        return { status: r.status, ms: Date.now() - t0, limitHeaders, body: r.status === 200 ? '' : body.slice(0, 150) };
    } catch (e) {
        return { status: `ERR:${e.cause?.code || e.name}`, ms: Date.now() - t0 };
    }
}

async function round(label, endpoint, total, conc) {
    const t0 = Date.now();
    const results = [];
    let started = 0;
    await Promise.all(Array.from({ length: conc }, async () => {
        while (started++ < total) results.push(await hit(endpoint));
    }));
    const secs = (Date.now() - t0) / 1000;
    const counts = {};
    results.forEach(r => counts[r.status] = (counts[r.status] || 0) + 1);
    const lat = results.map(r => r.ms).sort((a, b) => a - b);
    const bad = results.find(r => r.status !== 200);
    log(`${time()} ${label.padEnd(14)} n=${total} conc=${conc} ${secs.toFixed(1)}s ${(total / secs).toFixed(1)} req/s`,
        `status=${JSON.stringify(counts)} p50=${lat[lat.length >> 1]}ms p95=${lat[Math.floor(lat.length * 0.95)]}ms`,
        results.find(r => r.limitHeaders)?.limitHeaders || '',
        bad ? `first-error=${JSON.stringify(bad)}` : '');
    return !bad;
}

(async () => {
    log(`\n=== loadtest ${new Date().toISOString()} ===`);
    const ocr = await createWorker('eng', 1, { cachePath: __dirname });
    await ocr.setParameters({
        tessedit_char_whitelist: 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789',
        tessedit_pageseg_mode: '7',
    });

    csrf = (await (await req(`${BASE}/vahanpublicreport?lang=en`)).text()).match(/name="_csrf" value="([^"]+)"/)?.[1];
    if (!csrf) throw new Error('No _csrf on page — blocked or page changed');

    for (let k = 1; k <= 5; k++)
        log(`wrong captcha #${k}: unlocked=${(await tryCaptcha(ocr, 'zzzzzz')).unlocked}`);

    let unlocked = false;
    for (let k = 1; k <= 5 && !unlocked; k++) {
        const r = await tryCaptcha(ocr);
        unlocked = r.unlocked;
        log(`real captcha attempt ${k}: guess=${r.guess} unlocked=${unlocked}`);
    }
    if (!unlocked) { log('Could not unlock session — stopping'); process.exit(1); }
    log(`unlocked at ${time()} (window ~15 min)`);

    for (const [label, endpoint, total, conc] of RAMP) {
        if (!await round(label, endpoint, total, conc)) { log('non-200 seen — stopping ramp'); break; }
        await sleep(5000);
    }
    await ocr.terminate();
    log('done');
})().catch(e => { log('FATAL', e.stack); process.exit(1); });
