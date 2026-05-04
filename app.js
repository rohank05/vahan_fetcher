// app.js — VAHAN Dashboard Scraper (multi-worker)
// node app.js                  — uses WORKERS env var (default 3)
// WORKERS=5 node app.js        — 5 parallel workers

require('dotenv').config();
const { Worker, isMainThread, workerData } = require('worker_threads');
const { chromium } = require('playwright');
const fs   = require('fs');
const path = require('path');
const db   = require('./db');

// ─── CONFIG ──────────────────────────────────────────────────────────────────
const BASE_URL       = 'https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml';
const BASE_DL_DIR    = path.resolve(process.env.DL_DIR || './vahan_downloads');
const WORKER_COUNT   = parseInt(process.env.WORKERS || '3');
const FETCH_YEAR     = parseInt(process.env.YEAR || '2026');
const KEEPALIVE_MS   = 3.5 * 60 * 1000;
const RETRY_DELAY_MS = 20_000;
const MAX_RETRIES    = 6;
const AJAX_TIMEOUT_MS = 90_000;
const NAV_TIMEOUT_MS  = 90_000;
const STEP_DELAY_MS   = 800;
// ─────────────────────────────────────────────────────────────────────────────

// ═══════════════════════════════════════════════════════════════════════════════
// ORCHESTRATOR — runs in main thread, splits states and spawns workers
// ═══════════════════════════════════════════════════════════════════════════════
if (isMainThread) {
    orchestrate().catch(err => {
        console.error('Fatal:', err.message);
        process.exit(1);
    });
}

async function orchestrate() {
    await db.initDb();
    console.log('DB ready ✓');

    const states = await db.loadStates();
    await db.closeDb();

    // Split states into WORKER_COUNT chunks (contiguous — each worker owns full states)
    const chunks = Array.from({ length: WORKER_COUNT }, () => []);
    states.forEach((s, i) => chunks[i % WORKER_COUNT].push(s.code));

    console.log(`Spawning ${WORKER_COUNT} workers…`);
    chunks.forEach((codes, i) =>
        console.log(`  Worker ${i}: ${codes.join(', ')}`)
    );
    console.log();

    const workers = chunks
        .filter(codes => codes.length > 0)
        .map((stateCodes, workerIndex) =>
            new Worker(__filename, { workerData: { workerIndex, stateCodes } })
        );

    await Promise.all(workers.map(w => new Promise((resolve, reject) => {
        w.on('error', reject);
        w.on('exit', code => code === 0 ? resolve() : reject(new Error(`Worker exited with code ${code}`)));
    })));

    console.log('\nAll workers done ✓');
}

// ═══════════════════════════════════════════════════════════════════════════════
// WORKER — runs in each thread
// ═══════════════════════════════════════════════════════════════════════════════
if (!isMainThread) {
    runWorker(workerData).catch(async err => {
        log(`Fatal: ${err.message}`);
        await db.closeDb().catch(() => {});
        process.exit(1);
    });
}

// Per-worker prefix for all console output
function log(...args) {
    if (!isMainThread) {
        const prefix = `[W${workerData.workerIndex}]`;
        console.log(prefix, ...args);
    } else {
        console.log(...args);
    }
}
function warn(...args) {
    if (!isMainThread) {
        const prefix = `[W${workerData.workerIndex}]`;
        console.warn(prefix, ...args);
    } else {
        console.warn(...args);
    }
}

function comboKey(stateCode, rtoCode, vcIdx, year) {
    return `${stateCode}|${rtoCode}|${vcIdx}|${year}`;
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function withRetry(fn, label, maxRetries = MAX_RETRIES) {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            return await fn();
        } catch (err) {
            const msg = String(err.message || err);
            const isServerErr = /50[023]|gateway|timeout|ECONNRESET|net::|server error/i.test(msg);
            warn(`  [RETRY ${attempt}/${maxRetries}] ${label}`);
            warn(`    Reason: ${msg.split('\n')[0]}`);
            if (attempt === maxRetries) throw err;
            const delay = isServerErr ? RETRY_DELAY_MS * 2 : RETRY_DELAY_MS;
            log(`    Waiting ${delay / 1000}s…`);
            await sleep(delay);
        }
    }
}

// ─── AJAX WAIT ────────────────────────────────────────────────────────────────
async function waitForAjax(page, timeoutMs = AJAX_TIMEOUT_MS) {
    await sleep(300);
    await page.waitForFunction(() => {
        const blocker = document.querySelector('.ui-blockui');
        if (!blocker) return true;
        return blocker.style.display !== 'block';
    }, { timeout: timeoutMs });
    await sleep(STEP_DELAY_MS);
}

// ─── PRIMEFACES SELECT ────────────────────────────────────────────────────────
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

// ─── CLICK MAIN REFRESH ───────────────────────────────────────────────────────
async function clickMainRefresh(page) {
    await page.evaluate(() => {
        const filterLayout = document.getElementById('filterLayout');
        for (const b of document.querySelectorAll('button')) {
            if (filterLayout && filterLayout.contains(b)) continue;
            const oc = b.getAttribute('onclick') || '';
            if (oc.includes('VhCatg') && oc.includes('combTablePnl')) { b.click(); return; }
        }
        throw new Error('Main refresh button not found');
    });
    await waitForAjax(page);
}

// ─── CLICK SIDEBAR REFRESH ────────────────────────────────────────────────────
async function clickSidebarRefresh(page) {
    await page.evaluate(() => {
        const sidebar = document.getElementById('filterLayout');
        if (!sidebar) throw new Error('filterLayout not found');
        const btn = Array.from(sidebar.querySelectorAll('button')).find(b =>
            (b.getAttribute('onclick') || '').includes('combTablePnl')
        );
        if (!btn) throw new Error('Sidebar refresh button not found');
        btn.click();
    });
    await waitForAjax(page);
}

// ─── SIDEBAR OPEN ─────────────────────────────────────────────────────────────
async function ensureSidebarOpen(page) {
    const isClosed = await page.evaluate(() => {
        const t = document.getElementById('filterLayout-toggler');
        return t?.classList.contains('ui-layout-toggler-closed');
    });
    if (isClosed) {
        await page.evaluate(() => document.getElementById('filterLayout-toggler').click());
        await sleep(STEP_DELAY_MS);
    }
}

// ─── VEHICLE CLASS SELECT ─────────────────────────────────────────────────────
async function selectOneVehicleClass(page, vcIdx) {
    await page.evaluate((targetIdx) => {
        const inputs = document.querySelectorAll('input[name="VhClass"]');
        inputs.forEach((input, i) => {
            const box = input.closest('.ui-chkbox')?.querySelector('.ui-chkbox-box');
            if (!box) return;
            if (input.checked !== (i === targetIdx)) box.click();
        });
    }, vcIdx);
    await sleep(STEP_DELAY_MS);
}

// ─── EXPORT EXCEL ─────────────────────────────────────────────────────────────
async function downloadExcel(page, destPath) {
    if (!await page.evaluate(() => !!document.querySelector('img[title="Download EXCEL file"]')))
        throw new Error('Export button not found in DOM');

    const downloadPromise = page.waitForEvent('download', { timeout: 60_000 });
    await page.evaluate(() => {
        const btn = document.querySelector('img[title="Download EXCEL file"]')?.closest('a');
        if (!btn) throw new Error('Export anchor not found');
        btn.click();
    });
    await sleep(STEP_DELAY_MS);
    const download = await downloadPromise;
    await download.saveAs(destPath);
    await download.delete();
    log(`     Saved: ${path.basename(destPath)}`);
}

// ─── PAGE RECOVERY ────────────────────────────────────────────────────────────
async function recoverPage(page, state, rto) {
    log('  [RECOVERY] Page lost — reloading…');
    await page.goto(BASE_URL, { waitUntil: 'networkidle', timeout: NAV_TIMEOUT_MS });
    await page.waitForFunction(
        () => window.PrimeFaces && Object.keys(PrimeFaces.widgets).length > 5,
        { timeout: 30_000 }
    );
    await sleep(STEP_DELAY_MS);
    await pfSelect(page, 'widget_yaxisVar', 'Maker');
    await pfSelect(page, 'widget_xaxisVar', 'Month Wise');
    await pfSelect(page, 'widget_selectedYearType', 'C');
    await pfSelect(page, 'widget_selectedYear', String(FETCH_YEAR));
    const newStateKey = await getStateWidgetKey(page);
    await pfSelect(page, newStateKey, state.code);
    await sleep(STEP_DELAY_MS);
    await page.waitForFunction(() => {
        const sel = document.getElementById('selectedRto_input');
        return sel && sel.options.length > 1;
    }, { timeout: 20_000 });
    await pfSelect(page, 'widget_selectedRto', rto.value);
    await clickMainRefresh(page);
    await ensureSidebarOpen(page);
    log('  [RECOVERY] Done ✓');
    return newStateKey;
}

// ─── KEEP-ALIVE ───────────────────────────────────────────────────────────────
async function keepAliveIfNeeded(page, lastKeepAlive) {
    if (Date.now() - lastKeepAlive.ts < KEEPALIVE_MS) return;
    lastKeepAlive.ts = Date.now();
    log('  [KEEP-ALIVE] Pinging session…');
    try {
        await page.evaluate(() => {
            const nav = document.querySelector('nav.navbar');
            if (nav) nav.dispatchEvent(new MouseEvent('mouseover', { bubbles: true }));
        });
    } catch (_) { }
}

// ─── WORKER MAIN ─────────────────────────────────────────────────────────────
async function runWorker({ workerIndex, stateCodes }) {
    const DOWNLOAD_DIR = path.join(BASE_DL_DIR, `w${workerIndex}`);
    fs.mkdirSync(DOWNLOAD_DIR, { recursive: true });

    await db.initDb();
    const [allStates, VEHICLE_CLASSES, done] = await Promise.all([
        db.loadStates(),
        db.loadVehicleClasses(),
        db.loadCompleted(),
    ]);

    // Only process states assigned to this worker
    const STATES = allStates.filter(s => stateCodes.includes(s.code));
    log(`Starting — ${STATES.length} states, ${VEHICLE_CLASSES.length} VCs, ${done.size} combos already done`);

    const browser = await chromium.launch({
        headless: true,
        args: ['--no-sandbox', '--disable-setuid-sandbox'],
        downloadsPath: DOWNLOAD_DIR,
    });
    const context = await browser.newContext({
        acceptDownloads: true,
        viewport: { width: 1366, height: 768 },
    });
    const page = await context.newPage();
    page.setDefaultNavigationTimeout(NAV_TIMEOUT_MS);
    page.setDefaultTimeout(30_000);

    // ── 1. Load page ─────────────────────────────────────────────────────────
    await withRetry(async () => {
        await page.goto(BASE_URL, { waitUntil: 'networkidle', timeout: NAV_TIMEOUT_MS });
        await page.waitForSelector('#filterLayout-toggler', { state: 'attached', timeout: 30_000 });
        await page.waitForFunction(
            () => window.PrimeFaces && Object.keys(PrimeFaces.widgets).length > 5,
            { timeout: 30_000 }
        );
        await sleep(STEP_DELAY_MS);
        log('Page loaded ✓');
    }, 'Page load');

    // ── 2. One-time axis/year config ─────────────────────────────────────────
    await withRetry(async () => {
        await pfSelect(page, 'widget_yaxisVar', 'Maker');
        await pfSelect(page, 'widget_xaxisVar', 'Month Wise');
        await pfSelect(page, 'widget_selectedYearType', 'C');
        await pfSelect(page, 'widget_selectedYear', String(FETCH_YEAR));
        log('Axis/year configured ✓');
    }, 'Axis/year setup');

    let stateWidgetKey = await getStateWidgetKey(page);
    if (!stateWidgetKey) throw new Error('Cannot find state dropdown widget');

    await ensureSidebarOpen(page);

    const lastKeepAlive = { ts: Date.now() };

    // ── 3. Main loop ─────────────────────────────────────────────────────────
    for (const state of STATES) {
        log(`\n${'═'.repeat(55)}`);
        log(`STATE: ${state.name} (${state.code})`);
        log('═'.repeat(55));

        await withRetry(async () => {
            stateWidgetKey = await getStateWidgetKey(page) || stateWidgetKey;
            await pfSelect(page, stateWidgetKey, state.code);
            await sleep(STEP_DELAY_MS);
        }, `Select state ${state.name}`);

        await page.waitForFunction(() => {
            const sel = document.getElementById('selectedRto_input');
            return sel && sel.options.length > 1;
        }, { timeout: 20_000 });
        await sleep(STEP_DELAY_MS);

        const rtos = await page.evaluate(() => {
            const sel = document.getElementById('selectedRto_input');
            return Array.from(sel.options)
                .filter(o => o.value !== '-1')
                .map(o => ({ value: o.value, text: o.text.split('(')[0].trim() }));
        });
        log(`  ${rtos.length} RTOs`);
        await db.saveRtos(state.code, rtos);

        for (const rto of rtos) {
            log(`\n  ── RTO: ${rto.text} (${rto.value})`);

            await withRetry(async () => {
                await pfSelect(page, 'widget_selectedRto', rto.value);
                await sleep(STEP_DELAY_MS);
            }, `Select RTO ${rto.text}`);

            await withRetry(async () => {
                await clickMainRefresh(page);
                await sleep(STEP_DELAY_MS);
            }, `Main refresh ${state.code}/${rto.value}`);

            await ensureSidebarOpen(page);

            for (const vc of VEHICLE_CLASSES) {
                const key = comboKey(state.code, rto.value, vc.idx, FETCH_YEAR);
                if (done.has(key)) { process.stdout.write('·'); continue; }

                await keepAliveIfNeeded(page, lastKeepAlive);

                try {
                    await withRetry(async () => {
                        const pageGone = await page.evaluate(() =>
                            !document.getElementById('filterLayout') || !window.PrimeFaces
                        ).catch(() => true);
                        if (pageGone) stateWidgetKey = await recoverPage(page, state, rto);

                        await selectOneVehicleClass(page, vc.idx);
                        await clickSidebarRefresh(page);

                        const hasError = await page.evaluate(() => {
                            const panel = document.getElementById('combTablePnl') || document.body;
                            return /Service Unavailable|Internal Server Error/i.test(panel.innerText);
                        });
                        if (hasError) throw new Error('Server error after sidebar refresh');

                        const safeName = `${state.code}__${rto.value}__${String(vc.idx).padStart(2, '0')}__${vc.alias || vc.label}__${FETCH_YEAR}`;
                        const destPath = path.join(DOWNLOAD_DIR, `${safeName}.xls`);

                        let rows;
                        if (fs.existsSync(destPath)) {
                            // leftover from a previous partial run — process it now
                            rows = await db.processXlsFile(destPath, `${safeName}.xls`);
                            await db.logFetch(state.code, rto.value, vc.idx, 'success', `${rows} rows (existing file)`);
                        } else {
                            await db.logFetch(state.code, rto.value, vc.idx, 'started', null);
                            await downloadExcel(page, destPath);
                            rows = await db.processXlsFile(destPath, `${safeName}.xls`);
                            await db.logFetch(state.code, rto.value, vc.idx, 'success', `${rows} rows`);
                        }

                        done.add(key);
                        await db.markCompleted(state.code, rto.value, vc.idx, FETCH_YEAR);

                    }, `Export ${state.code}/${rto.value}/vc${vc.idx}:${vc.alias || vc.label}`);

                } catch (err) {
                    const msg = err.message?.split('\n')[0];
                    warn(`\n     [FAILED] ${state.code}/${rto.value}/vc${vc.idx} — ${msg}`);
                    await db.logFetch(state.code, rto.value, vc.idx, 'error', msg);
                }
            }
            log('');
        }
    }

    log('Worker done ✓');
    await browser.close();
    await db.closeDb();
}
