// recheck.js
//
// --sync               Backfill fetch_progress from vehicle_registrations
//                      (marks combos as done if data already exists)
//
// --fix                Reset state+RTO+year combos that are in fetch_progress
//                      but have 0 registrations (so scraper re-downloads them)
//
// Filters (work with both modes):
//   --year=2026
//   --state=UK
//
// Examples:
//   node recheck.js --sync                      dry run sync
//   node recheck.js --sync --fix                actually backfill
//   node recheck.js --fix --year=2026 --state=UK  reset UK 2026 empty combos

require('dotenv').config();
const db = require('./db');

const SYNC    = process.argv.includes('--sync');
const DRY_RUN = !process.argv.includes('--fix');
const yearArg  = (process.argv.find(a => a.startsWith('--year='))  || '').replace('--year=',  '');
const stateArg = (process.argv.find(a => a.startsWith('--state=')) || '').replace('--state=', '');

async function runSync(pool) {
    const conditions = ['vr.count > 0'];
    if (yearArg)  conditions.push(`vr.year = ${Number.parseInt(yearArg)}`);
    if (stateArg) conditions.push(`s.code = '${stateArg.toUpperCase()}'`);
    const WHERE = conditions.join(' AND ');

    // Find distinct combos in vehicle_registrations not yet in fetch_progress
    const { rows } = await pool.query(`
        SELECT DISTINCT
            vr.state_id, vr.rto_id, vr.vehicle_class_id, vr.year,
            s.code AS state_code, r.code AS rto_code
        FROM vehicle_registrations vr
        JOIN states s ON s.id = vr.state_id
        JOIN rtos   r ON r.id = vr.rto_id
        WHERE ${WHERE}
          AND NOT EXISTS (
              SELECT 1 FROM fetch_progress fp
              WHERE fp.state_id         = vr.state_id
                AND fp.rto_id           = vr.rto_id
                AND fp.vehicle_class_id = vr.vehicle_class_id
                AND fp.year             = vr.year
          )
        ORDER BY s.code, vr.year, r.code
    `);

    if (rows.length === 0) {
        console.log('Sync: fetch_progress already up to date — nothing to backfill.');
        return;
    }

    // Summarise by state+year
    const summary = {};
    for (const r of rows) {
        const key = `${r.state_code}|${r.year}`;
        if (!summary[key]) summary[key] = { state: r.state_code, year: r.year, count: 0 };
        summary[key].count++;
    }

    console.log(`Sync: found ${rows.length} combo(s) in registrations but missing from fetch_progress:\n`);
    for (const { state, year, count } of Object.values(summary)) {
        console.log(`  ${state} [year=${year}] — ${count} combos`);
    }

    if (DRY_RUN) {
        console.log('\nDry run — add --fix to actually backfill fetch_progress.');
        return;
    }

    const { rowCount } = await pool.query(`
        INSERT INTO fetch_progress (state_id, rto_id, vehicle_class_id, year)
        SELECT DISTINCT vr.state_id, vr.rto_id, vr.vehicle_class_id, vr.year
        FROM vehicle_registrations vr
        JOIN states s ON s.id = vr.state_id
        WHERE ${WHERE}
          AND NOT EXISTS (
              SELECT 1 FROM fetch_progress fp
              WHERE fp.state_id         = vr.state_id
                AND fp.rto_id           = vr.rto_id
                AND fp.vehicle_class_id = vr.vehicle_class_id
                AND fp.year             = vr.year
          )
        ON CONFLICT DO NOTHING
    `);
    console.log(`\nBackfilled ${rowCount} entries into fetch_progress.`);
}

async function runRecheck(pool) {
    const whereConditions = [];
    if (yearArg)  whereConditions.push(`fp.year = ${Number.parseInt(yearArg)}`);
    if (stateArg) whereConditions.push(`s.code = '${stateArg.toUpperCase()}'`);
    const WHERE = whereConditions.length ? `AND ${whereConditions.join(' AND ')}` : '';

    const { rows: empty } = await pool.query(`
        SELECT
            s.code AS state_code,
            r.code AS rto_code,
            fp.year,
            COUNT(fp.id)               AS fetch_entries,
            COALESCE(SUM(vr.count), 0) AS total_registrations
        FROM fetch_progress fp
        JOIN states s ON s.id = fp.state_id
        JOIN rtos   r ON r.id = fp.rto_id
        LEFT JOIN vehicle_registrations vr
            ON  vr.state_id         = fp.state_id
            AND vr.rto_id           = fp.rto_id
            AND vr.vehicle_class_id = fp.vehicle_class_id
            AND vr.year             = fp.year
        WHERE 1=1 ${WHERE}
        GROUP BY s.code, fp.state_id, r.code, fp.rto_id, fp.year
        HAVING COALESCE(SUM(vr.count), 0) = 0
        ORDER BY s.code, fp.year, r.code
    `);

    if (empty.length === 0) {
        console.log('Recheck: no empty RTO+year combos found — everything looks good.');
        return;
    }

    const summary = {};
    for (const row of empty) {
        const key = `${row.state_code}|${row.year}`;
        if (!summary[key]) summary[key] = { state: row.state_code, year: row.year, rtos: 0, entries: 0 };
        summary[key].rtos++;
        summary[key].entries += Number(row.fetch_entries);
    }

    console.log(`Recheck: found ${empty.length} RTO+year pair(s) with 0 registrations:\n`);
    for (const { state, year, rtos, entries } of Object.values(summary)) {
        console.log(`  ${state} [year=${year}] — ${rtos} RTOs, ${entries} fetch_progress entries to reset`);
    }

    if (DRY_RUN) {
        console.log('\nDry run — add --fix to reset these for re-download.');
        return;
    }

    const stateRtoYears = empty.map(r => `('${r.state_code}', '${r.rto_code}', ${r.year})`).join(', ');
    const { rowCount } = await pool.query(`
        DELETE FROM fetch_progress fp
        USING states s, rtos r
        WHERE s.id = fp.state_id
          AND r.id = fp.rto_id
          AND (s.code, r.code, fp.year) IN (${stateRtoYears})
    `);
    console.log(`\nReset ${rowCount} fetch_progress entries — they will be re-downloaded on next scraper run.`);
}

async function main() {
    await db.initDb();
    const { pool } = db;

    if (SYNC) {
        await runSync(pool);
    } else {
        await runRecheck(pool);
    }

    await db.closeDb();
}

main().catch(async err => {
    console.error('Fatal:', err.message);
    await db.closeDb().catch(() => {});
    process.exit(1);
});
