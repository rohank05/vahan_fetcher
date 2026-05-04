// recheck.js — Find completed combos with no registration data and reset them
// so the scraper re-downloads them on the next run.
//
// Usage:
//   node recheck.js          — dry run, shows what would be reset
//   node recheck.js --fix    — actually deletes from fetch_progress

require('dotenv').config();
const db = require('./db');

const DRY_RUN = !process.argv.includes('--fix');

async function main() {
    await db.initDb();

    const { pool } = db;

    const { rows: empty } = await pool.query(`
        SELECT
            fp.id,
            s.code  AS state_code,
            r.code  AS rto_code,
            vc.idx  AS vc_idx,
            vc.alias AS vc_alias,
            fp.year
        FROM fetch_progress fp
        JOIN states s           ON s.id  = fp.state_id
        JOIN rtos r             ON r.id  = fp.rto_id
        JOIN vehicle_classes vc ON vc.id = fp.vehicle_class_id
        WHERE NOT EXISTS (
            SELECT 1 FROM vehicle_registrations vr
            WHERE vr.state_id         = fp.state_id
              AND vr.rto_id           = fp.rto_id
              AND vr.vehicle_class_id = fp.vehicle_class_id
              AND vr.year             = fp.year
        )
        ORDER BY s.code, r.code, vc.idx
    `);

    if (empty.length === 0) {
        console.log('No empty combos found — everything looks good.');
        await db.closeDb();
        return;
    }

    // Group by state for readable output
    const byState = {};
    for (const row of empty) {
        (byState[row.state_code] = byState[row.state_code] || []).push(row);
    }

    console.log(`Found ${empty.length} completed combo(s) with no registration data:\n`);
    for (const [state, rows] of Object.entries(byState)) {
        const rtoCounts = {};
        for (const r of rows) rtoCounts[r.rto_code] = (rtoCounts[r.rto_code] || 0) + 1;
        const rtoSummary = Object.entries(rtoCounts)
            .map(([rto, cnt]) => `${rto}(${cnt})`)
            .join(', ');
        console.log(`  ${state} [year=${rows[0].year}] — ${rows.length} combos across RTOs: ${rtoSummary}`);
    }

    if (DRY_RUN) {
        console.log(`\nDry run — run with --fix to reset these combos for re-download.`);
        await db.closeDb();
        return;
    }

    const ids = empty.map(r => r.id);
    await pool.query('DELETE FROM fetch_progress WHERE id = ANY($1)', [ids]);
    console.log(`\nReset ${ids.length} combos — they will be re-downloaded on next scraper run.`);

    await db.closeDb();
}

main().catch(async err => {
    console.error('Fatal:', err.message);
    await db.closeDb().catch(() => {});
    process.exit(1);
});
