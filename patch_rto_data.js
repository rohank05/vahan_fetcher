// patch_rto_data.js — Update RTO names and fix wrong-state RTOs using rto_reference.json
// Usage:
//   node patch_rto_data.js              — dry run (shows what would change)
//   node patch_rto_data.js --apply      — apply name fixes and move wrong-state RTOs
//   node patch_rto_data.js --apply --fix-states  — also fix wrong-state RTOs (moves registrations too)

require('dotenv').config();
const { Pool } = require('pg');
const fs = require('fs');

const REF_FILE   = './rto_reference.json';
const DRY_RUN    = !process.argv.includes('--apply');
const FIX_STATES = process.argv.includes('--fix-states');

// Strip ONLY a trailing date like "( 29-NOV-2024 )" from the end of a name
function stripTrailingDate(text) {
    return text.replace(/\(\s*\d{1,2}-[A-Z]{3}-\d{4}\s*\)\s*$/, '').trim();
}

if (DRY_RUN) console.log('[DRY RUN] Pass --apply to apply changes.\n');

const pool = new Pool({
    host:     process.env.PG_HOST     || 'localhost',
    port:     parseInt(process.env.PG_PORT || '5432'),
    user:     process.env.PG_USER     || 'postgres',
    password: process.env.PG_PASSWORD,
    database: process.env.PG_DATABASE,
});

async function main() {
    if (!fs.existsSync(REF_FILE)) {
        console.error(`Missing ${REF_FILE} — run fetch_rto_data.js first.`);
        process.exit(1);
    }

    const ref = JSON.parse(fs.readFileSync(REF_FILE, 'utf8'));

    // Load states from DB
    const { rows: stateRows } = await pool.query('SELECT id, code, name FROM states');
    const stateByCode = new Map(stateRows.map(s => [s.code, s]));

    // Load all RTOs from DB
    const { rows: dbRtos } = await pool.query(`
        SELECT r.id, r.code, r.name, r.state_id, s.code AS state_code
        FROM rtos r JOIN states s ON s.id = r.state_id
        ORDER BY s.code, r.code
    `);

    // Build lookup: stateCode -> Map(rtoCode -> dbRto)
    const dbByState = new Map();
    for (const r of dbRtos) {
        if (!dbByState.has(r.state_code)) dbByState.set(r.state_code, new Map());
        dbByState.get(r.state_code).set(r.code, r);
    }

    // Build a set of valid (stateCode, rtoCode) pairs from the reference
    const refValidPairs = new Set(); // `${stateCode}:${rtoCode}`
    // Also build per-state name lookup: `${stateCode}:${rtoCode}` -> cleanName
    const refNameMap = new Map();
    for (const [stateCode, rtos] of Object.entries(ref)) {
        for (const rto of rtos) {
            const key = `${stateCode}:${rto.value}`;
            refValidPairs.add(key);
            refNameMap.set(key, stripTrailingDate(rto.rawText || rto.text));
        }
    }

    // ── 1. Find name mismatches ────────────────────────────────────────────────
    const nameUpdates = []; // { id, rtoCode, stateCode, oldName, newName }

    for (const dbRto of dbRtos) {
        const key = `${dbRto.state_code}:${dbRto.code}`;
        const cleanName = refNameMap.get(key);
        if (cleanName === undefined) continue; // not in reference for this state
        if (dbRto.name !== cleanName) {
            nameUpdates.push({
                id: dbRto.id,
                rtoCode: dbRto.code,
                stateCode: dbRto.state_code,
                oldName: dbRto.name,
                newName: cleanName,
            });
        }
    }

    // ── 2. Find RTOs in wrong state ────────────────────────────────────────────
    // Detect by extracting the state-code prefix embedded in the RTO name.
    // Most names end with " - <STATECODE><DIGITS>" e.g. "Anantapur RTA - AP2".
    // If the embedded prefix doesn't match the DB state, the RTO is misplaced.
    const nameSuffixRx = /\s*-\s*([A-Z]{2,3})\d+\s*$/;

    const wrongState = [];
    for (const dbRto of dbRtos) {
        const m = nameSuffixRx.exec(dbRto.name);
        if (!m) continue; // name has no state prefix suffix — can't determine
        const embeddedState = m[1];
        if (embeddedState === dbRto.state_code) continue; // looks fine
        if (!stateByCode.has(embeddedState)) continue; // unknown state prefix — skip
        wrongState.push({
            id: dbRto.id,
            rtoCode: dbRto.code,
            rtoName: dbRto.name,
            currentStateCode: dbRto.state_code,
            correctStateCode: embeddedState,
        });
    }

    // ── 3. Find RTOs in DB not present in reference for their assigned state ───
    const missing = [];
    for (const dbRto of dbRtos) {
        const key = `${dbRto.state_code}:${dbRto.code}`;
        if (!refValidPairs.has(key)) {
            missing.push({ code: dbRto.code, state: dbRto.state_code, name: dbRto.name });
        }
    }

    // ── 4. Report ──────────────────────────────────────────────────────────────
    console.log(`=== Name updates needed: ${nameUpdates.length} ===`);
    for (const u of nameUpdates) {
        console.log(`  [${u.stateCode}] ${u.rtoCode}`);
        console.log(`    OLD: ${u.oldName}`);
        console.log(`    NEW: ${u.newName}`);
    }

    console.log(`\n=== RTOs in wrong state: ${wrongState.length} ===`);
    for (const w of wrongState) {
        console.log(`  ${w.rtoCode} "${w.rtoName}": DB has state=${w.currentStateCode}, should be ${w.correctStateCode}`);
    }

    console.log(`\n=== RTOs in DB but not in reference for their state: ${missing.length} ===`);
    for (const m of missing) {
        console.log(`  [${m.state}] ${m.code} — "${m.name}"`);
    }

    if (DRY_RUN) {
        console.log('\nDry run complete. Pass --apply to apply changes.');
        await pool.end();
        return;
    }

    // ── 5. Apply name fixes ────────────────────────────────────────────────────
    console.log('\n=== Applying name fixes… ===');
    let nameFixed = 0;
    for (const u of nameUpdates) {
        await pool.query('UPDATE rtos SET name = $1 WHERE id = $2', [u.newName, u.id]);
        nameFixed++;
        console.log(`  Updated [${u.stateCode}] ${u.rtoCode}: "${u.oldName}" → "${u.newName}"`);
    }
    console.log(`Name fixes applied: ${nameFixed}`);

    // ── 6. Apply wrong-state fixes ────────────────────────────────────────────
    if (!FIX_STATES) {
        if (wrongState.length > 0) {
            console.log('\nWrong-state RTOs found but --fix-states not passed. Skipping state fixes.');
            console.log('Re-run with --apply --fix-states to also move RTOs to the correct state.');
        }
        await pool.end();
        return;
    }

    console.log('\n=== Applying wrong-state fixes… ===');
    let stateFixed = 0;
    for (const w of wrongState) {
        const correctState = stateByCode.get(w.correctStateCode);
        if (!correctState) {
            console.warn(`  SKIP ${w.rtoCode}: correct state ${w.correctStateCode} not in DB`);
            continue;
        }

        // Check if correct state already has an RTO with this code (duplicate after move)
        const { rows: existing } = await pool.query(
            'SELECT id FROM rtos WHERE code = $1 AND state_id = $2',
            [w.rtoCode, correctState.id]
        );

        const client = await pool.connect();
        try {
            await client.query('BEGIN');

            if (existing.length > 0) {
                const targetId = existing[0].id;
                console.log(`  ${w.rtoCode}: merging into existing id=${targetId} under ${w.correctStateCode}`);

                // Re-point all vehicle_registrations from wrong rto_id to correct rto_id
                await client.query(
                    `UPDATE vehicle_registrations SET rto_id = $1, state_id = $2
                     WHERE rto_id = $3
                     ON CONFLICT (state_id, rto_id, vehicle_class_id, maker_id, year, month) DO NOTHING`,
                    [targetId, correctState.id, w.id]
                );

                // Re-point fetch_progress
                await client.query(
                    `UPDATE fetch_progress SET rto_id = $1, state_id = $2
                     WHERE rto_id = $3
                     ON CONFLICT DO NOTHING`,
                    [targetId, correctState.id, w.id]
                );

                // Re-point fetch_logs
                await client.query(
                    'UPDATE fetch_logs SET rto_id = $1, state_id = $2 WHERE rto_id = $3',
                    [targetId, correctState.id, w.id]
                );

                // Delete the now-orphaned wrong-state RTO row
                await client.query('DELETE FROM rtos WHERE id = $1', [w.id]);
                console.log(`    Merged & deleted old rto id=${w.id}`);
            } else {
                // Just move the RTO to the correct state
                await client.query(
                    'UPDATE rtos SET state_id = $1 WHERE id = $2',
                    [correctState.id, w.id]
                );

                // Also fix state_id on registrations so it stays consistent
                await client.query(
                    'UPDATE vehicle_registrations SET state_id = $1 WHERE rto_id = $2',
                    [correctState.id, w.id]
                );
                await client.query(
                    'UPDATE fetch_progress SET state_id = $1 WHERE rto_id = $2',
                    [correctState.id, w.id]
                );
                await client.query(
                    'UPDATE fetch_logs SET state_id = $1 WHERE rto_id = $2',
                    [correctState.id, w.id]
                );

                console.log(`  Moved ${w.rtoCode} from ${w.currentStateCode} → ${w.correctStateCode}`);
            }

            await client.query('COMMIT');
            stateFixed++;
        } catch (err) {
            await client.query('ROLLBACK');
            console.error(`  ERROR fixing ${w.rtoCode}: ${err.message}`);
        } finally {
            client.release();
        }
    }
    console.log(`State fixes applied: ${stateFixed}`);

    await pool.end();
    console.log('\nDone ✓');
}

main().catch(err => {
    console.error('Fatal:', err.message);
    pool.end().catch(() => {});
    process.exit(1);
});
