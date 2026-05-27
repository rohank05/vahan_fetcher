// fix_rto_states.js — Fix old-format numeric-code RTOs that are in the wrong state,
// and clean up within-state duplicates (numeric code + prefixed code for same RTO).
//
// Usage:
//   node fix_rto_states.js           — dry run
//   node fix_rto_states.js --apply   — apply fixes

require('dotenv').config();
const { Pool } = require('pg');

const DRY_RUN = !process.argv.includes('--apply');
if (DRY_RUN) console.log('[DRY RUN] Pass --apply to apply.\n');

const pool = new Pool({
    host:     process.env.PG_HOST     || 'localhost',
    port:     parseInt(process.env.PG_PORT || '5432'),
    user:     process.env.PG_USER     || 'postgres',
    password: process.env.PG_PASSWORD,
    database: process.env.PG_DATABASE,
});

// Odisha's state code in DB is OR but RTO names use OD prefix
const STATE_ALIAS = { OD: 'OR' };

function resolveState(nameState, stateByCode) {
    const aliased = STATE_ALIAS[nameState] || nameState;
    return stateByCode.get(aliased) || null;
}

async function mergeRto(client, sourceId, targetId, targetStateId) {
    // Move registrations: if conflict exists keep existing (target already has correct data)
    await client.query(`
        INSERT INTO vehicle_registrations
            (state_id, rto_id, vehicle_class_id, maker_id, year, month, count, created_at)
        SELECT $2, $1, vehicle_class_id, maker_id, year, month, count, created_at
        FROM vehicle_registrations
        WHERE rto_id = $3
        ON CONFLICT (state_id, rto_id, vehicle_class_id, maker_id, year, month) DO NOTHING
    `, [targetId, targetStateId, sourceId]);

    // Move fetch_progress
    await client.query(`
        INSERT INTO fetch_progress (state_id, rto_id, vehicle_class_id, year, completed_at)
        SELECT $2, $1, vehicle_class_id, year, completed_at
        FROM fetch_progress WHERE rto_id = $3
        ON CONFLICT DO NOTHING
    `, [targetId, targetStateId, sourceId]);

    // Move fetch_logs (no unique constraint — just re-point)
    await client.query(
        'UPDATE fetch_logs SET rto_id = $1, state_id = $2 WHERE rto_id = $3',
        [targetId, targetStateId, sourceId]
    );

    // Delete source registrations then source RTO
    await client.query('DELETE FROM vehicle_registrations WHERE rto_id = $1', [sourceId]);
    await client.query('DELETE FROM fetch_progress WHERE rto_id = $1', [sourceId]);
    await client.query('DELETE FROM rtos WHERE id = $1', [sourceId]);
}

async function main() {
    const { rows: stateRows } = await pool.query('SELECT id, code FROM states');
    const stateByCode = new Map(stateRows.map(s => [s.code, s]));

    // ── Phase 1: Wrong-state old-format RTOs ─────────────────────────────────
    const { rows: wrongState } = await pool.query(`
        SELECT r.id, r.code, r.name, r.state_id, s.code AS db_state,
               substring(r.name FROM ' - ([A-Z]{2,3})[0-9]+$') AS name_state,
               (SELECT COUNT(*) FROM vehicle_registrations WHERE rto_id = r.id) AS reg_count
        FROM rtos r JOIN states s ON s.id = r.state_id
        WHERE r.code ~ '^[0-9]+$'
          AND r.name ~ ' - [A-Z]{2,3}[0-9]+$'
        ORDER BY s.code, r.code
    `);

    const reallyWrong = wrongState.filter(r => {
        if (!r.name_state) return false;
        const correctState = resolveState(r.name_state, stateByCode);
        if (!correctState) return false;
        return correctState.code !== r.db_state;
    });

    console.log(`Wrong-state old-format RTOs: ${reallyWrong.length}`);
    const withRegs = reallyWrong.filter(r => parseInt(r.reg_count) > 0);
    const noRegs   = reallyWrong.filter(r => parseInt(r.reg_count) === 0);
    console.log(`  with registrations: ${withRegs.length}`);
    console.log(`  without registrations: ${noRegs.length}`);

    // Group by db_state -> correct_state for summary
    const groups = {};
    for (const r of reallyWrong) {
        const cs = resolveState(r.name_state, stateByCode);
        const k = `${r.db_state} -> ${cs?.code}`;
        groups[k] = (groups[k] || 0) + 1;
    }
    console.log('  breakdown:', groups);

    // ── Phase 2: Within-state duplicates ─────────────────────────────────────
    // Find numeric-code RTOs that co-exist with a prefixed-code RTO in same state
    const { rows: allNumeric } = await pool.query(`
        SELECT r.id, r.code, r.name, r.state_id, s.code AS state_code,
               substring(r.name FROM ' - ([A-Z]{2,3}[0-9]+)$') AS prefixed_code,
               (SELECT COUNT(*) FROM vehicle_registrations WHERE rto_id = r.id) AS reg_count
        FROM rtos r JOIN states s ON s.id = r.state_id
        WHERE r.code ~ '^[0-9]+$'
          AND r.name ~ ' - [A-Z]{2,3}[0-9]+$'
        ORDER BY s.code, r.code
    `);

    // For each numeric RTO, look for a matching prefixed-code RTO in the same state
    const samePairs = []; // { numeric: row, prefixed: { id, code } }
    for (const r of allNumeric) {
        if (!r.prefixed_code) continue;
        const { rows: found } = await pool.query(
            'SELECT id FROM rtos WHERE code = $1 AND state_id = $2',
            [r.prefixed_code, r.state_id]
        );
        if (found.length > 0) {
            samePairs.push({ numeric: r, prefixedId: found[0].id });
        }
    }
    console.log(`\nWithin-state duplicates (numeric + prefixed): ${samePairs.length}`);
    const pairsWithRegs = samePairs.filter(p => parseInt(p.numeric.reg_count) > 0);
    const pairsNoRegs   = samePairs.filter(p => parseInt(p.numeric.reg_count) === 0);
    console.log(`  numeric has registrations: ${pairsWithRegs.length}`);
    console.log(`  numeric has no registrations: ${pairsNoRegs.length}`);

    if (DRY_RUN) {
        console.log('\nDry run done. Pass --apply to fix.');
        await pool.end();
        return;
    }

    // ── Apply Phase 1: Fix wrong-state RTOs ──────────────────────────────────
    console.log('\n=== Fixing wrong-state RTOs ===');
    let fixed = 0, failed = 0;

    for (const r of reallyWrong) {
        const correctState = resolveState(r.name_state, stateByCode);
        if (!correctState) { failed++; continue; }

        const client = await pool.connect();
        try {
            await client.query('BEGIN');

            // Look for an existing RTO in the correct state: prefixed code first, then numeric
            const prefixedCode = r.name_state + r.code; // e.g. "AP" + "102" = "AP102" -- wait, name_state could be "AP" and code is "102"
            // Actually prefixed code = the full code in name like "AP102"
            const nameMatch = r.name.match(/ - ([A-Z]{2,3}[0-9]+)$/);
            const fullCode = nameMatch ? nameMatch[1] : null;

            let targetId = null;
            if (fullCode) {
                const { rows: pf } = await client.query(
                    'SELECT id FROM rtos WHERE code = $1 AND state_id = $2',
                    [fullCode, correctState.id]
                );
                if (pf.length) targetId = pf[0].id;
            }
            if (!targetId) {
                const { rows: nm } = await client.query(
                    'SELECT id FROM rtos WHERE code = $1 AND state_id = $2',
                    [r.code, correctState.id]
                );
                if (nm.length) targetId = nm[0].id;
            }

            if (targetId) {
                // Merge into existing target RTO
                await mergeRto(client, r.id, targetId, correctState.id);
                process.stdout.write('M');
            } else {
                // No target exists: move numeric RTO to correct state
                await client.query(
                    'UPDATE rtos SET state_id = $1 WHERE id = $2', [correctState.id, r.id]
                );
                await client.query(
                    'UPDATE vehicle_registrations SET state_id = $1 WHERE rto_id = $2',
                    [correctState.id, r.id]
                );
                await client.query(
                    'UPDATE fetch_progress SET state_id = $1 WHERE rto_id = $2',
                    [correctState.id, r.id]
                );
                await client.query(
                    'UPDATE fetch_logs SET state_id = $1 WHERE rto_id = $2',
                    [correctState.id, r.id]
                );
                process.stdout.write('m');
            }

            await client.query('COMMIT');
            fixed++;
        } catch (err) {
            await client.query('ROLLBACK');
            console.error(`\nFailed [${r.db_state}] ${r.code}: ${err.message.split('\n')[0]}`);
            failed++;
        } finally {
            client.release();
        }
    }
    console.log(`\nWrong-state fixed: ${fixed}, failed: ${failed}`);

    // ── Apply Phase 2: Remove within-state numeric duplicates ─────────────────
    console.log('\n=== Removing within-state numeric duplicates ===');
    let cleaned = 0;

    // Re-fetch pairs since some may have been resolved in Phase 1
    const { rows: remaining } = await pool.query(`
        SELECT r.id, r.code, r.name, r.state_id,
               substring(r.name FROM ' - ([A-Z]{2,3}[0-9]+)$') AS prefixed_code,
               (SELECT COUNT(*) FROM vehicle_registrations WHERE rto_id = r.id) AS reg_count
        FROM rtos r
        WHERE r.code ~ '^[0-9]+$'
          AND r.name ~ ' - [A-Z]{2,3}[0-9]+$'
        ORDER BY r.state_id, r.code
    `);

    for (const r of remaining) {
        if (!r.prefixed_code) continue;
        const { rows: found } = await pool.query(
            'SELECT id FROM rtos WHERE code = $1 AND state_id = $2',
            [r.prefixed_code, r.state_id]
        );
        if (!found.length) continue;
        const targetId = found[0].id;

        const client = await pool.connect();
        try {
            await client.query('BEGIN');
            await mergeRto(client, r.id, targetId, r.state_id);
            await client.query('COMMIT');
            cleaned++;
            process.stdout.write('.');
        } catch (err) {
            await client.query('ROLLBACK');
            console.error(`\nFailed cleanup id=${r.id}: ${err.message.split('\n')[0]}`);
        } finally {
            client.release();
        }
    }
    console.log(`\nWithin-state duplicates removed: ${cleaned}`);

    await pool.end();
    console.log('\nDone ✓');
}

main().catch(err => {
    console.error('Fatal:', err.message);
    pool.end().catch(() => {});
    process.exit(1);
});
