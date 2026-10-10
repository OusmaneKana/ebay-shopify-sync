"""Apply a reviewed local report to staged records; never create Etsy listings."""
import asyncio
import json
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from bson import json_util
from app.database.mongo import db, close_mongo_client
from app.api.routes.reporting import (
    _seller_taxonomy_context, _validate_seller_taxonomy_properties,
    _refresh_publish_run_counts, _now_utc,
)
from app.services.etsy_taxonomy_validation import AUTO_APPROVED, REVIEW_REQUIRED, BLOCKED

RUN_ID = 'etsy-run-f4df5b80-8135-464c-a2c6-d4a260513e94'
ROOT = Path(__file__).resolve().parents[1]

async def main():
    report = json.loads((ROOT / 'reports/etsy_taxonomy_review_2026-09-12.json').read_text())
    rows = report['rows']
    skus = [r['sku'] for r in rows]
    assert len(skus) == len(set(skus)) == 270
    collection = db.etsy_publish_run_items
    run = await db.etsy_publish_runs.find_one({'_id': RUN_ID})
    items = await collection.find({'run_id': RUN_ID, 'sku': {'$in': skus}}).to_list(None)
    assert len(items) == len(skus), 'Staged records missing'
    by_sku = {i['sku']: i for i in items}
    assert all(i.get('decision_status') != 'EXCLUDED' and i.get('creation_status') not in ('created', 'excluded', 'creating') and i.get('processing_status') != 'processing' for i in items), 'Scope changed; aborting'
    _, index, tree_hash = await _seller_taxonomy_context()
    assert tree_hash == report['summary']['seller_tree_hash'] == run['taxonomy_tree_hash'], 'Seller Taxonomy changed'
    tids = sorted({r['recommended_taxonomy_id'] for r in rows if r['recommended_taxonomy_id'] is not None})
    assert all(tid in index for tid in tids)
    semaphore = asyncio.Semaphore(1)
    validated = {i.get("proposed_taxonomy_id"): i["taxonomy_technical_validation"] for i in items if i.get("taxonomy_technical_validation", {}).get("ok")}
    async def validate(tid):
        if tid in validated:
            return tid, validated[tid]
        async with semaphore:
            await asyncio.sleep(1.1)
            result = await _validate_seller_taxonomy_properties(tid)
            for attempt in range(3):
                if result.get("etsy_status_code") != 429:
                    break
                await asyncio.sleep(2 + attempt)
                result = await _validate_seller_taxonomy_properties(tid)
            return tid, result
    technical = dict(await asyncio.gather(*(validate(tid) for tid in tids)))
    print(json.dumps({'validated_categories': len(tids), 'technical_failures': [tid for tid, result in technical.items() if not result.get('ok')]}), flush=True)
    assert all(result.get("ok") for result in technical.values()), "Category validation failed; no staged records changed in this attempt"
    # Complete backup before the first staged-record mutation.
    stamp = _now_utc().strftime('%Y%m%dT%H%M%SZ')
    backup = ROOT / 'reports' / f'etsy_taxonomy_apply_backup_{stamp}.json'
    backup.write_text(json_util.dumps({'run': run, 'items': items}, indent=2))
    results = []
    for row in rows:
        item = by_sku[row['sku']]
        tid = row['recommended_taxonomy_id']
        node = index.get(tid)
        confidence = row['confidence_score'] / 100
        status = AUTO_APPROVED if row['review_status'] == 'High confidence' else REVIEW_REQUIRED
        lookup = technical.get(tid, {'ok': False, 'error': 'unresolved_product_type'})
        if tid is not None and not lookup.get('ok'):
            status = BLOCKED
        # Retain independent product/payload blockers and all creation states.
        old_error = item.get('validation_error_code')
        independent_blocker = item.get('decision_status') == BLOCKED and old_error not in (None, 'taxonomy_technical_validation_failed', 'taxonomy_not_in_current_seller_tree', 'missing_taxonomy_id', 'taxonomy_is_not_leaf')
        if independent_blocker:
            status = BLOCKED
        candidates = []
        if node:
            candidates.append({**node, 'ai_confidence': confidence, 'ai_reason': row['reason'], 'review_selected': True, 'local_score': None})
        alternate = index.get(row.get('alternative_taxonomy_id'))
        if alternate and alternate['taxonomy_id'] != tid:
            candidates.append({**alternate, 'ai_reason': 'Alternative for a plate primarily sold as a collectible display object.', 'local_score': None})
        decision = {'status': status, 'reason': row['reason'], 'reasons': [row['reason']], 'taxonomy_id': tid, 'taxonomy_path': node['full_path'] if node else None, 'taxonomy_leaf': bool(node and node['leaf']), 'source': 'manual' if row['review_status'] == 'High confidence' else 'review_report', 'confidence': confidence}
        now = _now_utc()
        update = {
            'taxonomy_candidates': candidates, 'taxonomy_decision': decision,
            'decision_status': status, 'proposed_taxonomy_id': tid,
            'proposed_taxonomy_path': node['full_path'] if node else None,
            'final_taxonomy_id': tid if status == AUTO_APPROVED else None,
            'final_taxonomy_path': node['full_path'] if status == AUTO_APPROVED else None,
            'reviewed_taxonomy_id': tid if row['review_status'] == 'High confidence' else None,
            'ai_confidence': confidence, 'taxonomy_technical_validation': lookup,
            'optimizations.taxonomy_id': tid, 'optimizations.taxonomy_confidence': confidence,
            'optimizations.taxonomy_reason': row['reason'],
            'taxonomy_review': {'report': 'etsy_taxonomy_review_2026-09-12.json', 'confidence_score': row['confidence_score'], 'review_status': row['review_status'], 'reason': row['reason'], 'applied_at': now, 'user_authorized': True},
            'review_note': row['reason'], 'reviewed_at': now, 'updated_at': now,
        }
        if not independent_blocker:
            update.update({'validation_error_code': 'taxonomy_technical_validation_failed' if status == BLOCKED else None, 'validation_error': 'Seller taxonomy property lookup failed.' if status == BLOCKED else None, 'retryable': status == BLOCKED, 'recommended_action': 'Inspect the original product and confirm its category.' if status == REVIEW_REQUIRED else 'Retry Seller Taxonomy validation.' if status == BLOCKED else None})
        guard = {'_id': item['_id'], 'decision_status': item.get('decision_status'), 'creation_status': item.get('creation_status'), 'updated_at': item.get('updated_at')}
        result = await collection.update_one(guard, {'$set': update})
        assert result.matched_count == 1, f'Concurrent change for {row["sku"]}; backup: {backup}'
        results.append({'sku': row['sku'], 'taxonomy_id': tid, 'status': status, 'change': row['change'], 'independent_blocker_retained': independent_blocker})
    counts = await _refresh_publish_run_counts(RUN_ID)
    saved = await collection.find({'run_id': RUN_ID, 'sku': {'$in': skus}}).to_list(None)
    for item in saved:
        old = by_sku[item['sku']]
        assert item.get('creation_status') == old.get('creation_status')
        assert item.get('taxonomy_review', {}).get('user_authorized')
        assert item.get('proposed_taxonomy_id') == next(r['recommended_taxonomy_id'] for r in rows if r['sku'] == item['sku'])
    outcome = {'applied': len(results), 'statuses': dict(Counter(r['status'] for r in results)), 'changes': dict(Counter(r['change'] for r in results)), 'counts': counts, 'backup': str(backup), 'results': results}
    (ROOT / 'reports' / f'etsy_taxonomy_apply_result_{stamp}.json').write_text(json.dumps(outcome, indent=2))
    print(json.dumps({k: v for k, v in outcome.items() if k != 'results'}), flush=True)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    finally:
        close_mongo_client()
