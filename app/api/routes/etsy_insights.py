import asyncio
import os
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from app.database.mongo import db
from app.security.passkey import require_authorized
from app.services.etsy_insights import acquire_refresh, build_dashboard, refresh, taxonomy_category_index, window_start

router = APIRouter(dependencies=[Depends(require_authorized)])


@router.get('/shops')
async def shops():
    try:
        ids = await asyncio.wait_for(db.etsy_insight_snapshots.distinct('shop_id'), timeout=8)
        existing = await asyncio.wait_for(db.etsy_listings_investigation.distinct('shop_id'), timeout=8)
        ids.extend(str(s) for s in existing if s)
        configured = os.getenv('ETSY_SHOP_ID')
        if configured:
            ids.append(configured)
        return {'shop_ids': sorted({str(s) for s in ids if str(s).isdigit()})}
    except Exception:
        raise HTTPException(503, 'Database unavailable. Check the app database connection.')


@router.get('/dashboard')
async def dashboard(shop_id: str = Query(pattern=r'^\d{1,20}$'), days: int = Query(30, ge=1, le=90)):
    try:
        latest = await asyncio.wait_for(db.etsy_insight_snapshots.find_one({'shop_id': shop_id}, sort=[('captured_at', -1)]), 8)
        status = await asyncio.wait_for(db.etsy_insight_status.find_one({'_id': shop_id}, {'_id': 0, 'lease_until': 0}), 8)
        if not latest:
            return {'empty': True, 'status': status or {'status': 'empty'}}
        # Older Insight snapshots predate taxonomy capture.  Reuse the local
        # listing investigation and saved seller taxonomy so category metrics are
        # useful immediately, without requiring another Etsy API refresh.
        try:
            missing_ids = [
                product.get('listing_id')
                for product in latest.get('products', [])
                if product.get('listing_id') and not product.get('category')
            ]
            if missing_ids:
                taxonomy_snapshot = await asyncio.wait_for(
                    db.etsy_seller_taxonomy_snapshots.find_one(
                        {'active': True}, sort=[('fetched_at', -1)]
                    ),
                    8,
                )
                taxonomy_index = taxonomy_category_index((taxonomy_snapshot or {}).get('nodes', []))
                rows = await asyncio.wait_for(
                    db.etsy_listings_investigation.find(
                        {'listing_id': {'$in': missing_ids}},
                        {'listing_id': 1, 'taxonomy_id': 1},
                    ).to_list(length=10000),
                    8,
                )
                listing_taxonomy = {str(row.get('listing_id')): row.get('taxonomy_id') for row in rows}
                for product in latest.get('products', []):
                    taxonomy_id = product.get('taxonomy_id') or listing_taxonomy.get(str(product.get('listing_id')))
                    try:
                        taxonomy = taxonomy_index.get(int(taxonomy_id)) if taxonomy_id is not None else None
                    except (TypeError, ValueError):
                        taxonomy = None
                    if taxonomy:
                        product.update(taxonomy_id=taxonomy_id, **taxonomy)
        except Exception:
            # Categories are an enhancement; never hide otherwise valid Insights
            # if the local taxonomy cache is temporarily unavailable.
            pass
        target = window_start(days).isoformat()
        baseline = await asyncio.wait_for(db.etsy_insight_snapshots.find_one({'shop_id': shop_id, 'captured_at': {'$lte': target}}, sort=[('captured_at', -1)]), 8)
        result = build_dashboard(latest, baseline, days)
        result.update(empty=False, status=status or {'status': 'ready'}, shop_id=shop_id)
        return result
    except Exception:
        raise HTTPException(503, 'Insights could not be loaded. Check the app database connection.')


@router.post('/refresh', status_code=202)
async def refresh_dashboard(tasks: BackgroundTasks, shop_id: str = Query(pattern=r'^\d{1,20}$')):
    try:
        lease = await asyncio.wait_for(acquire_refresh(shop_id), 8)
    except Exception:
        raise HTTPException(503, 'Refresh could not start. Check the app database connection.')
    if not lease:
        raise HTTPException(409, 'This shop is already refreshing.')
    tasks.add_task(refresh, shop_id)
    return {'status': 'refreshing'}
