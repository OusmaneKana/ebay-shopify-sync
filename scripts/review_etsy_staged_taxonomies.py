"""Assemble offline review reports. No network calls or saved taxonomy changes."""
import argparse,csv,json,re,sys
from collections import Counter
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from app.services.etsy_taxonomy_matching import object_tokens
from app.services.etsy_taxonomy_validation import flatten_taxonomy_tree,taxonomy_tree_hash

def signature(title):
    patterns={'plate':r'\bplates?\b','vase':r'\bvases?\b','planter':r'planter|cachepot','bowl':r'\bbowls?\b|tureen','necklace':r'\bnecklace\b','watch':r'\bwatch\b','camera':r'\bcamera\b','ring':r'\bring\b','bracelet':r'\bbracelet\b','earring':r'\bearrings?\b','figure':r'figurine|statue|sculpture|carving','lighter':r'\blighter\b','mirror':r'\bmirror\b','lamp':r'\blamps?\b','book':r'\bbook\b|franklin library','photo':r'\bphoto(?:graph)?\b','ball':r'\bfootball\b','ticket':r'\bticket\b|program','scarf':r'\bscarf\b','box':r'\bbox\b','case':r'\bcase\b','mineral':r'cluster|quartz|geode','print':r'print|lithograph|engraving|serigraph|etching','shaker':r'shakers?','basket':r'\bbasket\b','sunglasses':r'\bsunglasses\b'}
    return {name for name,pattern in patterns.items() if re.search(pattern,title,re.I)}

def compatible_reference(tid,title,reference):
    text=reference.lower()
    if tid==130 and re.search(r'lamp|lampshade',text):return False
    if tid==1197 and not re.search(r'charm bracelet|pandora',text):return False
    if tid==1194 and ('bangle' not in text or 'cuff' in text):return False
    if tid==1196 and not re.search(r'chain|link',text):return False
    if tid==1866 and re.search(r'vesta|match safe',title,re.I) and not re.search(r'vesta|match safe',text):return False
    return True

def build_report(source,decisions):
    index={n['taxonomy_id']:n for n in flatten_taxonomy_tree(source['nodes'])}
    expected={i['sku']for i in source['items']}
    assert len(decisions)==len(expected) and {d['sku']for d in decisions}==expected
    assert all(i['decision_status']!='EXCLUDED' and i['creation_status']not in ['created','excluded']for i in source['items'])
    by_sku={d['sku']:d for d in decisions};rows=[]
    for item in source['items']:
        d=by_sku[item['sku']];tid=d['taxonomy_id'];assert tid is None or tid in index,f'Invalid taxonomy: {tid}'
        assert 0<=d['confidence']<=1
        facts=item['fresh_inputs'];old=item.get('final_taxonomy_id')or item.get('proposed_taxonomy_id');node=index.get(tid)
        title=facts['original_title'];sig=signature(title)
        # Same physical object AND the independently recommended taxonomy.
        refs=[r for r in source['listings'] if int(r.get('taxonomy_id')or 0)==tid and sig & signature(r.get('title','')) and compatible_reference(tid,title,r.get('title',''))]
        ref=max(refs,key=lambda r:len(object_tokens(title)&object_tokens(r['title'])),default=None)
        current=(index.get(old)or{}).get('full_path')or item.get('proposed_taxonomy_path')or 'Unrecognized / not selected'
        high=tid is not None and d['confidence']>=.90 and not d['ambiguous']
        rows.append({'sku':item['sku'],'title':title,'object_type':d['object_type'],'current_taxonomy_id':old,'current_taxonomy':current,'recommended_taxonomy_id':tid,'recommended_taxonomy':node['full_path']if node else 'Unresolved — inspect actual object','confidence_score':round(d['confidence']*100),'review_status':'High confidence'if high else 'Review required','change':'Unresolved'if tid is None else 'Keep category'if tid==old else 'Change category','reason':d['reason'],'alternative_taxonomy_id':2850 if tid==1049 else None,'alternative_taxonomy':index[2850]['full_path']if tid==1049 else '', 'reference_listing_id':ref['listing_id']if ref else None,'reference_title':ref['title']if ref else 'No verified comparable with the recommended taxonomy','reference_taxonomy':node['full_path']if ref else '', 'reference_url':'https://www.etsy.com/listing/'+str(ref['listing_id'])if ref else '', 'saved_batch_status':item['decision_status'],'source_facts':facts})
    summary={'reviewed':len(rows),'review_statuses':dict(Counter(r['review_status']for r in rows)),'changes':dict(Counter(r['change']for r in rows)),'verified_references':sum(r['reference_listing_id']is not None for r in rows),'seller_taxonomy_nodes':len(index),'seller_tree_hash':taxonomy_tree_hash(source['nodes']),'confidence_definition':'Estimated classification certainty on a 0–100 scale. Not measured accuracy or Etsy approval probability.','scope':'Uncreated, non-excluded items in the current staged run. Source facts reviewed locally; no saved taxonomy decisions changed.'}
    return {'summary':summary,'rows':rows}

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--source',required=True);parser.add_argument('--decisions',required=True);parser.add_argument('--output',required=True);args=parser.parse_args()
    report=build_report(json.loads(Path(args.source).read_text()),json.loads(Path(args.decisions).read_text()));output=Path(args.output);output.parent.mkdir(parents=True,exist_ok=True)
    output.with_suffix('.json').write_text(json.dumps(report,indent=2,ensure_ascii=False));fields=[k for k in report['rows'][0]if k!='source_facts']
    with output.with_suffix('.csv').open('w',newline='')as handle:
        writer=csv.DictWriter(handle,fieldnames=fields);writer.writeheader();writer.writerows({k:r[k]for k in fields}for r in report['rows'])
    print(json.dumps(report['summary']))
