'use strict';
const $ = id => document.getElementById(id);
const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const labels = {performer:'Strong seller', restock:'Check stock', interest:'Interest, no sales', review:'Review feedback', steady:'—'};
const notes = {performer:'This product sold at least 3 units in this period. Check its stock and consider what you can learn from its positioning.',restock:'This product is sold out, or it has recent sales and 2 or fewer units remaining. Check availability before planning promotion.',interest:'Net favorites grew by at least 3, with no sales in the selected period. Review price, shipping, photos, and product information.',review:'Average rating is below 4 across at least 3 available reviews. Read the reviews to identify specific concerns.',steady:'No signal was flagged for this product. Review its metrics alongside comparable products and listing age.'};
let data = null, isDemo = false, page = 1, filtered = [], timer = null, loading = false, requestVersion = 0, attentionSignal = 'all';
let productSortKey = 'revenue', productSortDirection = 'desc', categorySortKey = 'median_days_to_sale', categorySortDirection = 'asc';
const fmt = n => Number(n || 0).toLocaleString();
const cash = (n, currency = $('currency').value) => {try {return new Intl.NumberFormat(undefined,{style:'currency',currency,maximumFractionDigits:2}).format(n || 0);} catch {return `${currency} ${Number(n || 0).toFixed(2)}`;}};
const signed = n => n == null ? '—' : `${n > 0 ? '+' : ''}${fmt(n)}`;
const age = days => days == null ? '—' : days === 0 ? 'Listed today' : `${fmt(days)} day${days === 1 ? '' : 's'}`;
const duration = days => days == null ? '—' : `${Number(days).toLocaleString(undefined,{maximumFractionDigits:1})} day${Number(days) === 1 ? '' : 's'}`;
const ratio = value => value == null ? '—' : Number(value).toLocaleString(undefined,{maximumFractionDigits:1});
const saleDate = value => value ? new Date(value).toLocaleDateString(undefined,{month:'short',day:'numeric',year:'numeric'}) : null;
function compareValues(left, right, direction) {
  const leftMissing=left==null||Number.isNaN(left),rightMissing=right==null||Number.isNaN(right);
  if(leftMissing||rightMissing)return leftMissing===rightMissing?0:leftMissing?1:-1;
  const comparison=typeof left==='string'?left.localeCompare(right):left-right;
  return direction==='asc'?comparison:-comparison;
}
function updateSortHeaders(attribute, key, direction) {
  document.querySelectorAll(`[${attribute}]`).forEach(button=>{
    const active=button.getAttribute(attribute)===key;
    button.classList.toggle('active',active);
    button.parentElement.setAttribute('aria-sort',active?(direction==='asc'?'ascending':'descending'):'none');
    button.querySelector('span').textContent=active?(direction==='asc'?'↑':'↓'):'↕';
  });
}
function lifecycle(product) {
  if(Number(product.units||0)>0){
    const sold=saleDate(product.sold_at);
    return `<span class="lifecycle-sold">${sold?`Sold ${esc(sold)}`:'Sold in period'}</span>${product.days_to_sale==null?'':`<small class="delta">After ${esc(duration(product.days_to_sale))}</small>`}`;
  }
  if(product.state==='active')return `${age(product.days_listed)}${product.listed_at?`<small class="delta">Since ${esc(new Date(product.listed_at).toLocaleDateString())}</small>`:''}`;
  return `<span>${esc(String(product.state||'inactive').replace('_',' '))}</span>${product.listed_at?`<small class="delta">Listed ${esc(new Date(product.listed_at).toLocaleDateString())}</small>`:''}`;
}
function derivedCategoryPerformance(products, days) {
  const groups=new Map();
  products.forEach(product=>{
    const units=Math.max(0,Number(product.units||0));
    if(product.state!=='active'&&units<=0)return;
    const category=String(product.category_group||product.category||'Uncategorized').split(/:| > /)[0].trim()||'Uncategorized';
    const group=groups.get(category)||{ids:new Set(),sold:new Set(),units:0,ages:[],views:0,viewUnits:0};
    group.ids.add(String(product.listing_id));
    if(units>0){group.sold.add(String(product.listing_id));group.units+=units;const soldDays=product.days_to_sale;if(soldDays!=null)group.ages.push(Number(soldDays));const views=product.views_for_sale??product.views;if(views!=null){group.views+=Number(views);group.viewUnits+=units;}}
    groups.set(category,group);
  });
  return [...groups].map(([category,group])=>{group.ages.sort((a,b)=>a-b);const middle=Math.floor(group.ages.length/2);const median=group.ages.length?(group.ages.length%2?group.ages[middle]:(group.ages[middle-1]+group.ages[middle])/2):null;return {category,listing_count:group.ids.size,sold_listings:group.sold.size,sold_units:group.units,median_days_to_sale:median,sell_through_rate:group.ids.size?group.sold.size/group.ids.size:0,views_per_sale:group.viewUnits?group.views/group.viewUnits:null,period_days:days};}).sort((a,b)=>(a.median_days_to_sale??Infinity)-(b.median_days_to_sale??Infinity)||b.sell_through_rate-a.sell_through_rate||a.category.localeCompare(b.category));
}
function notice(message, demo = false) {$('notice').textContent = message; $('notice').hidden = !message; $('notice').classList.toggle('demo',demo);}
function shopId() {const id = $('shop').value.trim(); if (!/^\d{1,20}$/.test(id)) throw Error('Enter a numeric Etsy shop ID to load live insights.'); return id;}
async function api(path, options = {}) {const response = await fetch('/etsy-insights/api/' + path, options); if (response.status === 401) {location.href='/login?next=/etsy-insights'; throw Error('Sign in to access your shop insights.');} const result = await response.json(); if (!response.ok) throw Error(typeof result.detail === 'string' ? result.detail : 'The request could not be completed.'); return result;}
function busy(value) {loading=value; $('refresh').disabled=value; $('load').disabled=value; $('refresh').textContent=value?'↻ Refreshing…':'↻ Refresh Etsy data';}
async function load() {
  let id; try {id=shopId();} catch(error) {notice(error.message); return;}
  clearTimeout(timer); const version = ++requestVersion;
  if (isDemo || (data && (data.shop_id !== id || data.days !== Number($('period').value)))) {data=null; $('dashboard').hidden=true; $('empty').hidden=false;}
  isDemo=false; $('connection').textContent='○ Loading shop data';
  try {
    const result=await api(`dashboard?shop_id=${encodeURIComponent(id)}&days=${$('period').value}`);
    if (version !== requestVersion) return;
    data=result; page=1; attentionSignal='all';
    const running=result.status.status==='refreshing'; busy(running);
    if (result.empty) {$('empty').hidden=false; $('dashboard').hidden=true;} else render();
    $('connection').textContent=running?'◌ Refresh in progress':result.empty?'○ No snapshot yet':'● Saved Etsy data';
    if (running) {notice(result.status.progress+'. You can keep browsing your last completed snapshot.'); timer=setTimeout(load,4000);}
    else if (result.status.status==='error') notice(result.status.error);
    else if (result.empty) notice('No saved data for this shop. Refresh Etsy data to collect your first snapshot.');
    else {const stale=Date.now()-new Date(result.captured_at).getTime()>36*3600000?'This snapshot is over 36 hours old. Refresh for current data. ':'';notice(stale+(result.warnings || []).join(' '));}
  } catch(error) {
    if (version !== requestVersion) return;
    busy(false); notice(error.message); $('connection').textContent='○ Connection needs attention';
  }
}
async function refresh() {
  let id; try {id=shopId();} catch(error) {notice(error.message); return;}
  busy(true);
  try {await api(`refresh?shop_id=${encodeURIComponent(id)}`,{method:'POST'}); await load();}
  catch(error) {busy(false); notice(error.message);}
}
function render() {
  $('empty').hidden=true; $('dashboard').hidden=false;
  const currencies=new Set(Object.keys(data.summary.revenue));
  data.products.forEach(p=>currencies.add(p.currency));
  const selected=$('currency').value;
  $('currency').innerHTML=[...currencies].sort().map(c=>`<option value="${esc(c)}">${esc(c)}</option>`).join('') || '<option>USD</option>';
  if (currencies.has(selected)) $('currency').value=selected;
  const s=data.summary;
  $('scope').textContent=isDemo?'SAMPLE SHOP · DEMO DATA':'SHOP '+$('shop').value+' · PERFORMANCE';
  $('updated').textContent='Snapshot '+new Date(data.captured_at).toLocaleString();
  $('revenue').textContent=cash(s.revenue[$('currency').value]); $('orders').textContent=fmt(s.orders);
  $('favorites').textContent=fmt(s.favorites); $('active').textContent=fmt(s.active);
  const activeAges=data.products.filter(p=>p.state==='active'&&!Number(p.units||0)&&p.days_listed!=null).map(p=>p.days_listed).sort((a,b)=>a-b);
  const medianAge=activeAges.length?activeAges[Math.floor(activeAges.length/2)]:null;
  $('listing-age').textContent=medianAge==null?'Listing age unavailable':`Median ${age(medianAge).toLowerCase()}`;
  $('favorite-change').textContent=data.baseline_at?`${signed(s.favorite_change)} net since ${new Date(data.baseline_at).toLocaleDateString()}`:'Growth available after a baseline is saved';
  for (const signal of ['interest','review']) $('count-'+signal).textContent=data.products.filter(p=>p.signal===signal).length;
  drawChart(); renderCategories(); renderRows();
}
function drawChart() {
  const values=data.series.map(d=>d.revenue[$('currency').value] || 0), max=Math.max(1,...values), w=Math.max(320,$('chart').clientWidth),h=205,left=65,right=12,top=14,bottom=33;
  const x=i=>left+i*(w-left-right)/Math.max(1,values.length-1), y=v=>h-bottom-v/max*(h-top-bottom);
  const points=values.map((v,i)=>`${x(i).toFixed(2)},${y(v).toFixed(2)}`).join(' ');
  let svg=`<svg viewBox="0 0 ${w} ${h}" aria-hidden="true"><defs><linearGradient id="fill" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="#a7c1a1" stop-opacity=".45"/><stop offset="100%" stop-color="#d8e5d0" stop-opacity=".08"/></linearGradient></defs>`;
  for(let i=0;i<4;i++){const v=max*i/3; svg+=`<line x1="${left}" y1="${y(v)}" x2="${w-right}" y2="${y(v)}" stroke="#e8ede4" stroke-dasharray="3 5"/><text x="${left-12}" y="${y(v)+3}" text-anchor="end" font-family="system-ui" font-size="9" fill="#8c968d">${esc(cash(v))}</text>`;}
  svg+=`<polygon points="${left},${h-bottom} ${points} ${x(values.length-1)},${h-bottom}" fill="url(#fill)"/><polyline points="${points}" fill="none" stroke="#426c51" stroke-width="2.4" stroke-linejoin="round"/>`;
  values.forEach((v,i)=>{svg+=`<circle cx="${x(i)}" cy="${y(v)}" r="6" fill="transparent"><title>${esc(data.series[i].date)}: ${esc(cash(v))} · ${data.series[i].orders} orders</title></circle>`;});
  [...new Set([0,Math.floor(values.length/3),Math.floor(2*values.length/3),values.length-1])].forEach(i=>{const date=new Date(data.series[i].date+'T12:00:00Z');svg+=`<text x="${x(i)}" y="${h-9}" text-anchor="${i===0?'start':i===values.length-1?'end':'middle'}" fill="#8c968d" font-family="system-ui" font-size="9">${esc(date.toLocaleDateString(undefined,{month:'short',day:'numeric'}))}</text>`;});
  $('chart').innerHTML=svg+'</svg>'; $('chart').setAttribute('aria-label',`Daily gross product revenue in ${$('currency').value}. Total ${cash(values.reduce((a,b)=>a+b,0))}.`);
  $('chart-currency').textContent=$('currency').value; $('chart-range').textContent=`${data.series[0].date} — ${data.series.at(-1).date}`;
}
function renderCategories() {
  const categories=[...((data.category_performance&&data.category_performance.length)?data.category_performance:derivedCategoryPerformance(data.products,data.days))];
  categories.sort((left,right)=>compareValues(left[categorySortKey],right[categorySortKey],categorySortDirection));
  $('category-period').textContent=`${data.days}-day inventory cohort`;
  const sellThroughButton=$('sell-through-heading').querySelector('button');
  if(sellThroughButton)sellThroughButton.childNodes[0].nodeValue=`${data.days}-day sell-through `;
  updateSortHeaders('data-category-sort',categorySortKey,categorySortDirection);
  $('category-rows').innerHTML=categories.map(row=>`<tr><td>${esc(row.category)}</td><td><span class="ratio-value">${fmt(row.listing_count)}</span><small class="ratio-note">active + sold</small></td><td><span class="ratio-value">${fmt(row.sold_listings)}</span><small class="ratio-note">${fmt(row.sold_units)} unit${Number(row.sold_units)===1?'':'s'}</small></td><td><span class="ratio-value">${duration(row.median_days_to_sale)}</span></td><td><span class="ratio-value">${(Number(row.sell_through_rate||0)*100).toFixed(1)}%</span></td><td><span class="ratio-value">${ratio(row.views_per_sale)}</span><small class="ratio-note">views per sold unit</small></td></tr>`).join('')||'<tr><td colspan="6" class="category-empty">No category sales are available for this period.</td></tr>';
}
function renderRows() {
  if (!data || data.empty) return;
  const q=$('search').value.toLowerCase().trim(),state=$('state').value,currency=$('currency').value;
  filtered=data.products.filter(p=>(state==='all'||p.state===state)&&(attentionSignal==='all'||p.signal===attentionSignal)&&`${p.title} ${p.category_group||p.category||''} ${(p.tags||[]).join(' ')} ${p.listing_id}`.toLowerCase().includes(q));
  const productValue=product=>{
    if(productSortKey==='revenue')return product.revenue[currency]||0;
    if(productSortKey==='lifecycle_date')return new Date(product.sold_at||product.listed_at||0).getTime()||null;
    return product[productSortKey];
  };
  filtered.sort((left,right)=>compareValues(productValue(left),productValue(right),productSortDirection));
  updateSortHeaders('data-product-sort',productSortKey,productSortDirection);
  const pages=Math.max(1,Math.ceil(filtered.length/12)); page=Math.min(page,pages);
  const safeImage=url=>{try {return new URL(url).protocol==='https:'?url:null;} catch {return null;}};
  $('rows').innerHTML=filtered.slice((page-1)*12,page*12).map(p=>{const image=safeImage(p.image);return `<tr><td><div class="product-cell">${image?`<img class="product-image" src="${esc(image)}" alt="" loading="lazy" referrerpolicy="no-referrer">`:'<span class="product-image">◇</span>'}<div><button class="product-title" data-id="${p.listing_id}" title="${esc(p.title)}">${esc(p.title)}</button><small>${esc(p.state.replace('_',' '))} <span>·</span> ${esc(cash(p.price,p.currency))}${p.category_group||p.category?` · ${esc(p.category_group||p.category)}`:''}</small></div></div></td><td>${cash(p.revenue[currency])}</td><td>${p.views == null?'—':fmt(p.views)}${p.view_change==null?'':`<small class="delta">${signed(p.view_change)} since baseline</small>`}</td><td>${ratio(p.views_per_sale)}</td><td>${duration(p.days_to_sale)}</td><td>${fmt(p.favorites)}${p.favorite_change==null?'':`<small class="delta">${signed(p.favorite_change)} net</small>`}</td><td>${p.rating==null?'—':`<span style="color:#d5964b">★</span> ${p.rating.toFixed(1)} <small style="color:#91998f">(${p.review_count})</small>`}</td><td>${lifecycle(p)}</td></tr>`;}).join('') || '<tr><td colspan="8" style="text-align:center;padding:42px;color:#78827b">No products match these filters. Try another search or status.</td></tr>';
  $('result-count').textContent=`${fmt(filtered.length)} products${filtered.length?` · showing ${(page-1)*12+1}–${Math.min(page*12,filtered.length)}`:''}`;
  $('page').textContent=`${page} / ${pages}`; $('prev').disabled=page===1; $('next').disabled=page===pages;
}
function detail(id) {
  const p=data.products.find(p=>String(p.listing_id)===id); if(!p)return;
  const sold=Number(p.units||0)>0;
  $('detail-content').innerHTML=`<h2 style="font-size:23px;line-height:1.4">${esc(p.title)}</h2><p class="detail-note">Listing ${p.listing_id} · ${esc(p.state.replace('_',' '))}${p.category_group||p.category?' · '+esc(p.category_group||p.category):''}</p><div class="detail-grid"><div><small>Gross revenue · ${data.days} days</small><strong>${cash(p.revenue[$('currency').value])}</strong></div><div><small>Orders · ${data.days} days</small><strong>${p.orders}</strong></div><div><small>Views</small><strong>${p.views==null?'—':fmt(p.views)}</strong></div><div><small>Views / sale</small><strong>${ratio(p.views_per_sale)}</strong></div><div><small>Time to sell</small><strong>${duration(p.days_to_sale)}</strong></div><div><small>${sold?'Sold':'Listed for'}</small><strong>${sold?(saleDate(p.sold_at)||'In selected period'):age(p.days_listed)}</strong></div></div><p class="detail-note">${p.listed_at?'Originally listed '+esc(new Date(p.listed_at).toLocaleDateString())+'. ':''}${data.baseline_at?'Engagement baseline: '+esc(new Date(data.baseline_at).toLocaleString()):'No engagement baseline is available yet. Current views are still shown.'}</p><div class="tags">${(p.tags||[]).map(t=>`<span>${esc(t)}</span>`).join('')}</div>${isDemo?'<p class="detail-note">This is a sample product. No live listing is linked.</p>':`<a href="https://www.etsy.com/listing/${p.listing_id}" target="_blank" rel="noopener noreferrer">View product on Etsy ↗</a>`}`;
  $('detail').showModal();
}
function exportCsv() {
  const currency=$('currency').value;
  const rows=[['Listing ID','Title','Status','Category',`Gross revenue (${currency})`,'Orders','Views','Views per sale','Sold at','Days to sale','View change','Favorites counter','Net favorite change','Rating','Review count','Originally listed','Active listing age (days)','Snapshot','Baseline','Data mode'],...filtered.map(p=>[p.listing_id,p.title,p.state,p.category_group||p.category,p.revenue[currency]||0,p.orders,p.views,p.views_per_sale,p.sold_at,p.days_to_sale,p.view_change,p.favorites,p.favorite_change,p.rating,p.review_count,p.listed_at,p.state==='active'&&!Number(p.units||0)?p.days_listed:null,data.captured_at,data.baseline_at,isDemo?'DEMO':'LIVE'])];
  // Protect spreadsheet readers from formula execution in seller-controlled titles/tags.
  const cell=v=>{let s=String(v??'');if(typeof v==='string'&&/^[=+@\-\t\r]/.test(s))s="'"+s;return '"'+s.replaceAll('"','""')+'"';};
  const url=URL.createObjectURL(new Blob(['\ufeff'+rows.map(r=>r.map(cell).join(',')).join('\r\n')],{type:'text/csv;charset=utf-8'}));
  const a=document.createElement('a');a.href=url;a.download=`etsy-insights-${isDemo?'demo':$('shop').value}-${data.days}d.csv`;document.body.appendChild(a);a.click();a.remove();setTimeout(()=>URL.revokeObjectURL(url),1000);
}
function demo() {
  clearTimeout(timer);++requestVersion;busy(false);isDemo=true;page=1;attentionSignal='all';
  const days=Number($('period').value), now=new Date(),baseline=new Date(now.getTime()-days*86400000);
  const names=['Vintage brass candlestick pair','Hand-thrown ceramic vase','Linen cushion cover · oatmeal','Antique botanical print set','Walnut jewelry box','Woven market basket','Stoneware espresso cup set','Vintage amber glass bottle','Solid brass drawer pulls','Handwoven cotton throw','Art deco picture frame','Ceramic incense holder','Vintage wool area rug','Pressed flower wall art','Oak floating shelf','Vintage silver serving tray'];
  const palette=['#ded3be','#c4b9a2','#e2d9c8','#c1c8b3','#b4a087','#d7c7a5','#a6b9af','#bc956d'],categories=['Home & Living','Art & Collectibles','Home & Living','Art & Collectibles','Accessories','Home & Living','Home & Living','Art & Collectibles'];
  const products=names.map((title,i)=>{const units=i===4||i===7||i===11?0:Math.round((17-i)*days/30),price=[42,68,29,36,85,48,34,24,18,95,56,22,165,39,78,64][i],favorites=29+i*13,quantity=i===0?2:i===2?0:3+i*2;
    const days_listed=14+i*19,listed_at=new Date(now.getTime()-days_listed*86400000).toISOString();
    const views=340+i*123,days_to_sale=units?Math.min(days_listed,8+i*4):null,sold_at=days_to_sale==null?null:new Date(new Date(listed_at).getTime()+days_to_sale*86400000).toISOString();
    return {listing_id:100000+i,title,state:i===2?'sold_out':i===14?'inactive':'active',quantity,price,currency:'USD',image:null,tags:['vintage','home decor',i%2?'handmade':'collectible'],category:categories[i%categories.length],category_group:categories[i%categories.length],favorites,views,units,orders:Math.ceil(units*.8),revenue:{USD:units*price},favorite_change:i===4?18:i===7?9:i===11?5:i%5===0?-2:3+i,view_change:32+i*17,rating:i===6?3.7:4.5+(i%3)*.2,review_count:6+i*2,signal:i===4||i===7||i===11?'interest':i===6?'review':units>=3?'performer':'steady',days_listed,listed_at,sold_at,days_to_sale,views_for_sale:units?views:null,views_per_sale:units?views/units:null,color:palette[i%8]};});
  const total=products.reduce((sum,p)=>sum+p.revenue.USD,0),weights=Array.from({length:days},(_,i)=>Math.max(.2,1+Math.sin(i*1.7)*.45+(i%7===5?.6:0))),weight=weights.reduce((a,b)=>a+b,0);
  data={empty:false,days,captured_at:now.toISOString(),baseline_at:baseline.toISOString(),products,category_performance:derivedCategoryPerformance(products,days),series:weights.map((v,i)=>({date:new Date(now.getTime()-(days-1-i)*86400000).toISOString().slice(0,10),revenue:{USD:total*v/weight},orders:Math.round(v*4),units:Math.round(v*5)})),summary:{revenue:{USD:total},orders:products.reduce((a,p)=>a+p.orders,0),units:products.reduce((a,p)=>a+p.units,0),favorites:products.reduce((a,p)=>a+p.favorites,0),favorite_change:products.reduce((a,p)=>a+p.favorite_change,0),active:products.filter(p=>p.state==='active').length,restock:2}};
  $('connection').textContent='● Demo workspace';notice('DEMO DATA · Sample products and metrics for exploring the dashboard. Enter your shop ID and load your shop to view real Etsy data.',true);render();
}
window.addEventListener('resize',()=>{if(data&&!data.empty)drawChart();});
$('refresh').addEventListener('click',refresh);$('load').addEventListener('click',load);$('shop').addEventListener('keydown',e=>{if(e.key==='Enter')load();});
$('demo').addEventListener('click',demo);$('empty-demo').addEventListener('click',demo);
$('period').addEventListener('change',()=>isDemo?demo():$('shop').value.trim()?load():null);
$('currency').addEventListener('change',()=>{if(data&&!data.empty){page=1;drawChart();$('revenue').textContent=cash(data.summary.revenue[$('currency').value]);renderRows();}});
for(const id of ['search','state'])$(id).addEventListener(id==='search'?'input':'change',()=>{attentionSignal='all';page=1;renderRows();});
$('sort').addEventListener('change',()=>{productSortKey=$('sort').value;productSortDirection=$('sort-direction').value;page=1;renderRows();});
$('sort-direction').addEventListener('change',()=>{productSortDirection=$('sort-direction').value;page=1;renderRows();});
for(const button of document.querySelectorAll('[data-product-sort]'))button.addEventListener('click',()=>{const key=button.dataset.productSort;productSortDirection=productSortKey===key?(productSortDirection==='asc'?'desc':'asc'):(key==='title'?'asc':'desc');productSortKey=key;if([...$('sort').options].some(option=>option.value===key))$('sort').value=key;$('sort-direction').value=productSortDirection;page=1;renderRows();});
for(const button of document.querySelectorAll('[data-category-sort]'))button.addEventListener('click',()=>{const key=button.dataset.categorySort;categorySortDirection=categorySortKey===key?(categorySortDirection==='asc'?'desc':'asc'):(key==='category'?'asc':'desc');categorySortKey=key;renderCategories();});
$('prev').addEventListener('click',()=>{page--;renderRows();});$('next').addEventListener('click',()=>{page++;renderRows();});
$('rows').addEventListener('click',e=>{const button=e.target.closest('[data-id]');if(button)detail(button.dataset.id);});
for(const button of document.querySelectorAll('[data-signal]'))button.addEventListener('click',()=>{$('state').value='all';$('search').value='';attentionSignal=button.dataset.signal;page=1;renderRows();document.querySelector('.products').scrollIntoView({behavior:'smooth',block:'start'});});
$('export').addEventListener('click',exportCsv);$('close-detail').addEventListener('click',()=>$('detail').close());
$('detail').addEventListener('click',e=>{if(e.target===$('detail')){const r=$('detail').getBoundingClientRect();if(e.clientX<r.left||e.clientX>r.right||e.clientY<r.top||e.clientY>r.bottom)$('detail').close();}});
(async()=>{if(new URLSearchParams(location.search).get('demo')==='1'){demo();return;}try{const shops=await api('shops');if(shops.shop_ids.length){$('shop').value=shops.shop_ids[0];await load();}}catch(error){notice(error.message);}})();
