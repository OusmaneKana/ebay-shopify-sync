'use strict';

const $ = id => document.getElementById(id);
const esc = value => String(value ?? '').replace(/[&<>"']/g, char => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[char]));
const signalLabels = {ad_waste:'Ad spend, no sales',conversion:'Traffic, no sales',stale:'Long-listed',performer:'Strong performer',steady:'No alert'};
let data = null;
let isDemo = false;
let page = 1;
let filtered = [];
let timer = null;
let requestVersion = 0;
let attentionSignal = 'all';
let productSortKey = 'revenue', productSortDirection = 'desc', categorySortKey = 'median_days_to_sale', categorySortDirection = 'asc';

const fmt = value => Number(value || 0).toLocaleString(undefined,{maximumFractionDigits:0});
const decimal = (value, digits=1) => Number(value || 0).toLocaleString(undefined,{minimumFractionDigits:digits,maximumFractionDigits:digits});
const percent = value => `${decimal(Number(value || 0) * 100, 1)}%`;
const cash = (value, currency = $('currency').value) => {
  try { return new Intl.NumberFormat(undefined,{style:'currency',currency,maximumFractionDigits:2}).format(value || 0); }
  catch { return `${currency} ${Number(value || 0).toFixed(2)}`; }
};
const age = days => days == null ? '—' : days === 0 ? 'Listed today' : `${fmt(days)} day${days === 1 ? '' : 's'}`;
const duration = days => days == null ? '—' : `${Number(days).toLocaleString(undefined,{maximumFractionDigits:1})} day${Number(days) === 1 ? '' : 's'}`;
const ratio = value => value == null ? '—' : Number(value).toLocaleString(undefined,{maximumFractionDigits:1});
const saleDate = value => value ? new Date(value).toLocaleDateString(undefined,{month:'short',day:'numeric',year:'numeric'}) : null;
const available = source => Boolean(data?.sources?.[source]);
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
  return `<span>${esc(String(product.state||'ended').replace('_',' '))}</span>${product.listed_at?`<small class="delta">Listed ${esc(new Date(product.listed_at).toLocaleDateString())}</small>`:''}`;
}
function derivedCategoryPerformance(products, days) {
  const groups=new Map();
  products.forEach(product=>{
    const units=Math.max(0,Number(product.units||0));
    if(product.state!=='active'&&units<=0)return;
    const category=String(product.category_group||product.category||'Uncategorized').split(/:| > /)[0].trim()||'Uncategorized';
    const group=groups.get(category)||{ids:new Set(),sold:new Set(),units:0,ages:[],views:0,viewUnits:0};
    group.ids.add(String(product.listing_id));
    if(units>0){group.sold.add(String(product.listing_id));group.units+=units;const soldDays=product.days_to_sale;if(soldDays!=null)group.ages.push(Number(soldDays));const views=product.views_for_sale??product.organic?.views;if(views!=null){group.views+=Number(views);group.viewUnits+=units;}}
    groups.set(category,group);
  });
  return [...groups].map(([category,group])=>{group.ages.sort((a,b)=>a-b);const middle=Math.floor(group.ages.length/2);const median=group.ages.length?(group.ages.length%2?group.ages[middle]:(group.ages[middle-1]+group.ages[middle])/2):null;return {category,listing_count:group.ids.size,sold_listings:group.sold.size,sold_units:group.units,median_days_to_sale:median,sell_through_rate:group.ids.size?group.sold.size/group.ids.size:0,views_per_sale:group.viewUnits?group.views/group.viewUnits:null,period_days:days};}).sort((a,b)=>(a.median_days_to_sale??Infinity)-(b.median_days_to_sale??Infinity)||b.sell_through_rate-a.sell_through_rate||a.category.localeCompare(b.category));
}
function notice(message, demo=false) {$('notice').textContent=message || '';$('notice').hidden=!message;$('notice').classList.toggle('demo',demo);}
async function api(path, options={}) {
  const response=await fetch('/ebay-insights/api/'+path,options);
  if(response.status===401){location.href='/login?next=/ebay-insights';throw Error('Sign in to access eBay Intelligence.');}
  const result=await response.json();
  if(!response.ok)throw Error(typeof result.detail==='string'?result.detail:'The request could not be completed.');
  return result;
}
function query(){return `marketplace_id=${encodeURIComponent($('marketplace').value)}&days=${encodeURIComponent($('period').value)}`;}
function busy(value){$('refresh').disabled=value;$('refresh').textContent=value?'↻ Refreshing…':'↻ Refresh eBay data';}

async function load(){
  clearTimeout(timer);
  const version=++requestVersion;
  isDemo=false;
  $('connection').textContent='○ Loading eBay data';
  try{
    const result=await api('dashboard?'+query());
    if(version!==requestVersion)return;
    data=result;page=1;attentionSignal='all';
    const running=result.status?.status==='refreshing';
    busy(running);
    if(result.empty){$('empty').hidden=false;$('dashboard').hidden=true;}else render();
    $('connection').textContent=running?'◌ Refresh in progress':result.empty?'○ No snapshot yet':'● Saved eBay data';
    if(running){notice(`${result.status.progress || 'Refresh in progress'}. You can keep browsing the last completed snapshot.`);timer=setTimeout(load,5000);}
    else if(result.status?.status==='error')notice(result.status.error);
    else if(result.empty)notice('No saved data for this period. Refresh eBay data to create it.');
    else{
      const stale=Date.now()-new Date(result.captured_at).getTime()>36*3600000?'This snapshot is over 36 hours old. ':'';
      notice(stale+(result.warnings||[]).join(' '));
    }
  }catch(error){if(version!==requestVersion)return;busy(false);notice(error.message);$('connection').textContent='○ Connection needs attention';}
}

async function refresh(){
  busy(true);
  try{await api('refresh?'+query(),{method:'POST'});await load();}
  catch(error){busy(false);notice(error.message);}
}

function render(){
  $('empty').hidden=true;$('dashboard').hidden=false;
  const currencies=new Set([...Object.keys(data.summary.revenue||{}),...Object.keys(data.summary.ad_fees||{}),...Object.keys(data.summary.ad_sales||{})]);
  data.products.forEach(product=>currencies.add(product.currency));
  const selected=$('currency').value;
  $('currency').innerHTML=[...currencies].filter(Boolean).sort().map(currency=>`<option value="${esc(currency)}">${esc(currency)}</option>`).join('')||'<option>USD</option>';
  if(currencies.has(selected))$('currency').value=selected;
  const currency=$('currency').value,s=data.summary;
  $('scope').textContent=isDemo?'SAMPLE STORE · DEMO DATA':`${data.marketplace_id} · ${data.days}-DAY PERFORMANCE`;
  $('updated').textContent='Snapshot '+new Date(data.captured_at).toLocaleString();
  $('revenue').textContent=available('orders')?cash(s.revenue[currency],currency):'—';
  $('orders').textContent=available('orders')?fmt(s.orders):'—';
  $('impressions').textContent=available('traffic')?fmt(s.views):'—';
  $('ad-spend').textContent=available('ads')?cash(s.ad_fees[currency],currency):'—';
  $('revenue-note').textContent=available('orders')?`${fmt(s.units)} units sold`:'Order data unavailable';
  $('orders-note').textContent=available('orders')?`${cash(s.revenue[currency],currency)} gross product revenue`:'Order data unavailable';
  $('traffic-note').textContent=available('traffic')?`${fmt(s.impressions)} impressions behind these views`:'Analytics permission required';
  $('ad-note').textContent=available('ads')?`${cash(s.ad_sales[currency],currency)} attributed sales · ${decimal(s.ad_roas,2)}× ROAS`:'Marketing permission required';
  for(const signal of ['ad_waste','conversion','stale'])$('count-'+signal).textContent=data.products.filter(product=>product.signal===signal).length;
  drawChart();renderCategories();renderRows();
}

function drawChart(){
  const currency=$('currency').value;
  const values=data.series.map(day=>day.revenue[currency]||0);
  const max=Math.max(1,...values),width=Math.max(320,$('chart').clientWidth),height=205,left=65,right=12,top=14,bottom=33;
  const x=index=>left+index*(width-left-right)/Math.max(1,values.length-1);
  const y=value=>height-bottom-value/max*(height-top-bottom);
  const points=values.map((value,index)=>`${x(index).toFixed(2)},${y(value).toFixed(2)}`).join(' ');
  let svg=`<svg viewBox="0 0 ${width} ${height}" aria-hidden="true"><defs><linearGradient id="ebay-fill" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="#7094f7" stop-opacity=".42"/><stop offset="100%" stop-color="#dce5ff" stop-opacity=".08"/></linearGradient></defs>`;
  for(let index=0;index<4;index++){const value=max*index/3;svg+=`<line x1="${left}" y1="${y(value)}" x2="${width-right}" y2="${y(value)}" stroke="#e8ede4" stroke-dasharray="3 5"/><text x="${left-12}" y="${y(value)+3}" text-anchor="end" font-family="system-ui" font-size="9" fill="#8c968d">${esc(cash(value,currency))}</text>`;}
  svg+=`<polygon points="${left},${height-bottom} ${points} ${x(values.length-1)},${height-bottom}" fill="url(#ebay-fill)"/><polyline points="${points}" fill="none" stroke="#3665f3" stroke-width="2.4" stroke-linejoin="round"/>`;
  values.forEach((value,index)=>{const day=data.series[index];svg+=`<circle cx="${x(index)}" cy="${y(value)}" r="6" fill="transparent"><title>${esc(day.date)}: ${esc(cash(value,currency))} · ${day.orders} orders · ${fmt(day.views)} views</title></circle>`;});
  [...new Set([0,Math.floor(values.length/3),Math.floor(2*values.length/3),values.length-1])].forEach(index=>{const date=new Date(data.series[index].date+'T12:00:00Z');svg+=`<text x="${x(index)}" y="${height-9}" text-anchor="${index===0?'start':index===values.length-1?'end':'middle'}" fill="#8c968d" font-family="system-ui" font-size="9">${esc(date.toLocaleDateString(undefined,{month:'short',day:'numeric'}))}</text>`;});
  $('chart').innerHTML=svg+'</svg>';
  $('chart').setAttribute('aria-label',`Daily eBay revenue in ${currency}. Total ${cash(values.reduce((a,b)=>a+b,0),currency)}.`);
  $('chart-currency').textContent=currency;$('chart-range').textContent=`${data.series[0].date} — ${data.series[data.series.length-1].date}`;
}

function sortValue(product, sort, currency){
  if(sort==='revenue')return product.revenue[currency]||0;
  if(sort==='impressions')return product.organic.impressions||0;
  if(sort==='views')return product.organic.views||0;
  if(sort==='ad_fees')return product.ads.fees||0;
  if(sort==='ad_roas')return product.ads.roas||0;
  if(sort==='lifecycle_date')return new Date(product.sold_at||product.listed_at||0).getTime()||null;
  return product[sort]??null;
}

function renderCategories(){
  const categories=[...((data.category_performance&&data.category_performance.length)?data.category_performance:derivedCategoryPerformance(data.products,data.days))];
  categories.sort((left,right)=>compareValues(left[categorySortKey],right[categorySortKey],categorySortDirection));
  $('category-period').textContent=`${data.days}-day inventory cohort`;
  const sellThroughButton=$('sell-through-heading').querySelector('button');
  if(sellThroughButton)sellThroughButton.childNodes[0].nodeValue=`${data.days}-day sell-through `;
  updateSortHeaders('data-category-sort',categorySortKey,categorySortDirection);
  $('category-rows').innerHTML=categories.map(row=>`<tr><td>${esc(row.category)}</td><td><span class="ratio-value">${fmt(row.listing_count)}</span><small class="ratio-note">active + sold</small></td><td><span class="ratio-value">${fmt(row.sold_listings)}</span><small class="ratio-note">${fmt(row.sold_units)} unit${Number(row.sold_units)===1?'':'s'}</small></td><td><span class="ratio-value">${duration(row.median_days_to_sale)}</span></td><td><span class="ratio-value">${percent(row.sell_through_rate)}</span></td><td><span class="ratio-value">${ratio(row.views_per_sale)}</span><small class="ratio-note">views per sold unit</small></td></tr>`).join('')||'<tr><td colspan="6" class="category-empty">No category sales are available for this period.</td></tr>';
}

function renderRows(){
  if(!data||data.empty)return;
  const search=$('search').value.toLowerCase().trim(),state=$('state').value,adFilter=$('ad-filter').value,currency=$('currency').value;
  filtered=data.products.filter(product=>(state==='all'||product.state===state)&&(adFilter==='all'||(adFilter==='advertised')===Boolean(product.ads.advertised))&&(attentionSignal==='all'||product.signal===attentionSignal)&&`${product.title} ${product.sku||''} ${product.category||''} ${product.listing_id}`.toLowerCase().includes(search));
  filtered.sort((left,right)=>compareValues(sortValue(left,productSortKey,currency),sortValue(right,productSortKey,currency),productSortDirection));
  updateSortHeaders('data-product-sort',productSortKey,productSortDirection);
  const pages=Math.max(1,Math.ceil(filtered.length/15));page=Math.min(page,pages);
  const safeImage=url=>{try{return new URL(url).protocol==='https:'?url:null;}catch{return null;}};
  $('rows').innerHTML=filtered.slice((page-1)*15,page*15).map(product=>{
    const image=safeImage(product.image),traffic=product.organic,ads=product.ads;
    const views=traffic.available?`<div class="metric-stack"><strong>${fmt(traffic.views)}</strong><small>${data.days}-day views</small></div>`:'<span class="muted-dash">—</span>';
    const advertising=!ads.available?'<span class="muted-dash">—</span>':ads.advertised?`<div class="metric-stack"><span class="ad-pill">Promoted${ads.ad_status?' · '+esc(String(ads.ad_status).toLowerCase()):''}</span><strong>${cash(ads.fees,currency)} fees</strong><small>${cash(ads.sale_amount,currency)} sales · ${decimal(ads.roas,2)}× ROAS</small></div>`:'<span class="muted-dash">Not promoted</span>';
    return `<tr><td><div class="product-cell">${image?`<img class="product-image" src="${esc(image)}" alt="" loading="lazy" referrerpolicy="no-referrer">`:'<span class="product-image">◇</span>'}<div><button class="product-title" data-id="${esc(product.listing_id)}" title="${esc(product.title)}">${esc(product.title)}</button><small>${esc(product.state.replace('_',' '))} · ${esc(product.sku||product.listing_id)} · ${esc(cash(product.price,product.currency))}${product.category_group||product.category?` · ${esc(product.category_group||product.category)}`:''}</small><span class="badge ${esc(product.signal)}">${esc(signalLabels[product.signal]||'No alert')}</span></div></div></td><td><div class="metric-stack"><strong>${available('orders')?cash(product.revenue[currency],currency):'—'}</strong><small>${available('orders')?`${fmt(product.units)} units · ${fmt(product.orders)} orders`:'Orders unavailable'}</small></div></td><td>${views}</td><td>${traffic.available?ratio(product.views_per_sale):'—'}</td><td>${duration(product.days_to_sale)}</td><td>${advertising}</td><td>${lifecycle(product)}</td></tr>`;
  }).join('')||'<tr><td colspan="7" style="text-align:center;padding:42px;color:#78827b">No listings match these filters.</td></tr>';
  $('result-count').textContent=`${fmt(filtered.length)} listings${filtered.length?` · showing ${(page-1)*15+1}–${Math.min(page*15,filtered.length)}`:''}`;
  $('page').textContent=`${page} / ${pages}`;$('prev').disabled=page===1;$('next').disabled=page===pages;
}

function detail(id){
  const product=data.products.find(row=>String(row.listing_id)===id);if(!product)return;
  const traffic=product.organic,ads=product.ads,currency=$('currency').value;
  const campaigns=(ads.campaigns||[]).map(campaign=>`<span>${esc(campaign.campaign_name||campaign.campaign_id||'Campaign')}${campaign.campaign_status?' · '+esc(String(campaign.campaign_status).toLowerCase()):''}</span>`).join('');
  const sold=Number(product.units||0)>0;
  $('detail-content').innerHTML=`<h2 style="font-size:23px;line-height:1.4">${esc(product.title)}</h2><p class="detail-note">Listing ${esc(product.listing_id)} · ${esc(product.state.replace('_',' '))} · SKU ${esc(product.sku||'—')}${product.category_group||product.category?' · '+esc(product.category_group||product.category):''}</p><div class="detail-grid"><div><small>Gross revenue</small><strong>${available('orders')?cash(product.revenue[currency],currency):'—'}</strong></div><div><small>Orders / units</small><strong>${available('orders')?`${fmt(product.orders)} / ${fmt(product.units)}`:'—'}</strong></div><div><small>Views</small><strong>${traffic.available?fmt(traffic.views):'—'}</strong></div><div><small>Views / sale</small><strong>${traffic.available?ratio(product.views_per_sale):'—'}</strong></div><div><small>Time to sell</small><strong>${duration(product.days_to_sale)}</strong></div><div><small>${sold?'Sold':'Listed for'}</small><strong>${sold?(saleDate(product.sold_at)||'In selected period'):age(product.days_listed)}</strong></div><div><small>Promoted fees</small><strong>${ads.available?cash(ads.fees,currency):'—'}</strong></div><div><small>Promoted ROAS</small><strong>${ads.available?decimal(ads.roas,2)+'×':'—'}</strong></div></div><h2>Traffic sources</h2><div class="source-line"><span>Search-result views</span><strong>${traffic.available?fmt(traffic.search_views):'—'}</strong></div><div class="source-line"><span>Store views</span><strong>${traffic.available?fmt(traffic.store_views):'—'}</strong></div><div class="source-line"><span>Direct views</span><strong>${traffic.available?fmt(traffic.direct_views):'—'}</strong></div><div class="source-line"><span>Off-eBay views</span><strong>${traffic.available?fmt(traffic.off_ebay_views):'—'}</strong></div><div class="source-line"><span>Other eBay views</span><strong>${traffic.available?fmt(traffic.other_ebay_views):'—'}</strong></div>${campaigns?`<h2 style="margin-top:22px">Advertising campaigns</h2><div class="campaign-list">${campaigns}</div>`:'<p class="detail-note">This listing is not currently associated with a returned Promoted Listings campaign.</p>'}<p class="detail-note">${product.listed_at?'Originally listed '+esc(new Date(product.listed_at).toLocaleDateString())+'. ':''}${product.sold_at?' First selected-period sale: '+esc(new Date(product.sold_at).toLocaleDateString())+'.':''}</p>${isDemo?'<p class="detail-note">This is sample data.</p>':`<a href="https://www.ebay.com/itm/${encodeURIComponent(product.listing_id)}" target="_blank" rel="noopener noreferrer">View listing on eBay ↗</a>`}`;
  $('detail').showModal();
}

function exportCsv(){
  const currency=$('currency').value;
  const rows=[['Listing ID','SKU','Title','Status','Category','Price','Currency','Originally listed','Active listing age (days)','Sold at','Days to sale','Views per sale',`Gross revenue (${currency})`,'Orders','Units','Organic impressions','Views','Search views','Store views','Direct views','Off-eBay views','Other eBay views','Organic CTR','Organic conversion','Advertised','Ad status',`Ad fees (${currency})`,`Attributed ad sales (${currency})`,'Ad impressions','Ad clicks','Ad CTR','Ad conversion','Ad CPC','Ad ROAS','Ad ACOS','Signal','Snapshot','Data mode'],...filtered.map(product=>[product.listing_id,product.sku,product.title,product.state,product.category_group||product.category,product.price,product.currency,product.listed_at,product.state==='active'&&!Number(product.units||0)?product.days_listed:null,product.sold_at,product.days_to_sale,product.views_per_sale,product.revenue[currency]||0,product.orders,product.units,product.organic.impressions,product.organic.views,product.organic.search_views,product.organic.store_views,product.organic.direct_views,product.organic.off_ebay_views,product.organic.other_ebay_views,product.organic.ctr,product.organic.conversion_rate,product.ads.advertised,product.ads.ad_status,product.ads.fees,product.ads.sale_amount,product.ads.impressions,product.ads.clicks,product.ads.ctr,product.ads.conversion_rate,product.ads.cpc,product.ads.roas,product.ads.acos,product.signal,data.captured_at,isDemo?'DEMO':'LIVE'])];
  const cell=value=>{let text=String(value??'');if(typeof value==='string'&&/^[=+@\-\t\r]/.test(text))text="'"+text;return '"'+text.replaceAll('"','""')+'"';};
  const url=URL.createObjectURL(new Blob(['\ufeff'+rows.map(row=>row.map(cell).join(',')).join('\r\n')],{type:'text/csv;charset=utf-8'}));
  const link=document.createElement('a');link.href=url;link.download=`ebay-intelligence-${isDemo?'demo':data.marketplace_id}-${data.days}d.csv`;document.body.appendChild(link);link.click();link.remove();setTimeout(()=>URL.revokeObjectURL(url),1000);
}

function demo(){
  clearTimeout(timer);++requestVersion;busy(false);isDemo=true;page=1;attentionSignal='all';
  const days=Number($('period').value),now=new Date(),currency='USD';
  const names=['Vintage brass candlestick pair','Mid-century ceramic vase','Antique botanical print set','Walnut jewelry box','Woven market basket','Art deco picture frame','Vintage amber glass bottle','Solid brass drawer pulls','Stoneware espresso cup set','Vintage silver serving tray','Oak display shelf','Handwoven cotton throw'],categories=['Collectibles','Pottery & Glass','Art','Collectibles','Home & Garden','Collectibles'];
  const products=names.map((title,index)=>{const units=index%4===0?0:Math.max(0,9-index),price=[58,72,39,115,64,48,29,24,55,88,95,125][index],daysListed=34+index*31,views=18+index*13,impressions=260+index*180,advertised=index%3!==1,fees=advertised?(index===0?18:4+index*1.7):0,adSales=index===0?0:advertised?units*price*.45:0,signal=fees>0&&adSales===0?'ad_waste':views>=20&&units===0?'conversion':daysListed>=120&&units===0?'stale':units>=3?'performer':'steady',category=categories[index%categories.length],days_to_sale=units?Math.min(daysListed,12+index*9):null,listed_at=new Date(now.getTime()-daysListed*86400000).toISOString(),sold_at=days_to_sale==null?null:new Date(new Date(listed_at).getTime()+days_to_sale*86400000).toISOString();return {listing_id:String(100000000000+index),sku:`EB-${1000+index}`,title,state:index===10?'ended':'active',quantity:1,lifetime_sold:units,price,currency,image:null,category,category_group:category,listed_at,days_listed:daysListed,sold_at,days_to_sale,views_for_sale:units?views:null,views_per_sale:units?views/units:null,orders:Math.ceil(units*.8),units,revenue:{USD:units*price},organic:{available:true,impressions,listing_impressions:impressions*.82,search_impressions:impressions*.7,store_impressions:impressions*.12,views,search_views:Math.round(views*.55),store_views:Math.round(views*.1),direct_views:Math.round(views*.12),off_ebay_views:Math.round(views*.08),other_ebay_views:Math.round(views*.15),transactions:units,ctr:views/impressions,conversion_rate:units/views},ads:{available:true,advertised,ad_status:advertised?'ACTIVE':null,bid_percentage:advertised?8.5:null,campaigns:advertised?[{campaign_id:'demo',campaign_name:'General antiques',campaign_status:'RUNNING'}]:[],funding_models:advertised?['COST_PER_SALE']:[],impressions:advertised?impressions*.35:0,clicks:advertised?Math.round(views*.3):0,fees,sales:advertised?units*.4:0,sale_amount:adSales,ctr:0.03,conversion_rate:0.08,cpc:0,roas:fees?adSales/fees:0,acos:adSales?fees/adSales:null},signal};});
  const revenue=products.reduce((sum,product)=>sum+product.revenue.USD,0),adFees=products.reduce((sum,product)=>sum+product.ads.fees,0),adSales=products.reduce((sum,product)=>sum+product.ads.sale_amount,0),weights=Array.from({length:days},(_,index)=>Math.max(.2,1+Math.sin(index*1.5)*.5+(index%7===5?.5:0))),weight=weights.reduce((a,b)=>a+b,0);
  const dateKey=value=>{const d=new Date(value);return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;};
  data={empty:false,days,marketplace_id:'EBAY_US',captured_at:now.toISOString(),warnings:[],sources:{traffic:true,orders:true,ads:true},period_start:dateKey(now.getTime()-(days-1)*86400000),period_end:dateKey(now),products,category_performance:derivedCategoryPerformance(products,days),series:weights.map((value,index)=>({date:dateKey(now.getTime()-(days-1-index)*86400000),revenue:{USD:revenue*value/weight},orders:Math.round(value*2),units:Math.round(value*2.5),impressions:Math.round(value*380),views:Math.round(value*31),transactions:Math.round(value*2)})),summary:{revenue:{USD:revenue},orders:products.reduce((sum,p)=>sum+p.orders,0),units:products.reduce((sum,p)=>sum+p.units,0),active:products.filter(p=>p.state==='active').length,impressions:products.reduce((sum,p)=>sum+p.organic.impressions,0),views:products.reduce((sum,p)=>sum+p.organic.views,0),ctr:.074,conversion_rate:.041,ad_fees:{USD:adFees},ad_sales:{USD:adSales},ad_roas:adFees?adSales/adFees:0,advertised:products.filter(p=>p.ads.advertised).length}};
  $('connection').textContent='● Demo workspace';notice('DEMO DATA · Explore the page, then refresh to load your eBay account.',true);render();
}

$('refresh').addEventListener('click',refresh);
$('demo').addEventListener('click',demo);$('empty-demo').addEventListener('click',demo);
for(const id of ['marketplace','period'])$(id).addEventListener('change',load);
$('currency').addEventListener('change',()=>{if(data&&!data.empty){page=1;render();}});
for(const id of ['search','state','ad-filter'])$(id).addEventListener(id==='search'?'input':'change',()=>{attentionSignal='all';page=1;renderRows();});
$('sort').addEventListener('change',()=>{productSortKey=$('sort').value;productSortDirection=$('sort-direction').value;page=1;renderRows();});
$('sort-direction').addEventListener('change',()=>{productSortDirection=$('sort-direction').value;page=1;renderRows();});
for(const button of document.querySelectorAll('[data-product-sort]'))button.addEventListener('click',()=>{const key=button.dataset.productSort;productSortDirection=productSortKey===key?(productSortDirection==='asc'?'desc':'asc'):(key==='title'?'asc':'desc');productSortKey=key;if([...$('sort').options].some(option=>option.value===key))$('sort').value=key;$('sort-direction').value=productSortDirection;page=1;renderRows();});
for(const button of document.querySelectorAll('[data-category-sort]'))button.addEventListener('click',()=>{const key=button.dataset.categorySort;categorySortDirection=categorySortKey===key?(categorySortDirection==='asc'?'desc':'asc'):(key==='category'?'asc':'desc');categorySortKey=key;renderCategories();});
$('prev').addEventListener('click',()=>{page--;renderRows();});$('next').addEventListener('click',()=>{page++;renderRows();});
$('rows').addEventListener('click',event=>{const button=event.target.closest('[data-id]');if(button)detail(button.dataset.id);});
for(const button of document.querySelectorAll('[data-signal]'))button.addEventListener('click',()=>{$('state').value='all';$('ad-filter').value='all';$('search').value='';attentionSignal=button.dataset.signal;page=1;renderRows();document.querySelector('.products').scrollIntoView({behavior:'smooth',block:'start'});});
$('export').addEventListener('click',exportCsv);$('close-detail').addEventListener('click',()=>$('detail').close());
$('detail').addEventListener('click',event=>{if(event.target===$('detail')){const rect=$('detail').getBoundingClientRect();if(event.clientX<rect.left||event.clientX>rect.right||event.clientY<rect.top||event.clientY>rect.bottom)$('detail').close();}});
window.addEventListener('resize',()=>{if(data&&!data.empty)drawChart();});

(async()=>{if(new URLSearchParams(location.search).get('demo')==='1'){demo();return;}await load();})();
