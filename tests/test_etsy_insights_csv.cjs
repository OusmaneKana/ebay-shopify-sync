const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
let exportedBlob, filename;
const elements = new Map();
const element = id => {
  if (!elements.has(id)) elements.set(id, {value: id === 'currency' ? 'USD' : id === 'shop' ? '123' : '30', addEventListener() {}, classList: {toggle() {}}, textContent: '', hidden: true});
  return elements.get(id);
};
const context = vm.createContext({
  document: {getElementById: element, querySelectorAll: () => [], body: {appendChild() {}}, createElement: () => ({click() {filename = this.download;}, remove() {}})},
  window: {addEventListener() {}}, location: {search: ''}, URLSearchParams, Intl, Date, Blob,
  URL: {createObjectURL(blob) {exportedBlob = blob; return 'blob:test';}, revokeObjectURL() {}},
  setTimeout() {}, clearTimeout() {}, fetch: async () => ({ok: true, status: 200, json: async () => ({shop_ids: []})})
});
vm.runInContext(fs.readFileSync('static/etsy_insights.js', 'utf8'), context);
vm.runInContext(`
  data={days:30,captured_at:'2026-09-14',baseline_at:null};
  isDemo=true;
  filtered=[{listing_id:123,title:'=HYPERLINK("bad","name")',state:'active',revenue:{USD:12.50},units:1,orders:1,views:null,view_change:null,favorites:5,favorite_change:-2,rating:null,review_count:0,quantity:3,signal:'steady'}];
  exportCsv();
`, context);
(async () => {
  const csv = await exportedBlob.text();
  assert.ok(csv.includes('"Gross revenue (USD)"'));
  assert.ok(csv.includes('"\'=HYPERLINK(""bad"",""name"")"'), 'Seller-controlled formulas must be escaped');
  assert.ok(csv.includes('"-2"'), 'Numeric negative changes must remain numeric');
  assert.ok(csv.includes('"DEMO"'), 'Sample exports must be labeled');
  assert.equal(filename, 'etsy-insights-demo-30d.csv');
  assert.equal(csv.split('\r\n').length, 2);
  console.log('CSV content, escaping, and demo labeling checks passed.');
})();
