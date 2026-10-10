import fs from 'fs/promises';
import { SpreadsheetFile, Workbook } from '@oai/artifact-tool';

const input = JSON.parse(await fs.readFile('/tmp/ebay_ad_review.json', 'utf8'));
const rows = input.rows.sort((a, b) => {
  const rank = {
    'PAUSE ADS — OVER 180 DAYS': 1,
    'PAUSE ADS — ATTENTION, NO SALE': 2,
    'MANUAL REVIEW — LONG-TAIL EXCEPTION': 3,
    'REVIEW — NEW OR LOW ATTENTION': 4,
    'KEEP / BOOST WINNER': 5,
  };
  return (rank[a.action] - rank[b.action]) || ((b.days_listed || 0) - (a.days_listed || 0));
});

const colors = {
  navy: '#17365D',
  blue: '#D9EAF7',
  red: '#FCE4D6',
  amber: '#FFF2CC',
  green: '#E2F0D9',
  gray: '#F2F2F2',
  white: '#FFFFFF',
};

const workbook = Workbook.create();
const summary = workbook.worksheets.add('Summary');
const detail = workbook.worksheets.add('eBay Listings');
const font = { name: 'Arial', size: 10 };

summary.getRange('A1:H1').merge();
summary.getRange('A1').values = [['eBay Advertising Review']];
summary.getRange('A1:H1').format = { fill: colors.navy, font: { name: 'Arial', size: 16, bold: true, color: colors.white } };
summary.getRange('A3:B8').values = [
  ['Snapshot captured', `As of ${input.captured_at}`],
  ['Analysis window', `${input.days} days`],
  ['Products reviewed', rows.length],
  ['Currently advertised', input.summary?.advertised ?? 'n.a.'],
  ['Ads performance', 'Fees/ROAS unavailable; eBay permissions required'],
  ['Use', 'Prioritize listings for manual eBay Promoted Listings review'],
];
summary.getRange('A3:A8').format.font = { name: 'Arial', bold: true };
summary.getRange('A3:B8').format = { font };
summary.getRange('A3:A8').format.font = { name: 'Arial', bold: true };

summary.getRange('A9:C9').values = [['Recommended action', 'Listings', 'Business rule']];
summary.getRange('A9:C9').format = { fill: colors.navy, font: { name: 'Arial', bold: true, color: colors.white } };
const actionDefs = [
  ['PAUSE ADS — OVER 180 DAYS', 'Age >= 180 days and no sale', colors.red],
  ['PAUSE ADS — ATTENTION, NO SALE', 'Views >= 25 or impressions >= 5,000, but no sale', colors.amber],
  ['MANUAL REVIEW — LONG-TAIL EXCEPTION', 'No sale, age >= 180 days, price >= $250', colors.blue],
  ['REVIEW — NEW OR LOW ATTENTION', 'No sale and does not meet pause criteria', colors.gray],
  ['KEEP / BOOST WINNER', 'At least one sale in the analysis window', colors.green],
];
const counts = Object.fromEntries(actionDefs.map(d => [d[0], rows.filter(r => r.action === d[0]).length]));
summary.getRange('A10:C14').values = actionDefs.map(([a, rule]) => [a, counts[a], rule]);
for (let i = 0; i < actionDefs.length; i++) {
  summary.getRange(`A${10 + i}:C${10 + i}`).format = { fill: actionDefs[i][2], font };
}

summary.getRange('A17:H17').merge();
summary.getRange('A17').values = [['Management notes']];
summary.getRange('A17:H17').format = { fill: colors.navy, font: { name: 'Arial', bold: true, color: colors.white } };
summary.getRange('A18:H22').merge();
summary.getRange('A18').values = [[
  'This workbook identifies listings for manual eBay Promoted Listings decisions. The latest snapshot could read ad membership, but eBay did not return ad fees or ROAS because Analytics and Marketing permissions are missing. Action labels are based on age, sales, views, impressions, advertised status, and price. Long-tail exceptions should be reviewed individually before pausing. Use the eBay Listings tab to sort and filter by action, category, age, or engagement.'
]];
summary.getRange('A18:H22').format = { wrapText: true, verticalAlignment: 'top', font };

summary.getRange('A24:B28').values = [
  ['Action sequence', '1. Pause clear stale/no-sale listings. 2. Fix high-attention/no-sale listings before re-advertising. 3. Keep or increase exposure for proven sellers.'],
  ['Attention rule', 'Views >= 25 or impressions >= 5,000.'],
  ['Long-tail exception', 'Price >= $250 and age >= 180 days.'],
  ['Data limitation', 'Ad fees and ROAS were unavailable in the latest eBay snapshot.'],
  ['Source', 'Latest saved eBay Insights snapshot'],
];
summary.getRange('A24:A28').format.font = { name: 'Arial', bold: true };
summary.getRange('A24:B28').format = { font, wrapText: true, verticalAlignment: 'top' };

const headers = ['Listing ID', 'Title', 'Category', 'State', 'Price (USD)', 'Days Listed', 'Impressions', 'Views', 'Units Sold (30d)', 'Orders (30d)', 'Revenue (USD)', 'Days to Sale', 'Advertised', 'Bid %', 'Ad Fees', 'Ad Sales', 'ROAS', 'Recommended Action'];
detail.getRange(`A1:R${rows.length + 1}`).values = [headers, ...rows.map(r => [
  r.listing_id, r.title, r.category, r.state, r.price, r.days_listed, r.impressions, r.views,
  r.units, r.orders, r.revenue, r.days_to_sale, r.advertised, r.bid_percentage, r.ad_fees, r.ad_sales, r.roas, r.action,
])];
detail.getRange(`A1:R${rows.length + 1}`).format = { font, verticalAlignment: 'center' };
detail.getRange('A1:R1').format = { fill: colors.navy, font: { name: 'Arial', bold: true, color: colors.white }, wrapText: true };
detail.freezePanes.freezeRows(1);
const table = detail.tables.add(`A1:R${rows.length + 1}`, true, 'EbayAdReviewTable');
table.style = 'TableStyleMedium2';
table.showFilterButton = true;
for (let i = 0; i < rows.length; i++) {
  const fill = actionDefs.find(d => d[0] === rows[i].action)?.[2] || colors.white;
  detail.getRange(`A${i + 2}:R${i + 2}`).format.fill = fill;
}
detail.getRange(`E2:E${rows.length + 1}`).format.numberFormat = '$#,##0.00';
detail.getRange(`K2:K${rows.length + 1}`).format.numberFormat = '$#,##0.00';
detail.getRange(`O2:P${rows.length + 1}`).format.numberFormat = '$#,##0.00';
detail.getRange(`A1:R${Math.min(rows.length + 1, 150)}`).format.autofitColumns();
detail.getRange('B:B').format.columnWidth = 42;
detail.getRange('C:C').format.columnWidth = 28;
detail.getRange('R:R').format.columnWidth = 34;
summary.getRange('A:A').format.columnWidth = 44;
summary.getRange('A:H').format.columnWidth = 20;
summary.getRange('A:A').format.columnWidth = 44;
summary.getRange('B:B').format.columnWidth = 28;
summary.getRange('C:C').format.columnWidth = 56;

workbook.recalculate();
const outDir = '/Users/administrator/Desktop/Dev/ebay-shopify-sync/outputs/ebay-ad-review';
await fs.mkdir(outDir, { recursive: true });
const preview = await workbook.render({ sheetName: 'Summary', autoCrop: 'all', scale: 1, format: 'png' });
await fs.writeFile(`${outDir}/summary-preview.png`, new Uint8Array(await preview.arrayBuffer()));
const xlsx = await SpreadsheetFile.exportXlsx(workbook);
await xlsx.save(`${outDir}/ebay-ad-review.xlsx`);
console.log(JSON.stringify({ output: `${outDir}/ebay-ad-review.xlsx`, rows: rows.length, counts }));
