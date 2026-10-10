import fs from 'fs/promises';
import { SpreadsheetFile, Workbook } from '@oai/artifact-tool';

const input = JSON.parse(await fs.readFile('/tmp/etsy_ad_review.json', 'utf8'));
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
const detail = workbook.worksheets.add('Etsy Listings');
const font = { name: 'Arial', size: 10 };

summary.getRange('A1:H1').merge();
summary.getRange('A1').values = [['Etsy Advertising Review']];
summary.getRange('A1:H1').format = { fill: colors.navy, font: { name: 'Arial', size: 16, bold: true, color: colors.white } };
summary.getRange('A3:B7').values = [
  ['Snapshot captured', `As of ${input.captured_at}`],
  ['Analysis window', '90 days'],
  ['Products reviewed', rows.length],
  ['Ads data', 'Not available through Etsy API'],
  ['Use', 'Prioritize listings for manual Etsy Ads review'],
];
summary.getRange('A3:A7').format.font = { name: 'Arial', bold: true };
summary.getRange('A3:B7').format = { font };
summary.getRange('A3:A7').format.font = { name: 'Arial', bold: true };

summary.getRange('A9:C9').values = [['Recommended action', 'Listings', 'Business rule']];
summary.getRange('A9:C9').format = { fill: colors.navy, font: { name: 'Arial', bold: true, color: colors.white } };
const actionDefs = [
  ['PAUSE ADS — OVER 180 DAYS', 'Age >= 180 days and no sale', colors.red],
  ['PAUSE ADS — ATTENTION, NO SALE', 'Favorites >= 3 or views >= 25, but no sale', colors.amber],
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
  'This workbook identifies listings for manual Etsy Ads decisions. Etsy’s public API does not expose Etsy Ads controls or per-listing ad spend, so the action labels are based on age, sales, views, favorites, and price. Long-tail exceptions should be reviewed individually before pausing. Use the Etsy Listings tab to sort and filter by action, category, age, or engagement.'
]];
summary.getRange('A18:H22').format = { wrapText: true, verticalAlignment: 'top', font };

summary.getRange('A24:B28').values = [
  ['Action sequence', '1. Pause clear stale/no-sale listings. 2. Fix high-attention/no-sale listings before re-advertising. 3. Keep or increase exposure for proven sellers.'],
  ['Attention rule', 'Favorites >= 3 or views >= 25.'],
  ['Long-tail exception', 'Price >= $250 and age >= 180 days.'],
  ['Data limitation', 'Inactive Etsy listings often report zero views; treat view-based flags as directional.'],
  ['Source', 'Latest saved Etsy Insights snapshot'],
];
summary.getRange('A24:A28').format.font = { name: 'Arial', bold: true };
summary.getRange('A24:B28').format = { font, wrapText: true, verticalAlignment: 'top' };

const headers = ['Listing ID', 'Title', 'Category', 'State', 'Price (USD)', 'Days Listed', 'Favorites', 'Views', 'Units Sold (90d)', 'Orders (90d)', 'Revenue (USD)', 'Days to Sale', 'Recommended Action'];
detail.getRange(`A1:M${rows.length + 1}`).values = [headers, ...rows.map(r => [
  r.listing_id, r.title, r.category, r.state, r.price, r.days_listed, r.favorites, r.views,
  r.units, r.orders, r.revenue, r.days_to_sale, r.action,
])];
detail.getRange(`A1:M${rows.length + 1}`).format = { font, verticalAlignment: 'center' };
detail.getRange('A1:M1').format = { fill: colors.navy, font: { name: 'Arial', bold: true, color: colors.white }, wrapText: true };
detail.freezePanes.freezeRows(1);
const table = detail.tables.add(`A1:M${rows.length + 1}`, true, 'EtsyAdReviewTable');
table.style = 'TableStyleMedium2';
table.showFilterButton = true;
for (let i = 0; i < rows.length; i++) {
  const fill = actionDefs.find(d => d[0] === rows[i].action)?.[2] || colors.white;
  detail.getRange(`A${i + 2}:M${i + 2}`).format.fill = fill;
}
detail.getRange(`E2:E${rows.length + 1}`).format.numberFormat = '$#,##0.00';
detail.getRange(`K2:K${rows.length + 1}`).format.numberFormat = '$#,##0.00';
detail.getRange(`A1:M${Math.min(rows.length + 1, 150)}`).format.autofitColumns();
detail.getRange('B:B').format.columnWidth = 42;
detail.getRange('C:C').format.columnWidth = 28;
detail.getRange('M:M').format.columnWidth = 34;
summary.getRange('A:A').format.columnWidth = 44;
summary.getRange('A:H').format.columnWidth = 20;
summary.getRange('A:A').format.columnWidth = 44;
summary.getRange('B:B').format.columnWidth = 28;
summary.getRange('C:C').format.columnWidth = 56;

workbook.recalculate();
const outDir = '/Users/administrator/Desktop/Dev/ebay-shopify-sync/outputs/etsy-ad-review';
await fs.mkdir(outDir, { recursive: true });
const preview = await workbook.render({ sheetName: 'Summary', autoCrop: 'all', scale: 1, format: 'png' });
await fs.writeFile(`${outDir}/summary-preview.png`, new Uint8Array(await preview.arrayBuffer()));
const xlsx = await SpreadsheetFile.exportXlsx(workbook);
await xlsx.save(`${outDir}/etsy-ad-review.xlsx`);
console.log(JSON.stringify({ output: `${outDir}/etsy-ad-review.xlsx`, rows: rows.length, counts }));
